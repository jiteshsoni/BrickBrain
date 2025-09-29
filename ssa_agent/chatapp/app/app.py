import logging
import os
import streamlit as st

from model_serving_utils import query_endpoint, is_endpoint_supported
from conversation import ConversationDB
from document_processor import DocumentProcessor

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("__BRICKBRAIN_CHATAPP__")

query_params = st.query_params 
thread_ts = query_params.get("thread_ts", None)
source = query_params.get("source", None)
online_store = "brickbrain-conversation-history"
CONVERSATION_DB = ConversationDB(online_store=online_store, catalog_name="databricks_postgres")

SERVING_ENDPOINT = "ka-ce79c9c6-endpoint"
endpoint_supported = is_endpoint_supported(SERVING_ENDPOINT)

doc_processor = DocumentProcessor(max_pdf_pages=3, max_chars_per_file=10000)

def get_user_info():
    headers = st.context.headers
    return dict(
        user_name=headers.get("X-Forwarded-Preferred-Username"),
        user_email=headers.get("X-Forwarded-Email"),
        user_id=headers.get("X-Forwarded-User"),
    )

user_info = get_user_info()

def load_conversation_history():
    if thread_ts:
        history = CONVERSATION_DB.get_conversation_history(thread_ts)
        messages = [] 
        for conv in history: 
            if conv["user_message"]:
                messages.append({"role": "user", "content": conv["user_message"]})
            if conv["assistant_message"]:
                messages.append({"role": "assistant", "content": conv["assistant_message"]})
        return messages
    else:
        logger.info(f"No conversation history found. Thread ID: {thread_ts}")
        return []

if "visibility" not in st.session_state:
    st.session_state.visibility = "visible"
    st.session_state.disabled = False

st.title("BRICKBRAIN")

if source == "slack": 
    st.info(f"Loading conversation history from Slack for thread {thread_ts}")
    st.session_state.messages = load_conversation_history()
else:
    st.session_state.messages = []

st.subheader("Document Upload")
uploaded_files = st.file_uploader("Upload a file", accept_multiple_files=False, type=["pdf", "txt", "md"])

if uploaded_files:
    with st.spinner("Waiting for file to be processed..."):
        processed_file = doc_processor.process_file(uploaded_files)
        st.session_state.processed_file = processed_file
else:
    st.session_state.processed_file = None

if not endpoint_supported:
    st.error("⚠️ Unsupported Endpoint Type")
else:
    if st.session_state.processed_file:
        st.info(f"You have added this file: {st.session_state.processed_file['filename']}")
    
    if "messages" not in st.session_state:
        st.session_state.messages = []

    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    if prompt := st.chat_input("How can I use cosine similarity in Databricks Vector Search? "):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            logger.debug(f"Serving endpoint: {SERVING_ENDPOINT}")
            
            messages_with_context = st.session_state.messages.copy()
            if st.session_state.processed_file:
                messages_with_context = doc_processor.inject_documents_into_messages(
                    messages_with_context, 
                    st.session_state.processed_file
                )
                
            assistant_response = query_endpoint(
                endpoint_name=SERVING_ENDPOINT,
                messages=messages_with_context,
                logger=logger,
            )
            st.markdown(assistant_response[0])
            trace_id = assistant_response[1]
        st.session_state.messages.append({"role": "assistant", "content": assistant_response[0]})
