from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
import slack_sdk

from databricks.sdk import WorkspaceClient
import mlflow
from mlflow.deployments import get_deploy_client
from mlflow.entities.assessment import AssessmentSource, AssessmentSourceType
from mlflow_client import link_traces_to_run, get_labelling_session

import os
import ssl
import re 
import logging 
import time
import urllib.parse
import base64
import json


from conversation import ConversationDB


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("__BRICKBRAIN_SLACKBOT__")

mlflow.set_tracking_uri("databricks")
mlflow.set_registry_uri("databricks-uc")
mlflow_client = get_deploy_client("databricks")
online_store = "brickbrain-conversation-history"
CONVERSATION_DB = ConversationDB(online_store=online_store, catalog_name="databricks_postgres")
ENDPOINT_NAME = "ka-ce79c9c6-endpoint"
EXPERIMENT = mlflow.set_experiment("/Users/jitesh.soni@databricks.com/ka-ce79c9c6-dev-experiment")
LABELLING_SESSION = get_labelling_session(labelling_session_name="slack")
APP_URL = "https://brickbrain-stream-lit-2876898140402128.aws.databricksapps.com/"

def get_thread_messages(client, channel, thread_ts):
    response = client.conversations_replies(
        channel=channel,
        ts=thread_ts,
        inclusive=True,  # Include the parent message
        limit=10  # Max messages to retrieve
    )
    logger.info(f"Retrieved {len(response['messages'])} messages from thread {thread_ts}")
    return response['messages']

def format_thread_messages(messages):
    return [{"role": "user", "content": message['text']} for message in messages] 

def get_history(client, channel, thread_ts):
    messages = get_thread_messages(client, channel, thread_ts)
    return format_thread_messages(messages)

def save_conversation_history(thread_id, message_text, agent_output):
    lakebase_id = CONVERSATION_DB.save_conversation(
        thread_id=thread_id,
        user_message=message_text,
        assistant_message=agent_output
    )
    logger.info(f"Conversation history saved - Lakebase Key ID: {lakebase_id}")

def generate_app_url(thread_ts): 
    params = {
        "thread_ts": thread_ts,
        "source": "slack"
    }
    query_string = urllib.parse.urlencode(params)
    return f"{APP_URL}?{query_string}"

def clean_footnotes_for_slack(text):
    text = re.sub(r'\[\^[^\]]+\]', '', text)
    lines = text.split('\n')
    cleaned_lines = []
    
    for line in lines:
        if re.match(r'^:\s+', line.strip()):
            continue
        if re.match(r'^\[\^[^\]]+\]:', line.strip()):
            continue
        cleaned_lines.append(line)
    cleaned_text = '\n'.join(cleaned_lines)
    cleaned_text = re.sub(r'\n\s*\n\s*\n+', '\n\n', cleaned_text)
    return cleaned_text.strip()

def call_llm(client, channel, message_text, thread_id):
    logger.info(f"Agent invoked - Thread: {thread_id}")
    history = get_history(client, channel, thread_id)
    logger.info(f"History length: {len(history)}")

    try:
        input_data = {
            "input": history + [{"role": "user", "content":  message_text}],
            "databricks_options": {"return_trace": True}
        }
        
        response = mlflow_client.predict(endpoint=ENDPOINT_NAME, inputs=input_data)
        trace_id = response['databricks_output']['trace']['info']['trace_id']
        agent_output = response['output'][0]['content'][0]['text']

        save_conversation_history(thread_id, message_text, agent_output)
        logger.info(f"Agent response succeeded - Thread: {thread_id}, Trace: {trace_id}")
        return agent_output, trace_id
    
    except Exception as e:
        logger.error(f"Agent response failed - Thread: {thread_id}, Error: {e}")
        return "Sorry, I had an error processing that.", "trace-id-unknown"

def get_slack_auth():
    w = WorkspaceClient()
    token_app = dbutils.secrets.get(scope="brickbrain-scope", key="slack-app-token")
    token_bot = dbutils.secrets.get(scope="brickbrain-scope", key="slack-bot-token")    
    return token_app, token_bot

def start_slack_client():
    logger.info("Initalized slack client. ")
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    client = slack_sdk.WebClient(token=token_bot, ssl=ssl_context)
    return App(client=client, process_before_response=False)

token_app, token_bot = get_slack_auth()
app = start_slack_client()

@app.event("message")
def llm_response(event, say, client):
    logger.info(f"Message received - User: {event['user']}, Text: {event['text'][:20]}...")

    if "brickbrain" not in event['text'].lower():
        logger.info(f"Skipping LLM call. No mention. ")
        return
    
    channel = event['channel']
    response, trace_id = call_llm(client, channel, event['text'], event['ts'])  
    slack_response = clean_footnotes_for_slack(response) # clean footnotes for slack

    streamlit_url = generate_app_url(event['ts'])
    logger.info(f"Streamlit URL: {streamlit_url}")

    result = client.chat_postMessage(
        channel=event['channel'],
        blocks=[
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": slack_response}
            },
            {
                "type": "actions", 
                "elements": [
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "Continue conversation in Databricks Apps"},
                        "url": streamlit_url, 
                        "action_id": "open_streamlit"
                    }
                ]
            }
        ],
        text=slack_response,
        thread_ts=event['ts'],  # reply in the thread
        metadata={
            "event_type": "agent_response",
            "event_payload": {
                "trace_id": trace_id,
                "thread_id": event['ts'],
                "resource_type": "AGENT_RESPONSE",
                "streamlit_url": streamlit_url
            }
        }
    )
    
@app.message_shortcut("log_feedback")
def handle_log_feedback_shortcut(ack, shortcut, client):
    ack()
    logger.info(f"Feedback message shortcut triggered by user: {shortcut['user']['name']}")
    
    message = shortcut['message']
    message_ts = message['ts']
    
    metadata = message.get('metadata', {})
    event_payload = metadata.get('event_payload', {})
    trace_id = event_payload.get('trace_id')
    logger.debug("Trace ID: ", trace_id)

    message_preview = message.get('text', '')[:100]
    if len(message.get('text', '')) > 100:
        message_preview += "..."
    
    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn", 
                "text": f"*Providing feedback for:*\n> {message_preview}"
            }
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Trace ID:* `{trace_id}`"
            }
        },
        {
            "type": "input",
            "block_id": "feedback_block",
            "element": {
                "type": "radio_buttons",
                "action_id": "feedback_value",
                "options": [
                    {
                        "text": {"type": "plain_text", "text": "👍 Positive"},
                        "value": "true"
                    },
                    {
                        "text": {"type": "plain_text", "text": "👎 Negative"},
                        "value": "false"
                    }, 
                ]
            },
            "label": {"type": "plain_text", "text": "Feedback"}
        },
        {
            "type": "input",
            "block_id": "comments_block",
            "element": {
                "type": "plain_text_input",
                "action_id": "comments_input",
                "multiline": True,
                "placeholder": {"type": "plain_text", "text": "Optional comments..."}
            },
            "label": {"type": "plain_text", "text": "Comments"},
            "optional": True
        }
    ]
    
    client.views_open(
        trigger_id=shortcut["trigger_id"],
        view={
            "type": "modal",
            "callback_id": "feedback_modal",
            "title": {"type": "plain_text", "text": "Log Feedback"},
            "submit": {"type": "plain_text", "text": "Submit"},
            "close": {"type": "plain_text", "text": "Cancel"},
            "blocks": blocks,
            "private_metadata": trace_id 
        }
    )


@app.view("feedback_modal")
def handle_feedback_submission(ack, body, view, client):
    ack()
    
    values = view["state"]["values"]
    trace_id = view.get("private_metadata", "")
    if not trace_id:
        logger.error("No trace_id found in modal metadata")
        client.chat_postMessage(
            channel=body['user']['id'],
            text="ERROR: No trace ID found. Please try again."
        )
        return
    
    logger.info(f"Using trace_id from metadata: {trace_id}")
    
    if "feedback_block" not in values or not values["feedback_block"]["feedback_value"]["selected_option"]:
        logger.error("No feedback value selected")
        client.chat_postMessage(
            channel=body['user']['id'],
            text="ERROR: Please select a feedback rating (positive or negative)."
        )
        return
    
    feedback_value = values["feedback_block"]["feedback_value"]["selected_option"]["value"] == "true"
    comments = values.get("comments_block", {}).get("comments_input", {}).get("value", "")
    
    try:
        if comments: 
            mlflow.log_feedback(
                trace_id=trace_id,
                name="slackbot_feedback",
                value=feedback_value,
                rationale=comments,
                source=AssessmentSource(
                    source_type=AssessmentSourceType.HUMAN,
                    source_id=body['user']['name'], 
                ),
            )
            
        else: 
            mlflow.log_feedback(
                trace_id=trace_id,
                name="slackbot_feedback",
                value=feedback_value,
                source=AssessmentSource(
                    source_type=AssessmentSourceType.HUMAN,
                    source_id=body['user']['name'], 
                ),
            )
        
        logger.info(f"Feedback logged successfully - Trace: {trace_id}, Value: {feedback_value}, User: {body['user']['name']}")
        link_traces_to_run(run_id=LABELLING_SESSION.mlflow_run_id, trace_ids=[trace_id])
        logger.info(f"Traces linked to run - Run: {LABELLING_SESSION.mlflow_run_id}, Trace: {trace_id}")

    except Exception as e:
        logger.error(f"Error logging feedback: {e}")
        client.chat_postMessage(
            channel=body['user']['id'],
            text=f"ERROR: {str(e)}"
        )

if __name__ == "__main__":
    SocketModeHandler(app, token_app).start()
