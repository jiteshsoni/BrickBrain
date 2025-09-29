import io
import re
from typing import List, Dict, Tuple, Optional
import streamlit as st
import pymupdf 


class DocumentProcessor:    
    def __init__(self, max_pdf_pages: int = 3, max_chars_per_file: int = 10000):
        self.max_pdf_pages = max_pdf_pages
        self.max_chars_per_file = max_chars_per_file
    
    def process_file(self, uploaded_file) -> Optional[Dict[str, str]]:
        file_extension = uploaded_file.name.lower().split('.')[-1]
        
        if file_extension == 'pdf':
            return self._process_pdf(uploaded_file)
        elif file_extension == 'txt':
            return self._process_generic(uploaded_file, doctype="txt")
        elif file_extension == 'md':
            return self._process_generic(uploaded_file, doctype="md")
        else: 
            return None
    
    def _process_pdf(self, uploaded_file) -> Optional[Dict[str, str]]:
        pdf_bytes = uploaded_file.read()    
        pdf_stream = io.BytesIO(pdf_bytes)
        doc = pymupdf.open(pdf_stream) # open a document
        reader = doc.pages
        total_pages = len(reader.pages)
        
        pages_to_process = min(self.max_pdf_pages, total_pages)
        
        text_content = ""
        for page_num in range(pages_to_process):
            page = reader.pages[page_num]
            page_text = page.get_text()
            text_content += f"\n--- Page {page_num + 1} ---\n{page_text}\n"
        
        cleaned_text = self._clean(text_content)
        limited_text = self._limit_text_length(cleaned_text)
        
        return {
            'filename': uploaded_file.name,
            'content': limited_text,
            'file_type': 'pdf',
            'page_count': pages_to_process,
            'total_pages': total_pages
        }
        
    def _process_generic(self, uploaded_file, doctype="text") -> Dict[str, str]:
        content = None
        for encoding in ['utf-8', 'latin-1', 'cp1252']:
            try:
                content = uploaded_file.read().decode(encoding)
                break
            except UnicodeDecodeError:
                uploaded_file.seek(0) 
                continue
        
        cleaned_text = self._clean(content)
        limited_text = self._limit_text_length(cleaned_text)
        
        return {
            'filename': uploaded_file.name,
            'content': limited_text,
            'file_type': doctype,
            'page_count': 1
        }
    
    def _clean(self, text: str) -> str:
        text = re.sub(r'\n\s*\n\s*\n+', '\n\n', text)
        text = re.sub(r'[ \t]+', ' ', text)
        text = ''.join(char for char in text if ord(char) >= 32 or char in '\n\t')
        
        return text.strip()
    
    def _limit_text_length(self, text: str) -> str:
        if len(text) <= self.max_chars_per_file:
            return text        
        truncated_text = text[:self.max_chars_per_file]
        truncated_text += f"\n\n[Content truncated at {len(truncated_text)} characters due to length limits]"
        return truncated_text
    
    def format_files_for_conversation(self, processed_file: Dict[str, str]) -> str:
        context_parts = []
        context_parts.append("## Uploaded Document\n")
        
        filename = processed_file['filename']
        content = processed_file['content']
        file_type = processed_file['file_type']
        
        context_parts.append(f"### Document: {filename} ({file_type.upper()})")
        
        if file_type == 'pdf' and 'page_count' in processed_file:
            context_parts.append(f"*Pages processed: {processed_file['page_count']}/{processed_file.get('total_pages', '?')}*")
        
        context_parts.append(f"```\n{content}\n```")
        context_parts.append("")
        return "\n".join(context_parts)
    
    def inject_documents_into_messages(self, messages: List[Dict[str, str]], processed_file: Dict[str, str]) -> List[Dict[str, str]]:
        documents_context = self.format_files_for_conversation(processed_file)
        document_message = {"role": "system", "content": f"The user has added some documents for context:\n\n{documents_context}\n\nUse this information."}
        modified_messages = []
        
        for message in messages:
            if message["role"] == "system":
                modified_messages.append(message)
            else:
                modified_messages.append(document_message)
                modified_messages.append(message)
        
        return modified_messages
