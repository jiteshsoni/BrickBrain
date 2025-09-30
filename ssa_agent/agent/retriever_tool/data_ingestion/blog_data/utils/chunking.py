"""
Chunking
Splits markdown content into chunks. 
"""

from typing import List, Dict, Any, Union, Optional
import re
from langchain_text_splitters import MarkdownHeaderTextSplitter, RecursiveCharacterTextSplitter
from transformers import AutoTokenizer

class Chunker:    
    def __init__(self, max_chunk_size: int = 800, chunk_overlap: int = 100, min_chunk_size: int = 200):
        self.max_chunk_size = max_chunk_size
        self.chunk_overlap = chunk_overlap
        self.min_chunk_size = min_chunk_size
        
        self.tokenizer = AutoTokenizer.from_pretrained("openai-community/openai-gpt")
        
        self.headers_to_split_on = [
            ("####", "Header 4"),
        ]
        
        self.markdown_splitter = MarkdownHeaderTextSplitter(
            headers_to_split_on=self.headers_to_split_on
        )
        
        self.text_splitter = RecursiveCharacterTextSplitter.from_huggingface_tokenizer(
            self.tokenizer, 
            chunk_size=self.max_chunk_size, 
            chunk_overlap=self.chunk_overlap,
            separators=["\n\n", "\n", ". ", "! ", "? "]
        )
    
    def estimate_tokens(self, text: str) -> int:
        return len(self.tokenizer.encode(text))

    def chunk_document(self, content: str, document_title: str = "") -> List[str]:
        """
        Chunk document content into a list of text chunks. Used within UDF. 
        """
        if not content or len(content.strip()) < 100:
            return []

        if not document_title:
            title_match = re.search(r'^#\s+(.+)$', content, re.MULTILINE)
            if title_match:
                document_title = title_match.group(1).strip()
        
        header_chunks = self.markdown_splitter.split_text(content)
        final_chunks = []        
        combined_chunks = self._combine_small_sections(header_chunks)
        
        for chunk_data in combined_chunks:
            chunk_text = chunk_data['text']
            chunk_title = chunk_data.get('title', '')
            
            if not chunk_title and document_title:
                chunk_title = document_title
            
            if self.estimate_tokens(chunk_text) > self.max_chunk_size:
                sub_chunks = self.text_splitter.split_text(chunk_text)
                for sub_chunk in sub_chunks:
                    if len(sub_chunk.strip()) >= self.min_chunk_size:
                        final_chunks.append(sub_chunk.strip())
            else:    
                if len(chunk_text.strip()) >= self.min_chunk_size:
                    final_chunks.append(chunk_text.strip())
        
        return final_chunks

    def _combine_small_sections(self, header_chunks: List[Any]) -> List[Dict[str, str]]:
        """
        Combine smaller chunks together. 
        """
        if not header_chunks:
            return []
        
        combined = []
        current_text = ""
        current_title = ""
        
        for chunk in header_chunks:
            chunk_text = chunk.page_content
            chunk_metadata = chunk.metadata
            
            chunk_title = chunk_metadata.get("Header 4", "")
            
            potential_text = current_text + "\n\n" + chunk_text if current_text else chunk_text
            potential_tokens = self.estimate_tokens(potential_text)
            
            if current_text and potential_tokens > self.max_chunk_size:
                combined.append({
                    'text': current_text.strip(),
                    'title': current_title
                })
                current_text = chunk_text
                current_title = chunk_title
            else:
                if current_text:
                    current_text += "\n\n" + chunk_text
                    if not current_title:
                        current_title = chunk_title
                else:
                    current_text = chunk_text
                    current_title = chunk_title
        
        if current_text:
            combined.append({
                'text': current_text.strip(),
                'title': current_title
            })
        
        return combined