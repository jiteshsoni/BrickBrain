"""
Chunking utilities 

Chunks markdown files by header and size. 
"""

from typing import List, Dict, Any
import uuid
import re
from datetime import datetime
from langchain_text_splitters import MarkdownHeaderTextSplitter, RecursiveCharacterTextSplitter
from transformers import AutoTokenizer

class Chunker:    
    def __init__(self, max_chunk_size: int = 500, chunk_overlap: int = 50):
        self.max_chunk_size = max_chunk_size
        self.chunk_overlap = chunk_overlap
        
        self.tokenizer = AutoTokenizer.from_pretrained("openai-community/openai-gpt")
        self.headers_to_split_on = [
            ("#", "Header 1"),
            ("##", "Header 2"), 
            ("###", "Header 3"),
        ]
        
        self.markdown_splitter = MarkdownHeaderTextSplitter(
            headers_to_split_on=self.headers_to_split_on
        )
        self.text_splitter = RecursiveCharacterTextSplitter.from_huggingface_tokenizer(
            self.tokenizer, 
            chunk_size=self.max_chunk_size, 
            chunk_overlap=self.chunk_overlap,
            separators=["\n\n", "\n", ".", "!", "?", ",", " ", ""]
        )
    
    def estimate_tokens(self, text: str) -> int:
        return len(self.tokenizer.encode(text))
 
    def chunk_document(self, content: str) -> List[str]:
        """Chunk document content into a list of text chunks.
        
        :param content: Document content to chunk
        :return: List of text chunks
        """
        if not content or len(content.strip()) < 100:
            return []
        
        content = self._prepare_markdown_for_chunking(content)
        header_chunks = self.markdown_splitter.split_text(content)
        final_chunks = []
        
        for chunk in header_chunks:
            chunk_text = chunk.page_content
            
            # Split large chunks further
            if self.estimate_tokens(chunk_text) > self.max_chunk_size:
                sub_chunks = self.text_splitter.split_text(chunk_text)
                
                for sub_chunk in sub_chunks:
                    if len(sub_chunk.strip()) >= 50:
                        final_chunks.append(sub_chunk.strip())
            else:
                if len(chunk_text.strip()) >= 50:
                    final_chunks.append(chunk_text.strip())
        
        return final_chunks
        
    def _prepare_markdown_for_chunking(self, content: str) -> str:
        """Prepare markdown content for optimal chunking."""
        content = re.sub(r'\n\s*\n\s*\n+', '\n\n', content)
        content = re.sub(r'[ \t]+', ' ', content)
        content = re.sub(r'(\n#{1,6}\s+[^\n]+)', r'\n\n\1', content)
        content = re.sub(r'\n\s*[-*+]\s*\n', '\n', content) 
        content = re.sub(r'\n\s*\d+\.\s*\n', '\n', content) 
        patterns_to_remove = [
            r'\[Share\]\([^)]*\)',  
            r'\[Subscribe\]\([^)]*\)',  
            r'\[Follow\]\([^)]*\)',  
            r'\*\*Subscribe\*\*', 
            r'\*\*Follow\*\*', 
        ]
        
        for pattern in patterns_to_remove:
            content = re.sub(pattern, '', content, flags=re.IGNORECASE)
        return content.strip()
