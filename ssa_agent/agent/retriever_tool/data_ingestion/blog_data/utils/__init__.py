"""
Helper utilities for Substack blog data ingestion.
"""

# Lazy imports to avoid loading heavy dependencies when not needed
# Import directly from submodules: from utils.chunking import Chunker

__all__ = [
    'scrape_blogs',
    'Chunker',
]

def __getattr__(name):
    """Lazy loading of modules to avoid importing unnecessary dependencies."""
    if name == 'scrape_blogs':
        from .blog_scraper import scrape_blogs
        return scrape_blogs
    elif name == 'Chunker':
        from .chunking import Chunker
        return Chunker
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")