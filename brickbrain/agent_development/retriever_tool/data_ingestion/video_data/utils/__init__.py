"""
Utilities for video data ingestion
"""

# Lazy imports to avoid loading heavy dependencies when not needed
# Import directly from submodules: from utils.cleaner import TranscriptCleaner

__all__ = [
    'YouTubeClient',
    'TranscriptClient', 
    'SpellChecker',
    'TranscriptCleaner',
]

def __getattr__(name):
    """Lazy loading of modules to avoid importing unnecessary dependencies."""
    if name == 'YouTubeClient':
        from .fetch_data import YouTubeClient
        return YouTubeClient
    elif name == 'TranscriptClient':
        from .fetch_data import TranscriptClient
        return TranscriptClient
    elif name == 'SpellChecker':
        from .cleaner import SpellChecker
        return SpellChecker
    elif name == 'TranscriptCleaner':
        from .cleaner import TranscriptCleaner
        return TranscriptCleaner
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")
