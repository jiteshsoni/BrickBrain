#!/usr/bin/env python3
"""
Blog Scraper Runner
Dynamically detects blogs from websites and downloads content as markdown
"""

from blog_scraper import BlogScraper
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def read_urls_from_file(file_path: str = "urls.txt") -> list:
    """Read URLs from a text file, ignoring comments and empty lines"""
    urls = []
    
    if not Path(file_path).exists():
        return urls
    
    with open(file_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            
            # Skip empty lines and comments
            if not line or line.startswith('#'):
                continue
            
            # Basic URL validation
            if line.startswith(('http://', 'https://')):
                urls.append(line)
            else:
                logger.warning(f"Invalid URL format on line {line_num}: {line}")
    
    return urls

def main():
    """Run the blog scraper with the specified websites"""
    
    # Base websites to discover blogs from
    websites = [
        "https://www.databricksters.com/",
        "https://www.canadiandataguy.com/"
    ]
    
    # Hardcoded URLs - add your specific URLs here
    hardcoded_urls = [
        # Example URLs - replace with your actual URLs
        # "https://example.com/specific-blog-post",
        # "https://another-site.com/article",
        # "https://www.databricksters.com/p/specific-post",
        # "https://www.canadiandataguy.com/p/another-post",
    ]
    
    # Also try to read URLs from urls.txt file if it exists
    file_urls = read_urls_from_file("urls.txt")
    if file_urls:
        hardcoded_urls.extend(file_urls)
        logger.info(f"Loaded {len(file_urls)} URLs from urls.txt")
    
    logger.info("Starting blog scraper...")
    logger.info(f"Base websites: {websites}")
    logger.info(f"Hardcoded URLs: {len(hardcoded_urls)} URLs")
    
    if hardcoded_urls:
        logger.info("Hardcoded URLs:")
        for url in hardcoded_urls:
            logger.info(f"  - {url}")
    
    # Create and run scraper
    scraper = BlogScraper(websites, delta_table_path="default.blog_content")
    scraper.scrape_all_blogs(hardcoded_urls=hardcoded_urls)
    
    logger.info("Scraping completed! Check the Delta table for results.")
    logger.info("You can query the table using:")
    logger.info("spark.table('default.blog_content').show()")

if __name__ == "__main__":
    main()
