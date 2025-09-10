#!/usr/bin/env python3
"""
Test script to verify blog scraper works with Databricks Connect
"""

import logging
from blog_scraper import BlogScraper

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_scraper():
    """Test the blog scraper with Databricks Connect"""
    try:
        # Test parameters
        websites = ["https://www.databricksters.com/", "https://www.canadiandataguy.com/"]
        delta_table_name = "main.default.blog_content_test"
        
        logger.info(f"Testing with websites: {websites}")
        logger.info(f"Using Delta table: {delta_table_name}")
        
        # Initialize scraper
        scraper = BlogScraper(websites, delta_table_path=delta_table_name)
        
        # Read hardcoded URLs
        hardcoded_urls = []
        try:
            with open('urls.txt', 'r') as f:
                hardcoded_urls = [line.strip() for line in f if line.strip() and not line.startswith('#')]
            logger.info(f"Loaded {len(hardcoded_urls)} hardcoded URLs")
        except FileNotFoundError:
            logger.info("No urls.txt file found, using only discovered URLs")
        
        # Test with just 2 specific URLs
        test_urls = [
            "https://www.databricksters.com/p/chat-with-your-data-in-slack-using",
            "https://www.canadiandataguy.com/p/how-do-i-think-about-setting-spark"
        ]
        
        logger.info(f"Testing with 2 specific URLs: {test_urls}")
        
        # Initialize the table
        scraper._init_delta_table()
        
        # Process just these 2 URLs
        for i, url in enumerate(test_urls, 1):
            logger.info(f"Processing test {i}/{len(test_urls)}: {url}")
            try:
                markdown_content, title = scraper.download_and_convert_to_markdown(url)
                if markdown_content and title:
                    scraper._merge_blog_post(url, markdown_content, title)
                    logger.info(f"Successfully processed: {title}")
                else:
                    logger.warning(f"No content extracted from: {url}")
            except Exception as e:
                logger.error(f"Failed to process {url}: {e}")
        
        # Verify results
        df = scraper.spark.table(delta_table_name)
        total_records = df.count()
        logger.info(f"Total records in table: {total_records}")
        
        # Show sample
        df.show(5, truncate=False)
        
        logger.info("Blog scraping test completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_scraper()
    exit(0 if success else 1)
