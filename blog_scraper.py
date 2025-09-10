#!/usr/bin/env python3
"""
Blog Scraper for Databricksters and Canadian Data Guy
Dynamically detects all blog posts and downloads content as markdown
"""

import requests
from bs4 import BeautifulSoup
import time
import os
import re
from urllib.parse import urljoin, urlparse
from pathlib import Path
import logging
from typing import List, Set, Dict
import json
from datetime import datetime
import html2text
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import databricks.connect as dbc

# Configure logging (only if not already configured, e.g., in Databricks)
if not logging.getLogger().handlers:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler()  # Remove FileHandler for Databricks compatibility
        ]
    )
logger = logging.getLogger(__name__)

class BlogScraper:
    def __init__(self, base_urls: List[str], delta_table_path: str = "blog_content_delta"):
        self.base_urls = base_urls
        self.delta_table_path = delta_table_path
        self.metadata_table_path = delta_table_path + "_metadata"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.h2t = html2text.HTML2Text()
        self.h2t.ignore_links = False
        self.h2t.ignore_images = False
        self.h2t.body_width = 0  # Don't wrap lines
        
        # Initialize Spark session with Delta Lake
        self.spark = self._init_spark_session()
        
        # Initialize the Delta tables
        self._init_delta_table()
        self._init_metadata_table()
    
    def _init_spark_session(self):
        """Initialize Spark session using Databricks Connect"""
        try:
            # Use Databricks Connect to connect to remote cluster
            spark = dbc.DatabricksSession.builder.getOrCreate()
            logger.info("Connected to Databricks cluster via Databricks Connect")
            return spark
        except Exception as e:
            logger.error(f"Failed to connect to Databricks cluster: {e}")
            logger.info("Make sure you have configured Databricks Connect properly")
            raise
    
    def _init_delta_table(self):
        """Initialize or create the Delta table if it doesn't exist"""
        try:
            # Check if table exists
            self.spark.table(self.delta_table_path)
            logger.info(f"Delta table already exists: {self.delta_table_path}")
        except Exception:
            # Table doesn't exist, create it
            logger.info(f"Creating new Delta table: {self.delta_table_path}")
            schema = StructType([
                StructField("url", StringType(), False),
                StructField("title", StringType(), True),
                StructField("markdown_content", StringType(), True),
                StructField("domain", StringType(), True),
                StructField("scraped_at", StringType(), True)
            ])
            
            # Create empty DataFrame with schema
            empty_df = self.spark.createDataFrame([], schema)
            
            # Write to Delta table
            empty_df.write \
                .format("delta") \
                .mode("overwrite") \
                .option("overwriteSchema", "true") \
                .saveAsTable(self.delta_table_path)
            
            logger.info(f"Created new Delta table: {self.delta_table_path}")
    
    def _init_metadata_table(self):
        """Initialize or create the metadata Delta table if it doesn't exist"""
        try:
            # Check if metadata table exists
            self.spark.table(self.metadata_table_path)
            logger.info(f"Metadata table already exists: {self.metadata_table_path}")
        except Exception:
            # Table doesn't exist, create it
            logger.info(f"Creating new metadata table: {self.metadata_table_path}")
            schema = StructType([
                StructField("metadata_type", StringType(), False),
                StructField("metadata_content", StringType(), True),
                StructField("created_at", StringType(), True)
            ])
            
            # Create empty DataFrame with schema
            empty_df = self.spark.createDataFrame([], schema)
            
            # Write to Delta table
            empty_df.write \
                .format("delta") \
                .mode("overwrite") \
                .option("overwriteSchema", "true") \
                .saveAsTable(self.metadata_table_path)
            
            logger.info(f"Created new metadata table: {self.metadata_table_path}")
    
    def get_sitemap_urls(self, base_url: str) -> List[str]:
        """Try to find sitemap and extract URLs"""
        sitemap_urls = []
        domain = urlparse(base_url).netloc
        
        # Common sitemap locations
        sitemap_paths = [
            '/sitemap.xml',
            '/sitemap_index.xml',
            '/sitemaps/sitemap.xml',
            '/wp-sitemap.xml',
            '/sitemap-posts.xml'
        ]
        
        for path in sitemap_paths:
            try:
                sitemap_url = urljoin(base_url, path)
                response = self.session.get(sitemap_url, timeout=10)
                if response.status_code == 200:
                    logger.info(f"Found sitemap at {sitemap_url}")
                    soup = BeautifulSoup(response.content, 'html.parser')
                    
                    # Extract URLs from sitemap
                    for loc in soup.find_all('loc'):
                        url = loc.text.strip()
                        logger.debug(f"Found URL in sitemap: {url}")
                        if domain in url:
                            logger.debug(f"URL contains domain, checking if blog post: {url}")
                            if self.is_blog_post(url):
                                logger.info(f"Identified as blog post: {url}")
                                sitemap_urls.append(url)
                            else:
                                logger.debug(f"Not identified as blog post: {url}")
                    
                    if sitemap_urls:
                        break
            except Exception as e:
                logger.debug(f"Could not access sitemap at {sitemap_url}: {e}")
                continue
        
        return sitemap_urls
    
    def is_blog_post(self, url: str) -> bool:
        """Determine if URL is likely a blog post"""
        # Substack-specific patterns
        substack_patterns = [
            r'/p/[^/]+$',      # Substack post pattern: /p/post-title
            r'/p/[^/]+/',      # Substack post pattern with trailing slash
        ]
        
        # Common blog post patterns
        blog_patterns = [
            r'/\d{4}/\d{2}/',  # Date pattern
            r'/post/',         # Post in URL
            r'/article/',      # Article in URL
            r'/blog/',         # Blog in URL
            r'/\d{4}/\d{2}/\d{2}/',  # Full date pattern
        ]
        
        # Exclude common non-blog pages
        exclude_patterns = [
            r'/tag/',
            r'/category/',
            r'/author/',
            r'/page/',
            r'/search',
            r'/feed',
            r'/rss',
            r'/sitemap',
            r'/about',
            r'/contact',
            r'/privacy',
            r'/terms',
            r'/subscribe',
            r'/newsletter',
            r'/archive$',      # Archive page
            r'/podcast$',      # Podcast page
        ]
        
        # Check exclusions first
        for pattern in exclude_patterns:
            if re.search(pattern, url, re.IGNORECASE):
                return False
        
        # Check Substack patterns first (most specific)
        for pattern in substack_patterns:
            if re.search(pattern, url, re.IGNORECASE):
                return True
        
        # Check other blog patterns
        for pattern in blog_patterns:
            if re.search(pattern, url, re.IGNORECASE):
                return True
        
        return False
    
    def discover_blog_urls(self, base_url: str) -> Set[str]:
        """Discover blog post URLs from a website"""
        discovered_urls = set()
        domain = urlparse(base_url).netloc
        
        logger.info(f"Discovering blog URLs for {base_url}")
        
        # Method 1: Try sitemap first
        sitemap_urls = self.get_sitemap_urls(base_url)
        discovered_urls.update(sitemap_urls)
        
        # Method 2: Crawl main page and look for blog links
        try:
            response = self.session.get(base_url, timeout=10)
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Find all links
                for link in soup.find_all('a', href=True):
                    href = link['href']
                    full_url = urljoin(base_url, href)
                    
                    # Check if it's a blog post URL
                    if (domain in full_url and 
                        self.is_blog_post(full_url) and 
                        full_url not in discovered_urls):
                        discovered_urls.add(full_url)
                
                # Look for pagination or "load more" links
                pagination_selectors = [
                    'a[href*="page"]',
                    'a[href*="next"]',
                    'a[href*="more"]',
                    '.pagination a',
                    '.load-more',
                    '.next-page'
                ]
                
                for selector in pagination_selectors:
                    for link in soup.select(selector):
                        if link.get('href'):
                            pagination_url = urljoin(base_url, link['href'])
                            if domain in pagination_url:
                                # Recursively crawl pagination pages
                                try:
                                    time.sleep(1)  # Be respectful
                                    pagination_response = self.session.get(pagination_url, timeout=10)
                                    if pagination_response.status_code == 200:
                                        pagination_soup = BeautifulSoup(pagination_response.content, 'html.parser')
                                        for pagination_link in pagination_soup.find_all('a', href=True):
                                            pagination_href = pagination_link['href']
                                            pagination_full_url = urljoin(base_url, pagination_href)
                                            if (domain in pagination_full_url and 
                                                self.is_blog_post(pagination_full_url) and 
                                                pagination_full_url not in discovered_urls):
                                                discovered_urls.add(pagination_full_url)
                                except Exception as e:
                                    logger.debug(f"Error crawling pagination {pagination_url}: {e}")
        
        except Exception as e:
            logger.error(f"Error discovering URLs for {base_url}: {e}")
        
        logger.info(f"Discovered {len(discovered_urls)} blog URLs for {base_url}")
        return discovered_urls
    
    def download_and_convert_to_markdown(self, url: str) -> Dict:
        """Download a blog post and convert to markdown"""
        try:
            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Remove unwanted elements
            for element in soup(['script', 'style', 'nav', 'footer', 'header', 'aside', 'advertisement']):
                element.decompose()
            
            # Try to find the main content (Substack-specific selectors first)
            content_selectors = [
                '.post-content',           # Substack post content
                '.entry-content',          # Substack entry content
                '[data-testid="post-content"]',  # Substack test ID
                '.post',                   # General post selector
                'article',                 # HTML5 article tag
                '.blog-content',           # Blog content
                '.content',                # General content
                'main',                    # HTML5 main tag
                '.article-content'         # Article content
            ]
            
            main_content = None
            for selector in content_selectors:
                main_content = soup.select_one(selector)
                if main_content:
                    break
            
            if not main_content:
                main_content = soup.find('body')
            
            # Extract title (Substack-specific selectors first)
            title = None
            title_selectors = [
                'h1.post-title',           # Substack post title
                'h1.entry-title',          # Substack entry title
                '[data-testid="post-title"]',  # Substack test ID
                'h1',                      # General h1
                '.post-title',             # Post title class
                '.entry-title',            # Entry title class
                '.blog-title',             # Blog title class
                'title'                    # HTML title tag
            ]
            for selector in title_selectors:
                title_elem = soup.select_one(selector)
                if title_elem:
                    title = title_elem.get_text().strip()
                    break
            
            if not title:
                title = urlparse(url).path.split('/')[-1].replace('-', ' ').title()
            
            # Extract metadata
            metadata = {
                'url': url,
                'title': title,
                'scraped_at': datetime.now().isoformat(),
                'domain': urlparse(url).netloc
            }
            
            # Convert to markdown
            markdown_content = self.h2t.handle(str(main_content))
            
            # Clean up markdown
            markdown_content = re.sub(r'\n\s*\n\s*\n', '\n\n', markdown_content)  # Remove excessive newlines
            markdown_content = markdown_content.strip()
            
            return {
                'metadata': metadata,
                'content': markdown_content,
                'success': True
            }
            
        except Exception as e:
            logger.error(f"Error downloading {url}: {e}")
            return {
                'metadata': {'url': url, 'error': str(e)},
                'content': '',
                'success': False
            }
    
    def save_blog_post(self, blog_data: Dict, domain: str):
        """Save blog post to Delta table using merge (upsert)"""
        if not blog_data['success']:
            return
        
        metadata = blog_data['metadata']
        content = blog_data['content']
        
        # Prepare data for Delta table
        row_data = {
            "url": metadata['url'],
            "title": metadata['title'],
            "markdown_content": content,
            "domain": metadata['domain'],
            "scraped_at": metadata['scraped_at']
        }
        
        # Create DataFrame with single row
        df = self.spark.createDataFrame([row_data])
        
        # Merge to Delta table (upsert based on URL)
        self._merge_blog_post(df)
        
        logger.info(f"Saved/updated in Delta table: {metadata['url']}")
    
    def _merge_blog_post(self, new_df):
        """Merge new blog post data into existing table"""
        try:
            # Create temporary view for merge
            new_df.createOrReplaceTempView("new_blog_data")
            
            # Perform merge operation
            merge_sql = f"""
            MERGE INTO {self.delta_table_path} AS target
            USING new_blog_data AS source
            ON target.url = source.url
            WHEN MATCHED THEN
                UPDATE SET 
                    title = source.title,
                    markdown_content = source.markdown_content,
                    domain = source.domain,
                    scraped_at = source.scraped_at
            WHEN NOT MATCHED THEN
                INSERT (url, title, markdown_content, domain, scraped_at)
                VALUES (source.url, source.title, source.markdown_content, source.domain, source.scraped_at)
            """
            
            self.spark.sql(merge_sql)
            
        except Exception as e:
            logger.error(f"Error during merge operation: {e}")
            # Fallback to append if merge fails
            new_df.write \
                .format("delta") \
                .mode("append") \
                .saveAsTable(self.delta_table_path)
    
    def _save_url_list_to_delta(self, unique_urls: List[str], hardcoded_urls: List[str] = None):
        """Save URL list information to metadata table"""
        url_list_data = {
            "metadata_type": "URL_LIST",
            "metadata_content": json.dumps({
                'urls': unique_urls,
                'scraped_at': datetime.now().isoformat(),
                'total_count': len(unique_urls),
                'hardcoded_urls_count': len(hardcoded_urls) if hardcoded_urls else 0,
                'discovered_urls_count': len(unique_urls) - (len(hardcoded_urls) if hardcoded_urls else 0)
            }, indent=2),
            "created_at": datetime.now().isoformat()
        }
        
        df = self.spark.createDataFrame([url_list_data])
        df.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable(self.metadata_table_path)
        
        logger.info("Saved URL list metadata to metadata table")
    
    def _save_summary_to_delta(self, unique_urls: List[str], successful_downloads: int, 
                              failed_downloads: int, hardcoded_urls: List[str] = None):
        """Save scraping summary to metadata table"""
        summary_data = {
            "metadata_type": "SCRAPING_SUMMARY",
            "metadata_content": json.dumps({
                'total_urls': len(unique_urls),
                'successful_downloads': successful_downloads,
                'failed_downloads': failed_downloads,
                'websites': self.base_urls,
                'hardcoded_urls_count': len(hardcoded_urls) if hardcoded_urls else 0,
                'discovered_urls_count': len(unique_urls) - (len(hardcoded_urls) if hardcoded_urls else 0),
                'completed_at': datetime.now().isoformat()
            }, indent=2),
            "created_at": datetime.now().isoformat()
        }
        
        df = self.spark.createDataFrame([summary_data])
        df.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable(self.metadata_table_path)
        
        logger.info("Saved scraping summary to metadata table")
    
    def scrape_all_blogs(self, hardcoded_urls: List[str] = None):
        """Main method to scrape all blogs from all websites"""
        # Truncate the Delta table at the beginning
        logger.info("Truncating Delta table...")
        self._init_delta_table()  # This overwrites the table with empty schema
        
        all_urls = []
        
        # Add hardcoded URLs first
        if hardcoded_urls:
            logger.info(f"Adding {len(hardcoded_urls)} hardcoded URLs")
            all_urls.extend(hardcoded_urls)
        
        # Discover URLs from all websites
        for base_url in self.base_urls:
            urls = self.discover_blog_urls(base_url)
            all_urls.extend(urls)
            time.sleep(2)  # Be respectful between websites
        
        # Remove duplicates
        unique_urls = list(set(all_urls))
        logger.info(f"Total unique blog URLs found: {len(unique_urls)}")
        
        # Save URL list to Delta table as well
        self._save_url_list_to_delta(unique_urls, hardcoded_urls)
        
        # Download and convert each blog post
        successful_downloads = 0
        failed_downloads = 0
        
        for i, url in enumerate(unique_urls, 1):
            logger.info(f"Processing {i}/{len(unique_urls)}: {url}")
            
            blog_data = self.download_and_convert_to_markdown(url)
            domain = urlparse(url).netloc.replace('www.', '')
            
            if blog_data['success']:
                self.save_blog_post(blog_data, domain)
                successful_downloads += 1
            else:
                failed_downloads += 1
                logger.error(f"Failed to download: {url}")
            
            # Be respectful - add delay between requests
            time.sleep(1)
        
        logger.info(f"Scraping complete! Success: {successful_downloads}, Failed: {failed_downloads}")
        
        # Save summary to Delta table
        self._save_summary_to_delta(unique_urls, successful_downloads, failed_downloads, hardcoded_urls)
        
        # Show final table count
        final_count = self.spark.table(self.delta_table_path).count()
        logger.info(f"Final Delta table contains {final_count} records")

def main():
    """Main function"""
    # Static list of URLs as requested
    websites = [
        "https://www.databricksters.com/",
        "https://www.canadiandataguy.com/"
    ]
    
    # Create scraper instance
    scraper = BlogScraper(websites, output_dir="blog_content")
    
    # Start scraping
    logger.info("Starting blog scraping process...")
    scraper.scrape_all_blogs()
    logger.info("Blog scraping completed!")

if __name__ == "__main__":
    main()
