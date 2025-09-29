"""
Blog scraper for Substack blogs. 

Downloads all content as markdown files. 
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
from datetime import datetime
import html2text

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
    def __init__(self, base_urls: List[str]):
        if isinstance(base_urls, str):
            self.base_urls = [base_urls]
            logger.warning(f"base_urls was passed as string, converted to list: {self.base_urls}")
        elif isinstance(base_urls, list):
            self.base_urls = base_urls
        else:
            raise ValueError(f"base_urls must be a string or list of strings, got {type(base_urls)}")
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.h2t = html2text.HTML2Text()
        self.h2t.ignore_links = False
        self.h2t.ignore_images = False
        self.h2t.body_width = 0  # Don't wrap lines
    
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
            unwanted_selectors = [
                'script', 'style', 'nav', 'footer', 'header', 'aside', 'advertisement',
                '.advertisement', '.ads', '.ad', '.sidebar', '.social-share', '.share-buttons',
                '.newsletter-signup', '.subscribe', '.subscribe-widget', '.email-signup',
                '.author-bio', '.author-card', '.author-info', '.bio-card',
                '.related-posts', '.recommended', '.you-might-also-like',
                '.pagination', '.nav-links', '.page-nav',
                '.comments', '.comment-section', '.comment-form',
                '.tags', '.tag-list', '.category-list', '.post-meta',
                '.breadcrumbs', '.breadcrumb',
                '.social-media', '.social-links', '.follow-buttons',
                '.popup', '.modal', '.overlay',
                '.cookie-notice', '.cookie-banner',
                '.search-box', '.search-form',
                '[data-testid="authorByline"]', 
                '[data-testid="like-button"]',   
                '[data-testid="share"]',         
                '[data-testid="subscribe"]',    
                '.post-header-meta',             
                '.post-date', '.publish-date',   
                '.reading-time',                 
                'img[src*="avatar"]',            
                'img[src*="profile"]',           
                'img[alt*="avatar"]',            
                'img[alt*="profile"]'           
            ]
            
            for selector in unwanted_selectors:
                for element in soup.select(selector):
                    element.decompose()
            
            for element in soup.find_all():
                if element.name:
                    classes = element.get('class', [])
                    if any(keyword in ' '.join(classes).lower() for keyword in 
                           ['subscribe', 'share', 'social', 'author', 'meta', 'ad', 'popup', 'modal']):
                        element.decompose()
                        continue
                    
                    text = element.get_text().strip().lower()
                    if any(keyword in text for keyword in 
                           ['subscribe', 'follow me', 'share this', 'like this post', 'sign up']):
                        if len(text) < 100:  # Only remove short elements with these phrases
                            element.decompose()
                            continue
            
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
            
            if main_content:
                for element in main_content.find_all(['iframe', 'embed', 'object']):
                    element.decompose()
                
                for element in main_content.find_all(['p', 'div']):
                    if not element.get_text().strip():
                        element.decompose()
            
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
            
            markdown_content = self._clean_markdown_content(markdown_content)
            
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
    
    def _clean_markdown_content(self, content: str) -> str:
        if not content:
            return ""
        
        patterns_to_remove = [
            r'\[!\[.*?\]\(.*?\)\]\(.*?\)',  # Image links with complex markdown
            r'!\[.*?avatar.*?\]\(.*?\)',    # Avatar images
            r'!\[.*?profile.*?\]\(.*?\)',   # Profile images
            r'\*\*\[.*?\]\(.*?\)\*\*\s*\n',  # Bold linked author names
            r'\[.*?\]\(javascript:void\(0\)\)',  # JavaScript links
            r'\*\*Sep \d+, \d+\*\*',        # Date patterns
            r'\*\*\d+\*\*\s*\n',            # Numbers (like social counts)
            r'\[Share\]\(.*?\)',            # Share links
            r'\[\]\(.*?comments\)',         # Comment links
            r'Subscribe.*?\n',              # Subscribe text
            r'Sign up.*?\n',                # Sign up text
            r'Follow.*?\n',                 # Follow text
        ]
        
        for pattern in patterns_to_remove:
            content = re.sub(pattern, '', content, flags=re.IGNORECASE)
        
        content = re.sub(r'\n\s*\n\s*\n+', '\n\n', content)
        content = re.sub(r'[ \t]+', ' ', content)
        content = re.sub(r'\n\s+\n', '\n\n', content)
        
        lines = content.split('\n')
        cleaned_lines = []
        
        for line in lines:
            line = line.strip()
            if len(line) < 3:
                continue
            if any(keyword in line.lower() for keyword in 
                   ['subscribe', 'follow', 'share', 'comments', 'like this']):
                continue
            if re.match(r'^[\d\s\[\]()]+$', line):
                continue
            
            cleaned_lines.append(line)
        
        content = '\n'.join(cleaned_lines)
        
        return content.strip()
    
    def scrape_all_blogs(self, hardcoded_urls: List[str] = None):
        """Main method to scrape all blogs from all websites"""
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
        
        # Download and convert each blog post
        successful_downloads = 0
        failed_downloads = 0
        blog_posts_data = []
        
        for i, url in enumerate(unique_urls, 1):
            logger.info(f"Processing {i}/{len(unique_urls)}: {url}")
            
            blog_data = self.download_and_convert_to_markdown(url)
            domain = urlparse(url).netloc.replace('www.', '')
            
            if blog_data['success']:
                # Prepare data for DataFrame
                row_data = {
                    "url": blog_data['metadata']['url'],
                    "title": blog_data['metadata']['title'],
                    "markdown_content": blog_data['content'],
                    "domain": blog_data['metadata']['domain'],
                    "scraped_at": blog_data['metadata']['scraped_at']
                }
                blog_posts_data.append(row_data)
                successful_downloads += 1
                logger.info(f"Successfully processed: {blog_data['metadata']['title']}")
            else:
                failed_downloads += 1
                logger.error(f"Failed to download: {url}")
            
            # Be respectful - add delay between requests
            time.sleep(1)
        
        logger.info(f"Scraping complete! Success: {successful_downloads}, Failed: {failed_downloads}")
        
        # Create and return DataFrame
        if blog_posts_data:
            from pyspark.sql import SparkSession
            spark = SparkSession.getActiveSession()
            df = spark.createDataFrame(blog_posts_data)
            logger.info(f"Created DataFrame with {df.count()} blog posts")
            return df
        else:
            logger.warning("No blog posts were successfully scraped")
            from pyspark.sql import SparkSession
            from pyspark.sql.types import StructType, StructField, StringType
            spark = SparkSession.getActiveSession()
            # Return empty DataFrame with correct schema
            schema = StructType([
                StructField("url", StringType(), False),
                StructField("title", StringType(), True),
                StructField("markdown_content", StringType(), True),
                StructField("domain", StringType(), True),
                StructField("scraped_at", StringType(), True)
            ])
            empty_df = spark.createDataFrame([], schema)
            return empty_df
