"""
Blog scraper
Downloads all content as markdown files. 
"""

import requests
from bs4 import BeautifulSoup
import time
import re
from urllib.parse import urljoin, urlparse
import logging
from typing import List, Dict
from datetime import datetime
import html2text
import xml.etree.ElementTree as ET
import feedparser
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, MapType

if not logging.getLogger().handlers:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler()]
    )
logger = logging.getLogger(__name__)


class GetBlogs:
    def __init__(self, base_urls):
        if isinstance(base_urls, str):
            self.base_urls = base_urls.split(",")
        elif isinstance(base_urls, List[str]): 
            self.base_urls = base_urls
            
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })

    def is_substack_domain(self, url: str) -> bool:
        domain = urlparse(url).netloc.lower()
        return 'substack.com' in domain
    
    def is_medium_domain(self, url: str) -> bool:
        domain = urlparse(url).netloc.lower()
        return 'medium.com' in domain
    
    def is_substack_post(self, url: str) -> bool:
        substack_patterns = [r'/p/[^/]+$', r'/p/[^/]+/']
        exclude_patterns = [r'/archive$', r'/podcast$', r'/about$', r'/subscribe$']
        
        for pattern in exclude_patterns:
            if re.search(pattern, url, re.IGNORECASE):
                return False
        
        for pattern in substack_patterns:
            if re.search(pattern, url, re.IGNORECASE):
                return True
        
        return False

    def is_medium_post(self, url: str) -> bool:
        medium_patterns = [
            r'medium\.com/@[^/]+/[^/]+-[a-f0-9]+$',
            r'[^.]+\.medium\.com/[^/]+-[a-f0-9]+$',
            r'medium\.com/[^/@]+/[^/]+-[a-f0-9]+$',
        ]
        
        for pattern in medium_patterns:
            if re.search(pattern, url, re.IGNORECASE):
                return True
        return False
    
    def get_substack_sitemap_urls(self, base_url: str) -> List[str]:
        urls = []
        domain = urlparse(base_url).netloc
        sitemap_paths = ['/sitemap.xml', '/sitemap_index.xml']
        
        for path in sitemap_paths:
            try:
                sitemap_url = urljoin(base_url, path)
                response = self.session.get(sitemap_url, timeout=10)
                
                if response.status_code == 200:
                    logger.info(f"Found sitemap at {sitemap_url}")
                    soup = BeautifulSoup(response.content, 'html.parser')
                    
                    for loc in soup.find_all('loc'):
                        url = loc.text.strip()
                        if domain in url and self.is_substack_post(url):
                            urls.append(url)
                    
                    if urls:
                        break
                        
            except Exception as e:
                logger.debug(f"Could not access sitemap at {sitemap_url}: {e}")
        
        return urls
    
    def get_medium_rss_urls(self, base_url: str) -> List[str]:
        urls = []
        domain = urlparse(base_url).netloc.lower()
        rss_feed_urls = []
        
        if 'medium.com' in domain:
            if '/@' in base_url:
                username = base_url.split('/@')[1].rstrip('/')
                rss_feed_urls.append(f"https://medium.com/feed/@{username}")
            elif base_url.count('/') > 2:
                pub_name = base_url.rstrip('/').split('/')[-1]
                if pub_name and pub_name != 'medium.com':
                    rss_feed_urls.append(f"https://medium.com/feed/{pub_name}")
            
            rss_feed_urls.append(f"{base_url.rstrip('/')}/feed")
        
        elif '.medium.com' in domain:
            rss_feed_urls.append(f"{base_url.rstrip('/')}/feed")
        
        for rss_url in rss_feed_urls:
            try:
                logger.info(f"Trying RSS feed: {rss_url}")
                response = self.session.get(rss_url, timeout=10)
                
                if response.status_code == 200:
                    logger.info(f"Found RSS feed at {rss_url}")
                    
                    try:
                        feed = feedparser.parse(response.content)
                        for entry in feed.entries:
                            if hasattr(entry, 'link') and self.is_medium_post(entry.link):
                                urls.append(entry.link)
                    except Exception:
                        root = ET.fromstring(response.content)
                        for item in root.findall('.//item'):
                            link_elem = item.find('link')
                            if link_elem is not None and link_elem.text:
                                if self.is_medium_post(link_elem.text):
                                    urls.append(link_elem.text)
                    
                    break
                    
            except Exception as e:
                logger.debug(f"Could not access RSS feed at {rss_url}: {e}")
        
        return urls

    def discover_all_posts(self) -> List[str]:
        all_urls = set()
        
        for base_url in self.base_urls:
            logger.info(f"Discovering posts from {base_url}")
            initial_count = len(all_urls)
            
            sitemap_urls = self.get_substack_sitemap_urls(base_url)
            if sitemap_urls:
                all_urls.update(sitemap_urls)
                logger.info(f"Found {len(sitemap_urls)} URLs from Substack sitemap")
                        
            rss_urls = self.get_medium_rss_urls(base_url)
            if rss_urls:
                new_rss_urls = [u for u in rss_urls if u not in all_urls]
                all_urls.update(new_rss_urls)
                logger.info(f"Found {len(new_rss_urls)} URLs from Medium RSS")
            
            if len(all_urls) == initial_count:
                logger.warning(f"No posts discovered from {base_url}")
            
            time.sleep(1)
        
        unique_urls = list(all_urls)
        logger.info(f"Total unique URLs discovered: {len(unique_urls)}")
        return unique_urls

class BlogContentScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        self.h2t = html2text.HTML2Text()
        self.h2t.ignore_links = False
        self.h2t.ignore_images = False
        self.h2t.body_width = 0
    
    def scrape_url(self, url: str) -> Dict:
        try:
            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            self._remove_unwanted_elements(soup)
            main_content = self._extract_main_content(soup, url)
            title = self._extract_title(soup, url)
            
            metadata = {
                'url': url,
                'title': title,
                'scraped_at': datetime.now().isoformat(),
                'domain': urlparse(url).netloc
            }
            
            markdown_content = self.h2t.handle(str(main_content))
            logger.debug(f"Markdown length before cleaning: {len(markdown_content)}")
            
            markdown_content = self._clean_markdown(markdown_content, url)
            logger.debug(f"Markdown length after cleaning: {len(markdown_content)}")
            
            markdown_content = self._remove_subheading_metadata(markdown_content)
            logger.debug(f"Markdown length after metadata removal: {len(markdown_content)}")
            
            if not markdown_content or len(markdown_content) < 100:
                logger.warning(f"Very short or empty content for {url}")
            
            return {
                'metadata': metadata,
                'content': markdown_content,
                'success': True
            }
            
        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")
            return {
                'metadata': {'url': url, 'error': str(e)},
                'content': '',
                'success': False
            }
    
    def _remove_unwanted_elements(self, soup: BeautifulSoup):
        unwanted_selectors = [
            'script', 'style', 'nav', 'footer', 'header',
            '.advertisement', '.ads', '.social-share',
            '[data-testid="authorByline"]', '[data-testid="like-button"]',
            '[data-testid="clap-button"]', '[data-testid="follow-button"]',
            '.member-preview-paywall'
        ]
        
        for selector in unwanted_selectors:
            for element in soup.select(selector):
                element.decompose()
    
    def _extract_main_content(self, soup: BeautifulSoup, url: str):
        content_selectors = [
            '.post-content',
            '[data-testid="post-content"]',
            '[data-testid="story-content"]',
            'article section',
            'article',
            'main'
        ]
        
        for selector in content_selectors:
            content = soup.select_one(selector)
            if content:
                return content
        
        return soup.find('body')
    
    def _extract_title(self, soup: BeautifulSoup, url: str) -> str:
        title_selectors = [
            'h1.post-title',
            '[data-testid="post-title"]',
            'h1[data-testid="headline"]',
            'article h1',
            'h1'
        ]
        
        for selector in title_selectors:
            title_elem = soup.select_one(selector)
            if title_elem:
                return title_elem.get_text().strip()
        
        return urlparse(url).path.split('/')[-1].replace('-', ' ').title()
    
    def _clean_markdown(self, content: str, url: str) -> str:
        if not content:
            return ""
        
        patterns = [
            r'\[!\[.*?\]\(.*?\)\]\(.*?\)',
            r'!\[.*?avatar.*?\]\(.*?\)',
            r'!\[.*?profile.*?\]\(.*?\)',
            r'\[Share\]\(.*?\)',
            r'Subscribe.*?\n',
            r'\*\*\d+\*\*\s*claps?\s*\n',
            r'Thanks for reading.*?\n',
            r'PreviousNext',
            r'Previous\s*Next',
            r'\[.*?\]\(.*?utm_source=.*?\)',
            r'^\[.*?\]\(.*?\)\s*'
        ] 

        for pattern in patterns:
            content = re.sub(pattern, '', content, flags=re.IGNORECASE)
        
        content = re.sub(r'\n\s*\n\s*\n+', '\n\n', content)
        content = re.sub(r'[ \t]+', ' ', content)
        
        return content.strip()

    def _remove_subheading_metadata(self, content: str) -> str:
        medium_metadata_patterns = [
            r'\[.*?\]\(/@.*?source=post_page---byline--.*?\)',
            r'\d+ min read\s+·\s+[A-Z][a-z]{2,8}\s+\d{1,2},\s+\d{4}',
            r'\[.*?\]\(/m/signin\?.*?clap_footer.*?\)',
            r'\[.*?\]\(/m/signin\?.*?bookmark_footer.*?\)',
            r'\\--',  # Divider
            r'Listen\s+Share',
            r'Press enter or click to view image in full size',
            r'\*\*Authors:\*\*\[.*?\]\(https://www\.linkedin\.com/.*?\)',
        ]
        
        subheading_metadata_patterns = [
            r'\[.*?\]\(http.*?\)',
            r'[A-Z][a-z]{2,8}\s+\d{1,2},\s+\d{4}',
            r'^\d+$',
            r'\[.*?\]\(.*?/comments\)',
            r'^\)$|^\(.*?\)$',
            r'^\[\]\(.*?\)$'
        ]
        
        for pattern in medium_metadata_patterns:
            content = re.sub(pattern, '', content)
        
        lines = content.split('\n')
        result = []
        i = 0
        
        while i < len(lines):
            line = lines[i]
            result.append(line)
            if line.startswith('###') and not line.startswith('####'):
                i += 1
                skip_count = 0
                while i + skip_count < len(lines):
                    next_line = lines[i + skip_count].strip()
                    if not next_line:
                        result.append(lines[i + skip_count])
                        skip_count += 1
                        continue
                    
                    is_metadata = any(re.search(pattern, next_line) for pattern in subheading_metadata_patterns)
                    
                    if is_metadata:
                        skip_count += 1
                    else:
                        break
                
                i += skip_count
            else:
                i += 1
        
        content = '\n'.join(result)
        content = re.sub(r'\n{3,}', '\n\n', content)
        content = re.sub(r'^\s+', '', content)
        content = re.sub(r'\s+$', '', content)
        return content

    
    def scrape_multiple(self, urls: List[str]) -> List[Dict]:
        results = []
        
        for i, url in enumerate(urls, 1):
            logger.info(f"Scraping {i}/{len(urls)}: {url}")
            result = self.scrape_url(url)
            results.append(result)
            time.sleep(1)
        
        return results


def scrape_blogs(base_urls, spark):    
    discoverer = GetBlogs(base_urls)
    urls = discoverer.discover_all_posts()
    
    if not urls:
        logger.warning("No URLs discovered")
        return None
    
    scraper = BlogContentScraper()
    results = scraper.scrape_multiple(urls)
    
    blog_posts_data = []

    schema = StructType([
        StructField("url", StringType(), True),
        StructField("title", StringType(), True),
        StructField("markdown_content", StringType(), True),
        StructField("domain", StringType(), True)
    ])

    for result in results:
        if result['success']:
            row = {
                "url": result['metadata']['url'],
                "title": result['metadata']['title'],
                "markdown_content": result['content'],
                "domain": result['metadata']['domain'],
            }
            blog_posts_data.append(row)
    
    logger.info(f"Successfully scraped {len(blog_posts_data)} out of {len(urls)} posts")
    
    if blog_posts_data:
        return spark.createDataFrame(blog_posts_data, schema)
    
    return None