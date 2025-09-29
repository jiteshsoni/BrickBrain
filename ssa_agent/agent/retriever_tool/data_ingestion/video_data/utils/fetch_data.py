"""
Clients to interact with YouTube API and Transcript API.
"""

import requests
from typing import List, Dict, Optional
import logging
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api.proxies import WebshareProxyConfig
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

logger = logging.getLogger(__name__)

class YouTubeClient:
    """Client for interacting with Youtube API"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://www.googleapis.com/youtube/v3"
    
    def get_channel_id_from_username(self, username: str) -> Optional[str]:
        """
        Get channel ID from username.
        """
        url = f"{self.base_url}/search"
        params = {
            'key': self.api_key,
            'q': username,
            'type': 'channel',
            'part': 'snippet',
            'maxResults': 1
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            if data.get('items'):
                channel_id = data['items'][0]['snippet']['channelId']
                logger.info(f"Found channel ID {channel_id} for username {username}")
                return channel_id
            else:
                logger.warning(f"No channel found for username: {username}")
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Error getting channel ID: {e}")
        except KeyError as e:
            logger.error(f"Error parsing channel response: {e}")
            
        return None
    
    def get_channel_videos(self, channel_id: str, max_results: int = 50) -> List[Dict]:
        videos = []
        next_page_token = None
        
        url = f"{self.base_url}/search"
        
        while len(videos) < max_results:
            results_needed = min(50, max_results - len(videos))
            
            params = {
                'key': self.api_key,
                'channelId': channel_id,
                'part': 'snippet,id',
                'order': 'date',
                'type': 'video',
                'maxResults': results_needed
            }
            
            if next_page_token:
                params['pageToken'] = next_page_token
            
            try:
                response = requests.get(url, params=params)
                response.raise_for_status()
                data = response.json()
                
                for item in data.get('items', []):
                    videos.append(item)
                
                next_page_token = data.get('nextPageToken')
                if not next_page_token:
                    break
                                        
            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching videos: {e}")
                break
            except KeyError as e:
                logger.error(f"Error parsing video response: {e}")
                break
        
        logger.info(f"Successfully fetched {len(videos)} videos from channel {channel_id}")
        return videos


class TranscriptClient:
    def __init__(self, proxy_username: str, proxy_password: str):
        # fyi: proxy is used to avoid rate limiting
        # using free webshare proxy, but can use any other proxy
        
        self.ytt_api = YouTubeTranscriptApi(
            proxy_config=WebshareProxyConfig(
                proxy_username=proxy_username,
                proxy_password=proxy_password,
            )
        )
    
    def get_video_transcription(self, video_metadata: Dict) -> Dict:
        """
        Fetch transcription for a single video.
        """
        video_id = video_metadata['id'].get('videoId')
        video_title = video_metadata['snippet'].get('title', 'Unknown')
        video_publish_data = video_metadata['snippet'].get('publishTime', 'YYYY-MM-DD')
        
        try:
            logger.debug(f"Fetching transcription for video: {video_title} ({video_id})")
            
            fetched_transcription = self.ytt_api.fetch(video_id)
            text_snippets = [s.text for s in fetched_transcription.snippets]
            
            video_metadata['transcription'] = " ".join(text_snippets)
            video_metadata['transcription_status'] = "Success"
            
            logger.debug(f"Successfully fetched transcription for: {video_title}")
            
        except Exception as e:
            logger.warning(f"Failed to fetch transcription for {video_title} ({video_id}): {str(e)}")
            video_metadata['transcription'] = ""
            video_metadata['transcription_status'] = "Fail"
        
        return video_metadata
    
    def get_transcriptions_parallel(
        self, 
        all_videos: List[Dict], 
        max_workers: int = 5, 
        delay: float = 0.5
    ) -> List[Dict]:
        """
        Fetch transcriptions for multiple videos in parallel via threads.
        """
        all_results = []
        success_count = 0
        fail_count = 0
        
        logger.info(f"Starting transcription fetch for {len(all_videos)} videos with {max_workers} workers")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_video = {
                executor.submit(self.get_video_transcription, video.copy()): video 
                for video in all_videos
            }
            
            for future in as_completed(future_to_video):
                try:
                    result = future.result()
                    all_results.append(result)
                    
                    if result['transcription_status'] == "Success":
                        success_count += 1
                    elif result['transcription_status'] == "Fail":
                        fail_count += 1
                    
                    if delay > 0:
                        time.sleep(delay)
                                        
                except Exception as e:
                    logger.error(f"Error processing transcription task: {e}")
                    fail_count += 1
        
        logger.info(f"Transcription fetch completed. Success: {success_count}, Failed: {fail_count}")
        return all_results