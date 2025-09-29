import json
import logging
import time
from functools import lru_cache
from typing import Any, Dict, List
from urllib.parse import urlparse

from fastapi import HTTPException
from mlflow.utils.databricks_utils import get_databricks_host_creds
from mlflow.utils.rest_utils import http_request_safe
import mlflow.genai.labeling as labeling

def _get_mlflow_api_url(path: str, creds) -> str:
    host = creds.host
    mlflow_base = '/api/2.0/mlflow'
    return f'{host}{mlflow_base}{path}'

def _fetch_databricks_sync(
    method: str,
    url: str, 
    creds, 
    data: Dict[str, Any] = None,
    timeout: float = 30.0,
) -> Any:
    """Synchronous HTTP client for Databricks API calls."""
    parsed_url = urlparse(url)
    endpoint_path = parsed_url.path
    endpoint_name = '/'.join(endpoint_path.split('/')[-2:]) if '/' in endpoint_path else endpoint_path

    endpoint = parsed_url.path
    if parsed_url.query:
        endpoint += f'?{parsed_url.query}'
    
    response = http_request_safe(
        method=method.upper(),
        endpoint=endpoint,
        host_creds=host_creds,
        json=data if data is not None else None,
        timeout=timeout,
    )
    
    return response.json()
        
def link_traces_to_run(run_id: str, trace_ids: List[str]) -> Dict[str, Any]:
    """ Entry point: Link traces to an MLflow run"""
    creds = get_databricks_host_creds()
    url = _get_mlflow_api_url('/traces/link-to-run', creds=creds)
    data = {'run_id': run_id, 'trace_ids': trace_ids}
    _fetch_databricks_sync(method='POST', url=url, creds=creds, data=data)


def get_labelling_session(labelling_session_name: str): 
    labeling_sessions = labeling.get_labeling_sessions()
    for session in labeling_sessions:
        if session.name == labelling_session_name:
            return session
    return None

