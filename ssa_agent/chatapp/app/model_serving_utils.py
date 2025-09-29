from mlflow.deployments import get_deploy_client
from databricks.sdk import WorkspaceClient

def _get_endpoint_task_type(endpoint_name: str) -> str:
    """Get the task type of a serving endpoint."""
    w = WorkspaceClient()
    ep = w.serving_endpoints.get(endpoint_name)
    return ep.task

def is_endpoint_supported(endpoint_name: str) -> bool:
    """Check if the endpoint has a supported task type."""
    task_type = _get_endpoint_task_type(endpoint_name)
    supported_task_types = ["agent/v1/chat", "agent/v2/chat", "llm/v1/chat", "agent/v1/responses"]
    return task_type in supported_task_types

def _validate_endpoint_task_type(endpoint_name: str) -> None:
    """Validate that the endpoint has a supported task type."""
    if not is_endpoint_supported(endpoint_name):
        raise Exception(
            f"Detected unsupported endpoint type for this basic chatbot template. "
            f"This chatbot template only supports chat completions-compatible endpoints. "
            f"For a richer chatbot template with support for all conversational endpoints on Databricks, "
            f"see https://docs.databricks.com/aws/en/generative-ai/agent-framework/chat-app"
        )

def _query_endpoint(endpoint_name: str, messages: list[dict[str, str]], logger) -> list[str, str]:
    """Calls a model serving endpoint."""
    _validate_endpoint_task_type(endpoint_name)
    
    inputs = {
        "input": messages, 
        "databricks_options": {"return_trace": True}
    }

    res = get_deploy_client('databricks').predict(
        endpoint=endpoint_name,
        inputs=inputs,
    )
    
    try: 
        return [res['output'][0]['content'][0]['text'], res['databricks_output']['trace']['info']['trace_id']]
    except: 
        return ["Something went wrong. Please try again. ", "trace-id-unknown"]

def query_endpoint(endpoint_name, messages, logger):
    """
    Query a chat-completions or agent serving endpoint
    If querying an agent serving endpoint that returns multiple messages, this method
    returns the last message
    ."""
    return _query_endpoint(endpoint_name, messages, logger)
