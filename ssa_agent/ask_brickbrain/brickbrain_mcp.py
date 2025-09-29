#!/usr/bin/env python3
"""
BrickBrain MCP Server - All-in-One
Consolidated MCP server with health check and ask_brickbrain functionality
Includes all utilities and test functions in a single file
"""

import json
import logging
import sys
import asyncio
import uuid
import argparse
import os
from typing import Any, Dict
from databricks.sdk import WorkspaceClient

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("brickbrain-mcp")

# ============================================================================
# PROMPTS SECTION
# ============================================================================

DATABRICKS_PROMPTS = {
    "databricks-troubleshooting": {
        "name": "databricks-troubleshooting",
        "description": "Get help troubleshooting Databricks issues with structured context",
        "arguments": [
            {
                "name": "error_message",
                "description": "The error message or issue you're experiencing",
                "required": True
            },
            {
                "name": "context",
                "description": "What you were trying to do when the issue occurred",
                "required": True
            },
            {
                "name": "environment",
                "description": "Environment details (cluster type, runtime version, notebook/job, etc.)",
                "required": False
            }
        ]
    },
    "delta-lake-optimization": {
        "name": "delta-lake-optimization",
        "description": "Get guidance on optimizing Delta Lake tables for better performance",
        "arguments": [
            {
                "name": "table_size",
                "description": "Approximate size of your Delta table (e.g., '10TB', '500GB')",
                "required": True
            },
            {
                "name": "query_patterns",
                "description": "How you typically query this table (e.g., 'time-series on date', 'point lookups by ID')",
                "required": True
            },
            {
                "name": "current_issues",
                "description": "Performance issues you're experiencing (optional)",
                "required": False
            }
        ]
    },
    "mlflow-workflow": {
        "name": "mlflow-workflow",
        "description": "Get guidance on MLflow workflows and best practices",
        "arguments": [
            {
                "name": "use_case",
                "description": "Your ML use case (e.g., 'model training', 'model deployment', 'experiment tracking')",
                "required": True
            },
            {
                "name": "model_type",
                "description": "Type of model you're working with (e.g., 'sklearn', 'pytorch', 'xgboost')",
                "required": False
            },
            {
                "name": "current_challenge",
                "description": "Specific challenge you're facing (optional)",
                "required": False
            }
        ]
    },
    "data-engineering-pipeline": {
        "name": "data-engineering-pipeline",
        "description": "Get help designing or optimizing data engineering pipelines",
        "arguments": [
            {
                "name": "data_source",
                "description": "Your data source (e.g., 'S3', 'Kafka', 'database', 'API')",
                "required": True
            },
            {
                "name": "data_volume",
                "description": "Volume of data you're processing (e.g., 'GB/day', 'TB/hour')",
                "required": True
            },
            {
                "name": "processing_type",
                "description": "Type of processing needed (e.g., 'batch ETL', 'streaming', 'real-time analytics')",
                "required": True
            },
            {
                "name": "target_destination",
                "description": "Where the processed data should go (optional)",
                "required": False
            }
        ]
    },
    "unity-catalog-setup": {
        "name": "unity-catalog-setup",
        "description": "Get guidance on Unity Catalog setup and data governance",
        "arguments": [
            {
                "name": "organization_size",
                "description": "Size of your organization (e.g., 'small team', 'enterprise', 'startup')",
                "required": True
            },
            {
                "name": "data_governance_needs",
                "description": "Your data governance requirements (e.g., 'compliance', 'access control', 'lineage tracking')",
                "required": True
            },
            {
                "name": "current_setup",
                "description": "Your current data setup (optional)",
                "required": False
            }
        ]
    },
    "spark-performance": {
        "name": "spark-performance",
        "description": "Get help optimizing Apache Spark performance in Databricks",
        "arguments": [
            {
                "name": "workload_type",
                "description": "Type of Spark workload (e.g., 'batch processing', 'streaming', 'ML training')",
                "required": True
            },
            {
                "name": "performance_issue",
                "description": "Performance issue you're experiencing (e.g., 'slow queries', 'OOM errors', 'high costs')",
                "required": True
            },
            {
                "name": "cluster_config",
                "description": "Current cluster configuration (optional)",
                "required": False
            }
        ]
    }
}

# ============================================================================
# UTILITIES SECTION
# ============================================================================

def get_databricks_client(host=None, token=None):
    """Get authenticated Databricks WorkspaceClient"""
    if host and token:
        return WorkspaceClient(host=host, token=token)
    else:
        # Use default authentication (env vars or config file)
        return WorkspaceClient()

def generate_conversation_id():
    """Generate a unique conversation ID"""
    return str(uuid.uuid4())

class Conversation:
    """Conversation handler for Knowledge Agent interactions"""
    
    def __init__(self, endpoint_name="ka-ce79c9c6-endpoint", thread_id=None, databricks_host=None, databricks_token=None):
        self.endpoint_name = endpoint_name
        self.thread_id = thread_id or str(uuid.uuid4())
        self.history = []
        self.client = get_databricks_client(databricks_host, databricks_token)

    def add_message(self, role, content):
        self.history.append({"role": role, "content": content})

    def get_messages(self):
        return self.history

    def call_knowledge_agent(self, message):
        self.add_message("user", message)
        response = self.client.serving_endpoints.query(
            name=self.endpoint_name,
            dataframe_records=[{
                "input": self.get_messages(),
                "thread_id": self.thread_id
            }]
        )
        try:
            text = response.predictions['output'][0]['content'][0]['text']
            self.add_message("assistant", text)
            return text
        except (KeyError, IndexError, TypeError):
            return str(response.predictions)

class KnowledgeAgentClient:
    """
    Client for interacting with Databricks Knowledge Agent endpoints
    Using working conversation-based approach
    """
    
    def __init__(self, endpoint_name: str = "ka-ce79c9c6-endpoint", databricks_host=None, databricks_token=None):
        """
        Initialize Knowledge Agent client
        
        Args:
            endpoint_name: Name of the Knowledge Agent serving endpoint
            databricks_host: Databricks workspace host URL
            databricks_token: Databricks access token
        """
        self.endpoint_name = endpoint_name
        self.client = get_databricks_client(databricks_host, databricks_token)
        self.conversation = Conversation(endpoint_name=endpoint_name, databricks_host=databricks_host, databricks_token=databricks_token)
    
    def query(self, message: str, conversation_id: str = None):
        """
        Query the Knowledge Agent with a message
        
        Args:
            message: The user's question or message
            conversation_id: Optional conversation ID for context
            
        Returns:
            str: The Knowledge Agent's response
        """
        try:
            # Use the working conversation approach
            response = self.conversation.call_knowledge_agent(message)
            return response
                
        except Exception as e:
            logger.error(f"Error querying Knowledge Agent: {str(e)}")
            return f"❌ Error calling Knowledge Agent: {str(e)}"
    
    def test_connection(self):
        """
        Test the connection to the Knowledge Agent
        
        Returns:
            bool: True if connection is successful, False otherwise
        """
        try:
            response = self.query("Hello")
            return not response.startswith("❌")
        except Exception as e:
            logger.error(f"Connection test failed: {str(e)}")
            return False
    
    def get_endpoint_info(self):
        """
        Get information about the Knowledge Agent endpoint
        
        Returns:
            dict: Endpoint information
        """
        try:
            return {
                "endpoint_name": self.endpoint_name,
                "status": "active" if self.test_connection() else "inactive"
            }
        except Exception as e:
            logger.error(f"Error getting endpoint info: {str(e)}")
            return {
                "endpoint_name": self.endpoint_name,
                "status": "error",
                "error": str(e)
            }

# ============================================================================
# MCP SERVER SECTION
# ============================================================================

class SimpleMCPServer:
    """Simple MCP Server for BrickBrain"""
    
    def __init__(self, databricks_host=None, databricks_token=None, endpoint_name="ka-ce79c9c6-endpoint"):
        self.ka_client = KnowledgeAgentClient(
            endpoint_name=endpoint_name,
            databricks_host=databricks_host,
            databricks_token=databricks_token
        )
        logger.info(f"Simple MCP Server initialized with endpoint: {endpoint_name}")
        if databricks_host:
            logger.info(f"Using Databricks host: {databricks_host}")
        else:
            logger.info("Using default Databricks authentication")
    
    async def handle_request(self, request: dict) -> dict:
        """Handle MCP requests"""
        try:
            method = request.get("method")
            request_id = request.get("id")
            
            if method == "initialize":
                return {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {
                        "protocolVersion": "2024-11-05",
                        "capabilities": {
                            "tools": {},
                            "prompts": {}
                        },
                        "serverInfo": {
                            "name": "brickbrain-mcp-server",
                            "version": "1.0.0"
                        }
                    }
                }
            
            elif method == "tools/list":
                return {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {
                        "tools": [
                            {
                                "name": "ask_brickbrain",
                                "description": "Ask questions to the BrickBrain Knowledge Agent about Databricks, data engineering, and related topics",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "question": {
                                            "type": "string",
                                            "description": "The question to ask BrickBrain"
                                        },
                                        "context": {
                                            "type": "string",
                                            "description": "Optional context for the question",
                                            "default": ""
                                        }
                                    },
                                    "required": ["question"]
                                }
                            },
                            {
                                "name": "databrickshealth",
                                "description": "Check the health status of the MCP server and Databricks connection",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "random_string": {
                                            "type": "string",
                                            "description": "Dummy parameter for no-parameter tools"
                                        }
                                    },
                                    "required": ["random_string"]
                                }
                            },
                            {
                                "name": "databricksinfo",
                                "description": "Get information about Databricks features, services, or topics",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "topic": {
                                            "type": "string",
                                            "description": "The topic to get information about (e.g., 'delta', 'mlflow', 'workflows')"
                                        }
                                    },
                                    "required": ["topic"]
                                }
                            },
                            {
                                "name": "databricksworkspace",
                                "description": "List workspace contents or get workspace information",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "path": {
                                            "type": "string",
                                            "description": "Workspace path to explore (default: /)"
                                        }
                                    }
                                }
                            }
                        ]
                    }
                }
            
            elif method == "prompts/list":
                return {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {
                        "prompts": [
                            {
                                "name": prompt_data["name"],
                                "description": prompt_data["description"],
                                "arguments": prompt_data["arguments"]
                            }
                            for prompt_data in DATABRICKS_PROMPTS.values()
                        ]
                    }
                }
            
            elif method == "prompts/get":
                prompt_name = request["params"]["name"]
                arguments = request["params"].get("arguments", {})
                
                if prompt_name not in DATABRICKS_PROMPTS:
                    return {
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "error": {
                            "code": -32602,
                            "message": f"Unknown prompt: {prompt_name}"
                        }
                    }
                
                return await self.handle_prompt_get(prompt_name, arguments, request_id)
            
            elif method == "tools/call":
                tool_name = request["params"]["name"]
                arguments = request["params"].get("arguments", {})
                
                if tool_name == "ask_brickbrain":
                    return await self.handle_ask_brickbrain(arguments, request_id)
                elif tool_name == "databrickshealth":
                    return await self.handle_health_check(arguments, request_id)
                elif tool_name == "databricksinfo":
                    return await self.handle_databricks_info(arguments, request_id)
                elif tool_name == "databricksworkspace":
                    return await self.handle_workspace_info(arguments, request_id)
                else:
                    return {
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "error": {
                            "code": -32601,
                            "message": f"Unknown tool: {tool_name}"
                        }
                    }
            
            else:
                return {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "error": {
                        "code": -32601,
                        "message": f"Unknown method: {method}"
                    }
                }
                
        except Exception as e:
            logger.error(f"Error handling request: {str(e)}")
            return {
                "jsonrpc": "2.0",
                "id": request.get("id"),
                "error": {
                    "code": -32603,
                    "message": str(e)
                }
            }
    
    async def handle_prompt_get(self, prompt_name: str, arguments: Dict[str, Any], request_id: Any):
        """Handle prompts/get requests - generate formatted prompt messages"""
        try:
            prompt_def = DATABRICKS_PROMPTS[prompt_name]
            
            # Validate required arguments
            for arg_def in prompt_def["arguments"]:
                if arg_def["required"] and arg_def["name"] not in arguments:
                    return {
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "error": {
                            "code": -32602,
                            "message": f"Missing required argument: {arg_def['name']}"
                        }
                    }
            
            # Generate prompt message based on the prompt type
            prompt_message = self._generate_prompt_message(prompt_name, arguments)
            
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "description": prompt_def["description"],
                    "messages": [
                        {
                            "role": "user",
                            "content": {
                                "type": "text",
                                "text": prompt_message
                            }
                        }
                    ]
                }
            }
            
        except Exception as e:
            logger.error(f"Error generating prompt: {str(e)}")
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {
                    "code": -32603,
                    "message": f"Error generating prompt: {str(e)}"
                }
            }
    
    def _generate_prompt_message(self, prompt_name: str, arguments: Dict[str, Any]) -> str:
        """Generate the actual prompt message text based on prompt type and arguments"""
        
        if prompt_name == "databricks-troubleshooting":
            error_message = arguments.get("error_message", "")
            context = arguments.get("context", "")
            environment = arguments.get("environment", "Not specified")
            
            return f"""I need help troubleshooting a Databricks issue. Here are the details:

**Error/Issue:** {error_message}

**What I was trying to do:** {context}

**Environment Details:** {environment}

Please help me:
1. Understand what's causing this issue
2. Provide step-by-step troubleshooting steps
3. Suggest preventive measures for the future
4. Share any relevant best practices

Please be specific and include code examples or configuration changes if applicable."""

        elif prompt_name == "delta-lake-optimization":
            table_size = arguments.get("table_size", "")
            query_patterns = arguments.get("query_patterns", "")
            current_issues = arguments.get("current_issues", "No specific issues mentioned")
            
            return f"""I need help optimizing a Delta Lake table for better performance:

**Table Size:** {table_size}

**Query Patterns:** {query_patterns}

**Current Performance Issues:** {current_issues}

Please provide guidance on:
1. Optimal partitioning strategy for my use case
2. Z-ordering recommendations
3. Table maintenance operations (OPTIMIZE, VACUUM)
4. Cluster configuration suggestions
5. Query optimization techniques
6. Cost optimization strategies

Please include specific SQL commands and configuration examples."""

        elif prompt_name == "mlflow-workflow":
            use_case = arguments.get("use_case", "")
            model_type = arguments.get("model_type", "Not specified")
            current_challenge = arguments.get("current_challenge", "General guidance needed")
            
            return f"""I need guidance on MLflow workflows and best practices:

**Use Case:** {use_case}

**Model Type:** {model_type}

**Current Challenge:** {current_challenge}

Please help me with:
1. MLflow workflow setup and configuration
2. Experiment tracking best practices
3. Model versioning and registry usage
4. Model deployment strategies
5. Integration with Databricks features
6. Monitoring and maintenance

Please provide code examples and step-by-step instructions."""

        elif prompt_name == "data-engineering-pipeline":
            data_source = arguments.get("data_source", "")
            data_volume = arguments.get("data_volume", "")
            processing_type = arguments.get("processing_type", "")
            target_destination = arguments.get("target_destination", "Not specified")
            
            return f"""I need help designing/optimizing a data engineering pipeline:

**Data Source:** {data_source}

**Data Volume:** {data_volume}

**Processing Type:** {processing_type}

**Target Destination:** {target_destination}

Please provide guidance on:
1. Optimal pipeline architecture for my use case
2. Data ingestion strategies and tools
3. Processing and transformation approaches
4. Error handling and monitoring
5. Performance optimization techniques
6. Cost optimization strategies
7. Scheduling and orchestration

Please include specific code examples and configuration recommendations."""

        elif prompt_name == "unity-catalog-setup":
            org_size = arguments.get("organization_size", "")
            governance_needs = arguments.get("data_governance_needs", "")
            current_setup = arguments.get("current_setup", "Starting from scratch")
            
            return f"""I need help with Unity Catalog setup and data governance:

**Organization Size:** {org_size}

**Data Governance Requirements:** {governance_needs}

**Current Setup:** {current_setup}

Please provide guidance on:
1. Unity Catalog architecture and setup
2. Metastore configuration
3. Catalog and schema organization
4. Access control and permissions
5. Data lineage and discovery
6. Integration with existing systems
7. Best practices for governance
8. Migration strategies (if applicable)

Please include step-by-step setup instructions and configuration examples."""

        elif prompt_name == "spark-performance":
            workload_type = arguments.get("workload_type", "")
            performance_issue = arguments.get("performance_issue", "")
            cluster_config = arguments.get("cluster_config", "Not specified")
            
            return f"""I need help optimizing Apache Spark performance in Databricks:

**Workload Type:** {workload_type}

**Performance Issue:** {performance_issue}

**Current Cluster Config:** {cluster_config}

Please help me with:
1. Cluster configuration optimization
2. Spark configuration tuning
3. Code optimization techniques
4. Memory and resource management
5. Data partitioning strategies
6. Caching and persistence strategies
7. Monitoring and debugging techniques
8. Cost optimization while maintaining performance

Please provide specific configuration examples and code optimizations."""

        else:
            return f"Please help me with {prompt_name}. Here are my details: {arguments}"
    
    async def handle_ask_brickbrain(self, arguments: Dict[str, Any], request_id: Any):
        """Handle ask_brickbrain tool calls"""
        question = arguments.get("question", "")
        context = arguments.get("context", "")
        
        if not question:
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {
                    "code": -32602,
                    "message": "No question provided"
                }
            }
        
        try:
            # Add context to question if provided
            if context:
                full_question = f"Context: {context}\n\nQuestion: {question}"
            else:
                full_question = question
            
            logger.info(f"Asking BrickBrain: {full_question[:100]}...")
            response = self.ka_client.query(full_question)
            
            # Format response
            if response.startswith("❌"):
                return {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {
                        "content": [{"type": "text", "text": response}],
                        "isError": True
                    }
                }
            else:
                return {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {
                        "content": [{"type": "text", "text": f"🧠 BrickBrain Response:\n\n{response}"}],
                        "isError": False
                    }
                }
                
        except Exception as e:
            logger.error(f"Error querying BrickBrain: {str(e)}")
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "content": [{"type": "text", "text": f"Error querying BrickBrain: {str(e)}"}],
                    "isError": True
                }
            }
    
    async def handle_health_check(self, arguments: Dict[str, Any], request_id: Any):
        """Handle health check requests"""
        try:
            # Test Knowledge Agent connection
            ka_status = self.ka_client.get_endpoint_info()
            is_connected = self.ka_client.test_connection()
            
            status_text = f"""🏥 BrickBrain MCP Health Check:

✅ MCP Server: healthy
✅ Knowledge Agent: {ka_status.get('status', 'unknown')}
✅ Endpoint: {ka_status.get('endpoint_name', 'unknown')}
✅ Connection: {'Connected' if is_connected else 'Disconnected'}
✅ Integration: All-in-One MCP Server
✅ Version: 1.0.0

Available Tools:
- ask_brickbrain: Ask questions to the knowledge agent
- databrickshealth: Check server health and status
- databricksinfo: Get Databricks feature information
- databricksworkspace: Explore workspace contents
"""
            
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "content": [{"type": "text", "text": status_text}],
                    "isError": False
                }
            }
            
        except Exception as e:
            logger.error(f"Error in health check: {str(e)}")
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "content": [{"type": "text", "text": f"Health check error: {str(e)}"}],
                    "isError": True
                }
            }
    
    async def handle_databricks_info(self, arguments: Dict[str, Any], request_id: Any):
        """Handle databricks info requests"""
        topic = arguments.get("topic", "")
        
        if not topic:
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {
                    "code": -32602,
                    "message": "No topic provided"
                }
            }
        
        try:
            question = f"Tell me about {topic} in Databricks. Provide detailed information about its features, use cases, and best practices."
            response = self.ka_client.query(question)
            
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "content": [{"type": "text", "text": f"📊 Databricks {topic.title()} Information:\n\n{response}"}],
                    "isError": False
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting Databricks info: {str(e)}")
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "content": [{"type": "text", "text": f"Error getting Databricks info: {str(e)}"}],
                    "isError": True
                }
            }
    
    async def handle_workspace_info(self, arguments: Dict[str, Any], request_id: Any):
        """Handle workspace info requests"""
        path = arguments.get("path", "/")
        
        try:
            question = f"What can you tell me about Databricks workspace organization and structure? Focus on path: {path}"
            response = self.ka_client.query(question)
            
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "content": [{"type": "text", "text": f"🗂️ Databricks Workspace Info (Path: {path}):\n\n{response}"}],
                    "isError": False
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting workspace info: {str(e)}")
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "content": [{"type": "text", "text": f"Error getting workspace info: {str(e)}"}],
                    "isError": True
                }
            }

# ============================================================================
# MAIN SERVER ENTRY POINT
# ============================================================================

async def run_mcp_server(databricks_host=None, databricks_token=None, endpoint_name="ka-ce79c9c6-endpoint"):
    """Main entry point for the MCP server"""
    server = SimpleMCPServer(
        databricks_host=databricks_host,
        databricks_token=databricks_token,
        endpoint_name=endpoint_name
    )
    
    logger.info("Starting BrickBrain All-in-One MCP Server...")
    
    # Read from stdin and write to stdout (stdio transport)
    while True:
        try:
            line = await asyncio.get_event_loop().run_in_executor(None, sys.stdin.readline)
            if not line:
                break
            
            request = json.loads(line.strip())
            response = await server.handle_request(request)
            
            print(json.dumps(response))
            sys.stdout.flush()
            
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON: {str(e)}")
            error_response = {
                "jsonrpc": "2.0",
                "id": None,
                "error": {
                    "code": -32700,
                    "message": "Parse error"
                }
            }
            print(json.dumps(error_response))
            sys.stdout.flush()
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            break

# ============================================================================
# TEST FUNCTIONS SECTION
# ============================================================================

async def run_tests(databricks_host=None, databricks_token=None, endpoint_name="ka-ce79c9c6-endpoint"):
    """Test the MCP server functionality"""
    server = SimpleMCPServer(
        databricks_host=databricks_host,
        databricks_token=databricks_token,
        endpoint_name=endpoint_name
    )
    
    print("🧪 Testing BrickBrain All-in-One MCP Server...")
    
    # Test 0: Direct utils test
    print("\n0. Testing Knowledge Agent directly...")
    ka_client = KnowledgeAgentClient()
    direct_response = ka_client.query("What is Apache Spark?")
    print(f"✅ Direct Knowledge Agent response: {direct_response[:100]}...")
    
    # Test 1: Initialize
    print("\n1. Testing initialize...")
    init_request = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {}
    }
    response = await server.handle_request(init_request)
    print(f"✅ Initialize response: {json.dumps(response, indent=2)}")
    
    # Test 2: List tools
    print("\n2. Testing tools/list...")
    list_request = {
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/list",
        "params": {}
    }
    response = await server.handle_request(list_request)
    print(f"✅ Tools list response: {json.dumps(response, indent=2)}")
    
    # Test 3: Health check
    print("\n3. Testing health check...")
    health_request = {
        "jsonrpc": "2.0",
        "id": 3,
        "method": "tools/call",
        "params": {
            "name": "databrickshealth",
            "arguments": {"random_string": "test"}
        }
    }
    response = await server.handle_request(health_request)
    print(f"✅ Health check response: {json.dumps(response, indent=2)}")
    
    # Test 4: Ask BrickBrain
    print("\n4. Testing ask_brickbrain...")
    ask_request = {
        "jsonrpc": "2.0",
        "id": 4,
        "method": "tools/call",
        "params": {
            "name": "ask_brickbrain",
            "arguments": {
                "question": "What is Databricks?",
                "context": "I'm new to Databricks and want to understand the basics."
            }
        }
    }
    response = await server.handle_request(ask_request)
    print(f"✅ Ask BrickBrain response: {json.dumps(response, indent=2)}")
    
    # Test 5: Databricks Info
    print("\n5. Testing databricks info...")
    info_request = {
        "jsonrpc": "2.0",
        "id": 5,
        "method": "tools/call",
        "params": {
            "name": "databricksinfo",
            "arguments": {
                "topic": "delta"
            }
        }
    }
    response = await server.handle_request(info_request)
    print(f"✅ Databricks info response: {json.dumps(response, indent=2)}")
    
    # Test 6: List prompts
    print("\n6. Testing prompts/list...")
    prompts_list_request = {
        "jsonrpc": "2.0",
        "id": 6,
        "method": "prompts/list",
        "params": {}
    }
    response = await server.handle_request(prompts_list_request)
    print(f"✅ Prompts list response: {json.dumps(response, indent=2)}")
    
    # Test 7: Get specific prompt
    print("\n7. Testing prompts/get...")
    prompt_get_request = {
        "jsonrpc": "2.0",
        "id": 7,
        "method": "prompts/get",
        "params": {
            "name": "databricks-troubleshooting",
            "arguments": {
                "error_message": "OutOfMemoryError: Java heap space",
                "context": "Running a large Spark job with 100GB dataset",
                "environment": "Standard cluster, 8GB RAM, Databricks Runtime 13.3"
            }
        }
    }
    response = await server.handle_request(prompt_get_request)
    print(f"✅ Prompt get response: {json.dumps(response, indent=2)}")
    
    # Test 8: Get Delta optimization prompt
    print("\n8. Testing delta optimization prompt...")
    delta_prompt_request = {
        "jsonrpc": "2.0",
        "id": 8,
        "method": "prompts/get",
        "params": {
            "name": "delta-lake-optimization",
            "arguments": {
                "table_size": "5TB",
                "query_patterns": "Time-series queries filtering by date and customer_id",
                "current_issues": "Slow query performance, high costs"
            }
        }
    }
    response = await server.handle_request(delta_prompt_request)
    print(f"✅ Delta optimization prompt response: {json.dumps(response, indent=2)}")
    
    print("\n🎉 All tests completed!")

# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="BrickBrain MCP Server")
    parser.add_argument("--databricks-host", 
                       help="Databricks workspace host URL (e.g., https://dbc-xxxxxxxx-xxxx.cloud.databricks.com)")
    parser.add_argument("--databricks-token", 
                       help="Databricks access token")
    parser.add_argument("--endpoint-name", 
                       default="ka-ce79c9c6-endpoint",
                       help="Knowledge Agent endpoint name (default: ka-ce79c9c6-endpoint)")
    parser.add_argument("test", nargs="?", 
                       help="Run tests instead of starting server")
    return parser.parse_args()

def main():
    """Main entry point - determines whether to run server or tests"""
    args = parse_args()
    
    # Get credentials from args, environment variables, or config file
    databricks_host = args.databricks_host or os.getenv("DATABRICKS_HOST")
    databricks_token = args.databricks_token or os.getenv("DATABRICKS_TOKEN")
    endpoint_name = args.endpoint_name
    
    if args.test == "test":
        # Run tests
        asyncio.run(run_tests(databricks_host, databricks_token, endpoint_name))
    else:
        # Run MCP server
        asyncio.run(run_mcp_server(databricks_host, databricks_token, endpoint_name))

if __name__ == "__main__":
    main()
