// src/index.ts
import { Hono } from "hono";
import { cors } from "hono/cors";

// Workers Env bindings (set in wrangler.toml / secrets)
export interface Env {
  DATABRICKS_SERVING_URL: string; // https://.../serving-endpoints/.../invocations
  DATABRICKS_TOKEN: string;       // PAT or OAuth token
  MCP_SHARED_KEY?: string;        // optional client auth for Cursor
}

const app = new Hono<{ Bindings: Env }>();
app.use("*", cors()); // Enable CORS

// In-memory conversation management
// Maps user identifiers to their conversation IDs
// Cleared when worker restarts (~30 mins of inactivity or on deploy)
const conversations = new Map<string, { 
  conversationId: string; 
  lastUsed: number;
  messageCount: number;
}>();

function getUserId(req: Request): string {
  // Try to get a stable user identifier
  // Priority: user_id param > IP address > random session
  const forwarded = req.headers.get('cf-connecting-ip') || 
                    req.headers.get('x-forwarded-for') || 
                    'anonymous';
  return forwarded.split(',')[0].trim();
}

function getOrCreateConversation(userId: string, explicitConversationId?: string): string {
  // If user provided explicit conversation_id, use it
  if (explicitConversationId) {
    const session = conversations.get(userId);
    if (!session || session.conversationId !== explicitConversationId) {
      // New conversation or different ID - reset
      conversations.set(userId, {
        conversationId: explicitConversationId,
        lastUsed: Date.now(),
        messageCount: 1
      });
    } else {
      // Continue existing conversation
      session.lastUsed = Date.now();
      session.messageCount++;
    }
    return explicitConversationId;
  }

  // Auto-manage conversation
  const existing = conversations.get(userId);
  const now = Date.now();
  const TTL = 30 * 60 * 1000; // 30 minutes

  if (existing && (now - existing.lastUsed) < TTL) {
    // Continue existing conversation
    existing.lastUsed = now;
    existing.messageCount++;
    return existing.conversationId;
  }

  // Create new conversation
  const newConvId = `conv_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
  conversations.set(userId, {
    conversationId: newConvId,
    lastUsed: now,
    messageCount: 1
  });
  
  return newConvId;
}

function cleanupOldConversations() {
  const now = Date.now();
  const TTL = 30 * 60 * 1000;
  
  for (const [userId, session] of conversations.entries()) {
    if ((now - session.lastUsed) > TTL) {
      conversations.delete(userId);
    }
  }
}

// Security: optional shared secret header for clients (e.g., Cursor)
function assertClientAuth(req: Request, env: Env) {
  if (!env.MCP_SHARED_KEY) return;
  const got = req.headers.get("x-mcp-key");
  if (!got || got !== env.MCP_SHARED_KEY) {
    throw new Error("Unauthorized MCP client");
  }
}

// MCP JSON-RPC 2.0 handler
async function handleMCPRequest(request: any, env: Env, originalReq: Request): Promise<any> {
  try {
    assertClientAuth(originalReq, env);

    const { method, params, id } = request;

    // Handle MCP protocol methods
    switch (method) {
      case "initialize":
        return {
          jsonrpc: "2.0",
          id,
          result: {
            protocolVersion: "2024-11-05",
            serverInfo: {
              name: "brickbrain-agent-bridge",
              version: "1.0.0",
            },
            capabilities: {
              tools: {},
              prompts: {},
            },
          },
        };
      
      case "notifications/initialized":
        // Client confirmation - no response needed
        return null;

      case "prompts/list":
        return {
          jsonrpc: "2.0",
          id,
          result: {
            prompts: []
          },
        };

      case "tools/list":
        return {
          jsonrpc: "2.0",
          id,
          result: {
            tools: [
              {
                name: "ask_brickbrain",
                description:
                  "Ask BrickBrain (Databricks Knowledge Assistant) questions about Databricks, Spark, Delta Lake, MLflow, Unity Catalog, and related data engineering topics. Returns expert answers with citations. Conversations are automatically maintained for 30 minutes.",
                inputSchema: {
                  type: "object",
                  properties: {
                    prompt: {
                      type: "string",
                      description: "Your question about Databricks or data engineering topics",
                    },
                    conversation_id: {
                      type: "string",
                      description: "Optional: Provide explicit conversation ID. If omitted, automatically managed per user.",
                    },
                    user_id: {
                      type: "string",
                      description: "Optional user identifier for tracking. Defaults to IP address.",
                    },
                    return_trace: {
                      type: "boolean",
                      description: "Include execution trace in response (useful for debugging)",
                    },
                  },
                  required: ["prompt"],
                },
              },
              {
                name: "reset_conversation",
                description:
                  "Reset your conversation history and start fresh. Use this when you want to begin a new topic or clear previous context.",
                inputSchema: {
                  type: "object",
                  properties: {
                    user_id: {
                      type: "string",
                      description: "Optional user identifier. Defaults to IP address.",
                    },
                  },
                  required: [],
                },
              },
            ],
          },
        };

      case "tools/call":
        const { name, arguments: args } = params;

        if (name === "ask_brickbrain") {
          // Clean up old conversations periodically
          if (Math.random() < 0.1) cleanupOldConversations(); // 10% of requests
          
          // Get or create conversation ID
          const userId = args.user_id || getUserId(originalReq);
          const conversationId = getOrCreateConversation(userId, args.conversation_id);
          
          const payload = {
            input: [{ role: "user", content: String(args.prompt) }],
            databricks_options: {
              conversation_id: conversationId,
              return_trace: Boolean(args.return_trace),
            },
            context: {
              conversation_id: conversationId,
              user_id: userId,
            },
          };

          // Long timeout for slow Databricks agent (up to 2 minutes)
          const controller = new AbortController();
          const timeoutId = setTimeout(() => controller.abort(), 150000); // 150 seconds (2.5 min)

          let r;
          try {
            r = await fetch(env.DATABRICKS_SERVING_URL, {
              method: "POST",
              headers: {
                Authorization: `Bearer ${env.DATABRICKS_TOKEN}`,
                "Content-Type": "application/json",
              },
              body: JSON.stringify(payload),
              signal: controller.signal,
            });
          } catch (error: any) {
            clearTimeout(timeoutId);
            if (error.name === 'AbortError') {
              return {
                jsonrpc: "2.0",
                id,
                error: {
                  code: -32603,
                  message: `Request timed out after 2.5 minutes. The agent may be processing a complex query. Please try again or simplify your question.`,
                },
              };
            }
            throw error;
          }
          
          clearTimeout(timeoutId);

          if (!r.ok) {
            const text = await r.text();
            return {
              jsonrpc: "2.0",
              id,
              error: {
                code: -32603,
                message: `Databricks call failed: ${r.status} ${text}`,
              },
            };
          }

          const data = (await r.json()) as any;

          // Extract response text - handle nested content structure
          let textOut: string;
          
          // Try various response formats
          if (data?.output?.[0]?.content) {
            // Databricks Knowledge Agent format: output[0].content[0].text
            const content = data.output[0].content;
            if (Array.isArray(content) && content[0]?.text) {
              textOut = content[0].text;
            } else if (typeof content === 'string') {
              textOut = content;
            } else {
              textOut = JSON.stringify(content);
            }
          } else if (data?.choices?.[0]?.message?.content) {
            // OpenAI-style format
            textOut = data.choices[0].message.content;
          } else if (data?.message) {
            textOut = data.message;
          } else {
            textOut = JSON.stringify(data);
          }

          // Add conversation metadata
          const session = conversations.get(userId);
          if (session && !args.conversation_id) {
            // Only show auto-managed conversation info
            textOut += `\n\n---\n💬 *Conversation: ${session.messageCount} messages (auto-managed, resets after 30min inactivity)*`;
          }

          if (args.return_trace && data?.trace) {
            textOut += `\n---\n[trace]\n${JSON.stringify(data.trace, null, 2)}`;
          }

          return {
            jsonrpc: "2.0",
            id,
            result: {
              content: [{ type: "text", text: textOut }],
              metadata: {
                conversation_id: conversationId,
                user_id: userId,
                message_count: session?.messageCount || 1
              }
            },
          };
        }

        if (name === "reset_conversation") {
          const userId = args.user_id || getUserId(originalReq);
          const oldSession = conversations.get(userId);
          conversations.delete(userId);
          
          return {
            jsonrpc: "2.0",
            id,
            result: {
              content: [{
                type: "text",
                text: `✅ Conversation reset! ${oldSession ? `Previous conversation had ${oldSession.messageCount} messages.` : ''} Your next question will start a fresh conversation.`
              }],
              metadata: {
                user_id: userId,
                previous_message_count: oldSession?.messageCount || 0
              }
            },
          };
        }

        return {
          jsonrpc: "2.0",
          id,
          error: {
            code: -32601,
            message: `Unknown tool: ${name}`,
          },
        };

      default:
        return {
          jsonrpc: "2.0",
          id,
          error: {
            code: -32601,
            message: `Unknown method: ${method}`,
          },
        };
    }
  } catch (error: any) {
    return {
      jsonrpc: "2.0",
      id: request.id,
      error: {
        code: -32603,
        message: error.message || "Internal error",
      },
    };
  }
}

// Health check endpoint
app.get("/", (c) => {
  return c.json({
    name: "BrickBrain MCP Bridge",
    version: "1.0.0",
    status: "healthy",
    endpoints: {
      mcp: "/mcp",
      health: "/",
    },
    description: "MCP bridge to Databricks Agent",
    protocol: "JSON-RPC 2.0 over HTTP",
  });
});

// MCP endpoint (JSON-RPC 2.0 over HTTP)
app.post("/mcp", async (c) => {
  try {
    const request = await c.req.json();
    const env = c.env as Env;
    const result = await handleMCPRequest(request, env, c.req.raw);
    
    // Handle notification responses (null = no response needed)
    if (result === null) {
      return c.text("", 204); // No Content
    }
    
    return c.json(result);
  } catch (error: any) {
    return c.json(
      {
        jsonrpc: "2.0",
        id: null,
        error: {
          code: -32700,
          message: error.message || "Parse error",
        },
      },
      400
    );
  }
});

// SSE endpoint for MCP (Server-Sent Events transport)
app.get("/sse", async (c) => {
  const env = c.env as Env;
  
  try {
    assertClientAuth(c.req.raw, env);
  } catch (error: any) {
    return c.json({ error: error.message }, 401);
  }

  // Create SSE stream
  const stream = new ReadableStream({
    start(controller) {
      const encoder = new TextEncoder();
      
      // Send initial connection message
      const message = `event: message\ndata: ${JSON.stringify({
        jsonrpc: "2.0",
        method: "connected",
        params: {
          serverInfo: {
            name: "brickbrain-agent-bridge",
            version: "1.0.0"
          }
        }
      })}\n\n`;
      
      controller.enqueue(encoder.encode(message));
      
      // Keep connection alive with periodic pings
      const interval = setInterval(() => {
        try {
          controller.enqueue(encoder.encode(": ping\n\n"));
        } catch {
          clearInterval(interval);
        }
      }, 15000);
      
      // Note: In a real implementation, you'd handle incoming requests here
      // For now, this demonstrates the SSE endpoint structure
    },
  });

  return new Response(stream, {
    headers: {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache",
      "Connection": "keep-alive",
    },
  });
});

export default app;
