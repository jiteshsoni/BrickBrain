// src/index.ts
import { McpAgent, defineTool } from "agents"; // Cloudflare Agents SDK
import { Hono } from "hono";                   // comes with the template
import { cors } from "hono/cors";

// Workers Env bindings (set in wrangler.toml / secrets)
export interface Env {
  DATABRICKS_SERVING_URL: string; // https://.../serving-endpoints/.../invocations
  DATABRICKS_TOKEN: string;       // PAT or OAuth token
  MCP_SHARED_KEY?: string;        // optional client auth for Cursor
}

const app = new Hono<{ Bindings: Env }>();
app.use("*", cors()); // optional

// Minimal MCP agent
const agent = new McpAgent({
  name: "brickbrain-agent-bridge",
  version: "1.0.0",
  description: "MCP bridge to Databricks Agent (BrickBrain) - provides access to Databricks knowledge base",
});

// Security: optional shared secret header for clients (e.g., Cursor)
function assertClientAuth(req: Request, env: Env) {
  if (!env.MCP_SHARED_KEY) return;
  const got = req.headers.get("x-mcp-key");
  if (!got || got !== env.MCP_SHARED_KEY) {
    throw new Response("Unauthorized MCP client", { status: 401 });
  }
}

// ---- MCP tool: ask_brickbrain ----
agent.addTool(
  defineTool({
    name: "ask_brickbrain",
    description:
      "Ask BrickBrain (Databricks Knowledge Assistant) questions about Databricks, Spark, Delta Lake, MLflow, Unity Catalog, and related data engineering topics. Returns expert answers with citations.",
    inputSchema: {
      type: "object",
      properties: {
        prompt: { 
          type: "string",
          description: "Your question about Databricks or data engineering topics"
        },
        conversation_id: { 
          type: "string",
          description: "Optional conversation ID to maintain context across multiple questions"
        },
        user_id: { 
          type: "string",
          description: "Optional user identifier for tracking"
        },
        return_trace: { 
          type: "boolean", 
          default: false,
          description: "Include execution trace in response (useful for debugging)"
        }
      },
      required: ["prompt"]
    },
    handler: async (input, { req, env }) => {
      assertClientAuth(req, env);

      const payload = {
        input: [{ role: "user", content: String(input.prompt) }],
        databricks_options: {
          ...(input.conversation_id ? { conversation_id: input.conversation_id } : {}),
          return_trace: Boolean(input.return_trace)
        },
        context: {
          ...(input.conversation_id ? { conversation_id: input.conversation_id } : {}),
          ...(input.user_id ? { user_id: input.user_id } : {})
        }
      };

      const r = await fetch(env.DATABRICKS_SERVING_URL, {
        method: "POST",
        headers: {
          "Authorization": `Bearer ${env.DATABRICKS_TOKEN}`,
          "Content-Type": "application/json"
        },
        body: JSON.stringify(payload)
      });

      if (!r.ok) {
        const text = await r.text();
        throw new Error(`Databricks call failed: ${r.status} ${text}`);
      }

      const data = await r.json() as any;

      // Common response shapes
      let textOut =
        data?.output?.[0]?.content ??
        data?.choices?.[0]?.message?.content ??
        data?.message ??
        JSON.stringify(data);

      if (input.return_trace && data?.trace) {
        textOut += `\n---\n[trace]\n${JSON.stringify(data.trace, null, 2)}`;
      }

      // MCP tool return shape: array of content parts
      return { content: [{ type: "text", text: textOut }] };
    }
  })
);

// Health check endpoint
app.get("/", (c) => {
  return c.json({
    name: "BrickBrain MCP Bridge",
    version: "1.0.0",
    status: "healthy",
    endpoints: {
      sse: "/sse",
      mcp: "/mcp",
      health: "/"
    },
    description: "MCP bridge to Databricks Agent"
  });
});

// Expose both transports (SSE + Streamable HTTP) on predictable paths
//   /sse   -> SSE transport
//   /mcp   -> Streamable HTTP transport (recommended)
app.get("/sse", (c) => agent.sse(c.req.raw));           // SSE
app.all("/mcp", (c) => agent.streamableHttp(c.req.raw)); // Streamable HTTP

export default app;

