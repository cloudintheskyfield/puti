"""
@Author: obstacles
@Time:  2025-07-14 16:30
@Description: Simplified MCP server implementation with SSE
"""
import os
import sys
import json
import uuid
import time
import logging
import asyncio
import traceback
from typing import Dict, Any, List, Optional, Callable, Union
import uvicorn
from starlette.applications import Starlette
from starlette.responses import Response, JSONResponse, StreamingResponse
from starlette.routing import Route
from starlette.requests import Request
from starlette.middleware import Middleware
from starlette.middleware.base import BaseHTTPMiddleware

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("simplified_mcp_server")

# Global state
active_sessions = {}  # Maps session_id to session data
registered_tools = {}  # Maps tool_name to tool handler and info

class MCPSession:
    """Represents an active MCP session"""
    
    def __init__(self, session_id: str):
        self.id = session_id
        self.created_at = time.time()
        self.last_activity = time.time()
        self.initialized = False
        self.event_queue = asyncio.Queue()  # Queue for SSE events
        self.is_connected = False  # Whether the client is connected to SSE
        self.pending_events = []  # Events queued while not connected
    
    def activity(self):
        """Update last activity timestamp"""
        self.last_activity = time.time()
    
    async def send_event(self, event_type: str, data: Dict):
        """Send event to client via SSE"""
        event = {
            "type": event_type,
            "data": data,
            "timestamp": time.time()
        }
        
        # Log the event
        logger.info(f"[Event Queued] Session {self.id}: {event_type} - {json.dumps(data)}")
        
        # Add to queue
        await self.event_queue.put(event)


# Tool registration
def register_tool(name: str, handler: Callable, description: str = "", parameters: Dict = None):
    """Register a tool with the MCP server"""
    global registered_tools
    
    registered_tools[name] = {
        "name": name,
        "handler": handler,
        "description": description,
        "parameters": parameters or {}
    }
    
    logger.info(f"[Tool Registered] {name}: {description}")


# Request handlers
async def create_session(request: Request) -> StreamingResponse:
    """SSE endpoint to create a new session and stream events"""
    # Create a new session
    session_id = str(uuid.uuid4())
    session = MCPSession(session_id)
    active_sessions[session_id] = session
    
    logger.info(f"[Session Created] {session_id}")
    
    # Set up an event queue with proper buffer size
    session.event_queue = asyncio.Queue(maxsize=100)
    
    # Return streaming response
    return StreamingResponse(
        event_stream(session, session_id),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )

async def event_stream(session: MCPSession, session_id: str):
    """Generate SSE events for a session"""
    try:
        # Send initial session ID
        session_info = {"session_id": session_id}
        initial_event = f"event: session\ndata: {json.dumps(session_info)}\n\n"
        logger.info(f"[Event Stream] Sending initial session event: {initial_event.strip()}")
        yield initial_event
        
        # Mark session as connected
        session.is_connected = True
        
        # Keep connection alive and process events
        ping_count = 0
        last_ping = time.time()
        
        while True:
            # Try to get event from queue with timeout
            try:
                # Wait for event with shorter timeout (1 second)
                event = await asyncio.wait_for(session.event_queue.get(), timeout=1.0)
                
                # Send event to client
                event_type = event["type"]
                event_data = event["data"]
                
                event_str = f"event: {event_type}\ndata: {json.dumps(event_data)}\n\n"
                logger.info(f"[Event Stream] Sending event: {event_str.strip()}")
                yield event_str
                
                # Mark task as done
                session.event_queue.task_done()
                
            except asyncio.TimeoutError:
                # Check if we should send a ping
                now = time.time()
                if now - last_ping >= 5:  # Send ping every 5 seconds (reduced from 10)
                    # Send ping event
                    ping_data = {
                        "session_id": session_id,
                        "count": ping_count,
                        "time": time.strftime("%Y-%m-%d %H:%M:%S")
                    }
                    ping_event = f"event: ping\ndata: {json.dumps(ping_data)}\n\n"
                    logger.debug(f"[Event Stream] Sending ping: {ping_event.strip()}")
                    yield ping_event
                    
                    last_ping = now
                    ping_count += 1
    
    except Exception as e:
        logger.error(f"[SSE Error] Session {session_id}: {str(e)}")
        logger.error(traceback.format_exc())
    
    finally:
        # Mark session as disconnected
        if session_id in active_sessions:
            active_sessions[session_id].is_connected = False
            logger.info(f"[Session Disconnected] {session_id}")


async def handle_message(request: Request) -> JSONResponse:
    """Handle JSON-RPC messages"""
    # Get session ID from query params
    session_id = request.query_params.get("session_id")
    if not session_id or session_id not in active_sessions:
        return JSONResponse(
            status_code=403,
            content={
                "jsonrpc": "2.0",
                "error": {
                    "code": -32000,
                    "message": "Invalid or missing session ID"
                },
                "id": None
            }
        )
    
    # Get session
    session = active_sessions[session_id]
    session.activity()
    
    # Parse request body
    try:
        body = await request.body()
        payload = json.loads(body)
        
        # Check for valid JSON-RPC
        if not isinstance(payload, dict) or payload.get("jsonrpc") != "2.0" or "method" not in payload:
            return JSONResponse(
                status_code=400,
                content={
                    "jsonrpc": "2.0",
                    "error": {
                        "code": -32600,
                        "message": "Invalid JSON-RPC request"
                    },
                    "id": payload.get("id", None)
                }
            )
        
        # Get request details
        request_id = payload.get("id", 0)
        method = payload.get("method")
        params = payload.get("params", {})
        
        # Handle different methods
        if method == "initialize":
            return await handle_initialize(session, request_id, params)
        elif method == "tools/list":
            return await handle_tools_list(session, request_id, params)
        elif method == "tools/call":
            return await handle_tool_call(session, request_id, params)
        else:
            # Unknown method
            return JSONResponse(
                content={
                    "jsonrpc": "2.0",
                    "error": {
                        "code": -32601,
                        "message": f"Method '{method}' not found"
                    },
                    "id": request_id
                }
            )
    
    except json.JSONDecodeError:
        return JSONResponse(
            status_code=400,
            content={
                "jsonrpc": "2.0",
                "error": {
                    "code": -32700,
                    "message": "Parse error"
                },
                "id": None
            }
        )
    except Exception as e:
        logger.error(f"[Message Error] {str(e)}")
        logger.error(traceback.format_exc())
        return JSONResponse(
            status_code=500,
            content={
                "jsonrpc": "2.0",
                "error": {
                    "code": -32603,
                    "message": f"Internal error: {str(e)}"
                },
                "id": None
            }
        )


async def handle_initialize(session: MCPSession, request_id: int, params: Dict) -> JSONResponse:
    """Handle initialize method"""
    session.initialized = True
    logger.info(f"[Initialize] Session {session.id}")
    
    return JSONResponse(
        content={
            "jsonrpc": "2.0",
            "result": {
                "capabilities": {
                    "protocolVersion": "0.1",
                    "serverInfo": {
                        "name": "simplified-mcp-server",
                        "version": "1.0.0"
                    }
                }
            },
            "id": request_id
        }
    )


async def handle_tools_list(session: MCPSession, request_id: int, params: Dict) -> JSONResponse:
    """Handle tools/list method"""
    tools_list = []
    for name, info in registered_tools.items():
        tools_list.append({
            "name": name,
            "description": info["description"],
            "parameters": info["parameters"]
        })
    
    logger.info(f"[Tools List] Session {session.id}: {len(tools_list)} tools")
    
    return JSONResponse(
        content={
            "jsonrpc": "2.0",
            "result": {
                "tools": tools_list
            },
            "id": request_id
        }
    )


async def handle_tool_call(session: MCPSession, request_id: int, params: Dict) -> JSONResponse:
    """Handle tools/call method"""
    tool_name = params.get("name")
    arguments = params.get("arguments", {})
    
    # Check if tool exists
    if tool_name not in registered_tools:
        return JSONResponse(
            content={
                "jsonrpc": "2.0",
                "error": {
                    "code": -32602,
                    "message": f"Tool '{tool_name}' not found"
                },
                "id": request_id
            }
        )
    
    # Immediately return acceptance response
    logger.info(f"[Tool Call] Session {session.id}: {tool_name}")
    
    # Start tool execution in the background
    asyncio.create_task(execute_tool(session, tool_name, arguments, request_id))
    
    return JSONResponse(
        content={
            "jsonrpc": "2.0",
            "result": {
                "status": "accepted",
                "message": f"Tool '{tool_name}' call accepted and is being processed"
            },
            "id": request_id
        }
    )


async def execute_tool(session: MCPSession, tool_name: str, arguments: Dict, request_id: int):
    """Execute a tool and send result via SSE"""
    tool_info = registered_tools[tool_name]
    handler = tool_info["handler"]
    
    try:
        # Execute tool
        logger.info(f"[Tool Execute] Session {session.id}: {tool_name} (request {request_id})")
        result = await handler(**arguments)
        logger.info(f"[Tool Result] Got result for {tool_name}: {result}")
        
        # Prepare result
        if not isinstance(result, dict):
            result = {"result": result}
            
        # Send result via SSE
        result_event = {
            "request_id": request_id,
            "tool": tool_name,
            "result": result,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }
        
        logger.info(f"[Tool Success] Sending result for {tool_name} via SSE: {json.dumps(result_event)}")
        
        # Instead of using send_event, add directly to queue for immediate processing
        if session.is_connected and session.event_queue:
            await session.event_queue.put({
                "type": "tool_result",
                "data": result_event,
                "timestamp": time.time()
            })
            logger.info(f"[Tool Result] Added to event queue for session {session.id}")
        else:
            logger.warning(f"[Tool Result] Session {session.id} not connected, can't send result")
        
    except Exception as e:
        # Send error via SSE
        logger.error(f"[Tool Error] Session {session.id}: {tool_name} - {str(e)}")
        logger.error(traceback.format_exc())
        
        error_event = {
            "request_id": request_id,
            "tool": tool_name,
            "error": str(e),
            "code": -32603,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }
        
        # Add directly to queue
        if session.is_connected and session.event_queue:
            await session.event_queue.put({
                "type": "tool_error",
                "data": error_event,
                "timestamp": time.time()
            })
            logger.info(f"[Tool Error] Added to event queue for session {session.id}")
        else:
            logger.warning(f"[Tool Error] Session {session.id} not connected, can't send error")


async def get_tools(request: Request) -> JSONResponse:
    """REST endpoint to get available tools"""
    # Get session ID from query params
    session_id = request.query_params.get("session_id")
    if not session_id or session_id not in active_sessions:
        return JSONResponse(
            status_code=403,
            content={
                "error": "Invalid or missing session ID",
                "message": "Please establish an SSE connection first to get a valid session ID"
            }
        )
    
    # Get session
    session = active_sessions[session_id]
    session.activity()
    
    # Build tool list
    tools_list = []
    for name, info in registered_tools.items():
        tools_list.append({
            "name": name,
            "description": info["description"],
            "schema": info["parameters"]
        })
    
    logger.info(f"[GET Tools] Session {session.id}: {len(tools_list)} tools")
    
    return JSONResponse(
        content={
            "jsonrpc": "2.0",
            "result": {
                "tools": tools_list,
                "count": len(tools_list),
                "server_time": time.strftime("%Y-%m-%d %H:%M:%S")
            }
        }
    )


# Example tools
async def get_server_time(**kwargs) -> str:
    """Get the current server time"""
    return f"Current server time: {time.strftime('%Y-%m-%d %H:%M:%S')}"


async def calculate(expression: str = "", **kwargs) -> str:
    """Perform basic math calculations"""
    if not expression:
        return "No expression provided"
    
    try:
        # Safely evaluate the expression
        allowed_names = {"abs": abs, "max": max, "min": min, "sum": sum, "pow": pow}
        result = eval(expression, {"__builtins__": {}}, allowed_names)
        return f"Result: {result}"
    except Exception as e:
        return f"Error calculating: {str(e)}"


async def get_system_info(**kwargs) -> Dict:
    """Get system information"""
    import platform
    
    return {
        "os": platform.system(),
        "os_version": platform.version(),
        "python_version": platform.python_version(),
        "cpu_count": os.cpu_count()
    }


async def translate_text(text: str, target_language: str = "en", **kwargs) -> str:
    """Simulate translation functionality"""
    languages = {
        "en": "English",
        "zh": "Chinese",
        "fr": "French",
        "de": "German",
        "ja": "Japanese",
        "es": "Spanish"
    }
    
    if target_language.lower() not in languages:
        return f"Unsupported target language. Supported languages: {', '.join(languages.keys())}"
    
    return f"[Translation to {languages[target_language.lower()]}] {text}"


# Request logging middleware
async def log_middleware(request: Request, call_next):
    """Log all requests"""
    start_time = time.time()
    client_info = f"{request.client.host}:{request.client.port}" if request.client else "unknown"
    
    logger.info(f"[Request] {request.method} {request.url.path} from {client_info}")
    
    try:
        response = await call_next(request)
        duration = time.time() - start_time
        logger.info(f"[Response] {request.method} {request.url.path} - {response.status_code} ({duration:.3f}s)")
        return response
    except Exception as e:
        logger.error(f"[Error] {request.method} {request.url.path} - {str(e)}")
        logger.error(traceback.format_exc())
        return JSONResponse(
            status_code=500,
            content={"error": "Internal Server Error"}
        )


def main(port: int = 8001, debug: bool = False):
    """Start the MCP server"""
    # Set log level
    if debug:
        logger.setLevel(logging.DEBUG)
    
    # Register built-in tools
    register_tool("get_server_time", get_server_time, "Get the current server time")
    register_tool("calculate", calculate, "Perform basic mathematical calculations")
    register_tool("system_info", get_system_info, "Get system information")
    register_tool("translate", translate_text, "Translate text to another language")
    
    # Define routes
    routes = [
        Route("/sse", endpoint=create_session, methods=["GET"]),
        Route("/messages", endpoint=handle_message, methods=["POST"]),
        Route("/tools", endpoint=get_tools, methods=["GET"]),
    ]
    
    # Create application
    middleware = [
        Middleware(BaseHTTPMiddleware, dispatch=log_middleware)
    ]
    
    app = Starlette(
        debug=debug,
        routes=routes,
        middleware=middleware
    )
    
    # Log startup info
    logger.info(f"[Server] Starting MCP server on port {port}")
    logger.info(f"[Tools] Registered {len(registered_tools)} tools")
    logger.info(f"[Endpoints] SSE: http://localhost:{port}/sse")
    logger.info(f"[Endpoints] Messages: http://localhost:{port}/messages")
    logger.info(f"[Endpoints] Tools: http://localhost:{port}/tools")
    logger.info("[Usage] 1. Connect to /sse to get a session ID")
    logger.info("[Usage] 2. Use session ID with other endpoints")
    
    # Start server
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=port,
        log_level="info" if not debug else "debug"
    )


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="MCP Server")
    parser.add_argument("--port", type=int, default=8001, help="Port to listen on")
    parser.add_argument("--debug", action="store_true", help="Enable debug mode")
    
    args = parser.parse_args()
    main(port=args.port, debug=args.debug)
