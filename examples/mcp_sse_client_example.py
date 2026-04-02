"""
@Author: obstacles
@Time:  2025-07-14 16:40
@Description: Simple MCP SSE client to test the MCP server
"""
import sys
import json
import time
import asyncio
import aiohttp
import logging
from typing import Dict, Any, List, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("mcp_client")

class SimpleMCPClient:
    """Simple client for MCP server with SSE support"""
    
    def __init__(self, server_url: str):
        self.server_url = server_url.rstrip("/")
        self.session_id = None
        self.http_session = None
        self.sse_task = None
        self.events = []
        self.request_counter = 0
    
    async def connect(self) -> bool:
        """Connect to SSE endpoint and get session ID"""
        self.http_session = aiohttp.ClientSession()
        sse_url = f"{self.server_url}/sse"
        
        print(f"Connecting to SSE endpoint: {sse_url}")
        
        try:
            # Create SSE connection task
            self.sse_task = asyncio.create_task(self._process_sse_stream(sse_url))
            
            # Wait for session ID
            for i in range(10):
                if self.session_id:
                    print(f"Connected with session ID: {self.session_id}")
                    return True
                await asyncio.sleep(0.5)
                print(f"Waiting for session ID... ({i+1}/10)")
            
            print("Timeout waiting for session ID")
            return False
        
        except Exception as e:
            print(f"Connection error: {str(e)}")
            return False
    
    async def _process_sse_stream(self, url: str):
        """Process SSE events from server"""
        try:
            async with self.http_session.get(url) as response:
                if response.status != 200:
                    print(f"SSE connection failed: HTTP {response.status}")
                    return
                
                print("SSE connection established")
                
                # Process event stream
                event_type = None
                event_data = ""
                
                async for line in response.content:
                    line = line.decode("utf-8").strip()
                    print(f"SSE > {line}")
                    
                    if not line:
                        # Empty line marks the end of an event
                        if event_type and event_data:
                            try:
                                data = json.loads(event_data)
                                await self._handle_event(event_type, data)
                            except json.JSONDecodeError:
                                print(f"Invalid JSON data: {event_data}")
                            
                            # Reset for next event
                            event_type = None
                            event_data = ""
                        continue
                    
                    if line.startswith("event:"):
                        event_type = line.split(":", 1)[1].strip()
                    elif line.startswith("data:") and event_type:
                        event_data = line.split(":", 1)[1].strip()
        
        except Exception as e:
            print(f"SSE processing error: {str(e)}")
            import traceback
            traceback.print_exc()
        
        finally:
            print("SSE connection closed")
    
    async def _handle_event(self, event_type: str, data: Dict):
        """Handle SSE events"""
        # Store event
        event = {
            "type": event_type,
            "data": data,
            "time": time.time()
        }
        self.events.append(event)
        
        # Handle specific event types
        if event_type == "session":
            self.session_id = data.get("session_id")
            print(f"Session established: {self.session_id}")
        elif event_type == "tool_result":
            request_id = data.get("request_id")
            tool = data.get("tool")
            result = data.get("result")
            
            print(f"\n📊 TOOL RESULT [Request {request_id}]")
            print(f"Tool: {tool}")
            print(f"Result: {json.dumps(result, indent=2)}")
        elif event_type == "tool_error":
            request_id = data.get("request_id")
            tool = data.get("tool")
            error = data.get("error")
            
            print(f"\n❌ TOOL ERROR [Request {request_id}]")
            print(f"Tool: {tool}")
            print(f"Error: {error}")
    
    async def call_rpc(self, method: str, params: Dict = None) -> Optional[Dict]:
        """Call a JSON-RPC method"""
        if not self.session_id:
            print("No active session")
            return None
        
        if params is None:
            params = {}
        
        # Create unique request ID
        self.request_counter += 1
        request_id = self.request_counter
        
        # Create request payload
        payload = {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
            "params": params
        }
        
        url = f"{self.server_url}/messages?session_id={self.session_id}"
        
        print(f"\nCalling {method} [Request {request_id}]")
        print(f"Params: {json.dumps(params, indent=2)}")
        
        try:
            async with self.http_session.post(url, json=payload) as response:
                if response.status == 200:
                    result = await response.json()
                    print(f"Response: {json.dumps(result, indent=2)}")
                    return result
                else:
                    error_text = await response.text()
                    print(f"Request failed: HTTP {response.status}")
                    print(f"Error: {error_text}")
                    return None
        except Exception as e:
            print(f"RPC call error: {str(e)}")
            return None
    
    async def initialize(self) -> bool:
        """Initialize the session"""
        result = await self.call_rpc("initialize", {
            "protocolVersion": "0.1",
            "capabilities": {},
            "clientInfo": {
                "name": "mcp-test-client",
                "version": "1.0.0"
            }
        })
        
        return result is not None and "result" in result
    
    async def list_tools(self) -> List[Dict]:
        """Get list of available tools"""
        result = await self.call_rpc("tools/list")
        
        if result and "result" in result and "tools" in result["result"]:
            tools = result["result"]["tools"]
            print(f"\nAvailable tools: {len(tools)}")
            for i, tool in enumerate(tools, 1):
                print(f"{i}. {tool['name']} - {tool.get('description', 'No description')}")
            return tools
        
        return []
    
    async def call_tool(self, name: str, arguments: Dict = None) -> bool:
        """Call a tool"""
        if arguments is None:
            arguments = {}
        
        result = await self.call_rpc("tools/call", {
            "name": name,
            "arguments": arguments
        })
        
        if result and "result" in result and "status" in result["result"]:
            print(f"Tool call accepted: {result['result'].get('message', '')}")
            print("Waiting for result via SSE...")
            return True
        
        return False
    
    async def get_tools_rest(self) -> Optional[Dict]:
        """Get tools via REST endpoint"""
        if not self.session_id:
            print("No active session")
            return None
        
        url = f"{self.server_url}/tools?session_id={self.session_id}"
        
        print(f"\nGetting tools via REST: GET {url}")
        
        try:
            async with self.http_session.get(url) as response:
                if response.status == 200:
                    result = await response.json()
                    print(f"Response: {json.dumps(result, indent=2)}")
                    return result
                else:
                    error_text = await response.text()
                    print(f"Request failed: HTTP {response.status}")
                    print(f"Error: {error_text}")
                    return None
        except Exception as e:
            print(f"REST call error: {str(e)}")
            return None
    
    async def close(self):
        """Close the client connection"""
        if self.sse_task and not self.sse_task.done():
            self.sse_task.cancel()
            try:
                await self.sse_task
            except asyncio.CancelledError:
                pass
        
        if self.http_session:
            await self.http_session.close()
            print("\nClient connection closed")


async def run_demo():
    """Run a demo of the MCP client"""
    server_url = "http://localhost:8001"
    
    print("=" * 50)
    print("MCP Client Demo")
    print("=" * 50)
    
    # Create client
    client = SimpleMCPClient(server_url)
    
    try:
        # Connect to SSE
        if not await client.connect():
            print("Failed to connect")
            return
        
        # Initialize session
        if not await client.initialize():
            print("Failed to initialize session")
            return
        
        # Get tools (REST endpoint)
        await client.get_tools_rest()
        
        # Get tools (RPC method)
        tools = await client.list_tools()
        if not tools:
            print("No tools available")
            return
        
        # Call calculate tool
        print("\nTesting calculate tool")
        await client.call_tool("calculate", {"expression": "2 + 3 * 4"})
        
        # Wait for result
        await asyncio.sleep(2)
        
        # Call system_info tool
        print("\nTesting system_info tool")
        await client.call_tool("system_info")
        
        # Wait for result
        await asyncio.sleep(2)
        
        # Call translate tool
        print("\nTesting translate tool")
        await client.call_tool("translate", {
            "text": "Hello, world!",
            "target_language": "zh"
        })
        
        # Wait for result
        await asyncio.sleep(2)
        
        # Demo complete
        print("\nDemo complete!")
    
    except KeyboardInterrupt:
        print("\nDemo interrupted by user")
    except Exception as e:
        print(f"\nError: {str(e)}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Close client
        await client.close()


if __name__ == "__main__":
    asyncio.run(run_demo()) 