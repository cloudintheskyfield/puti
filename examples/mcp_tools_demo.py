"""
@Author: obstacles
@Time:  2025-07-14 16:35
@Description: Demonstration of the simplified MCP server's functionality

This script demonstrates the functionality of the simplified MCP server implementation.
The MCP server implements a JSON-RPC over HTTP server with SSE for asynchronous tool results.

The server provides:
1. SSE Connection (/sse) - Establishes a session and creates a persistent connection for events
2. JSON-RPC Messages (/messages) - Processes JSON-RPC requests with session authentication
3. Tool Listing (/tools) - Returns a list of available tools via REST API

The client flow is:
1. Connect to SSE endpoint to get a session ID
2. Use the session ID for all subsequent requests
3. Call tools asynchronously and receive results through the SSE connection

Supported methods:
- initialize: Initialize a session
- tools/list: Get a list of available tools
- tools/call: Call a tool asynchronously

Features:
- Session management with unique session IDs
- Authentication of all requests with session ID
- Asynchronous tool execution with immediate response
- Real-time tool results via SSE connection
- Error handling and reporting via SSE events
"""
import json
import time
import asyncio
import aiohttp
import logging
from typing import Dict, Any, List, Optional

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("mcp_tools_demo")

class MCPDemo:
    """MCP server demonstration"""
    
    def __init__(self, server_url: str = "http://localhost:8001"):
        self.server_url = server_url.rstrip("/")
        self.session_id = None
        self.http_client = None
        self.sse_task = None
        self.events = []
        self.request_count = 0
    
    async def start(self):
        """Start the demo"""
        print("\n" + "=" * 60)
        print("MCP Server Demonstration")
        print("=" * 60)
        
        # Create HTTP client
        self.http_client = aiohttp.ClientSession()
        
        # Connect to SSE endpoint
        print("\nStep 1: Connecting to SSE endpoint")
        if not await self.connect_sse():
            print("Failed to connect to SSE endpoint")
            return False
        
        # Initialize session
        print("\nStep 2: Initializing session")
        if not await self.initialize_session():
            print("Failed to initialize session")
            return False
        
        # Get tools list
        print("\nStep 3: Getting tool list")
        tools = await self.get_tools()
        if not tools:
            print("Failed to get tools list")
            return False
        
        # Call each tool
        print("\nStep 4: Testing tools")
        success = await self.test_tools()
        if not success:
            print("Tool testing failed")
            return False
        
        print("\nDemo completed successfully!")
        return True
    
    async def connect_sse(self) -> bool:
        """Connect to SSE endpoint and get session ID"""
        try:
            url = f"{self.server_url}/sse"
            print(f"Connecting to {url}")
            
            # Start SSE connection
            response = await self.http_client.get(url)
            if response.status != 200:
                print(f"SSE connection failed: HTTP {response.status}")
                return False
            
            print("SSE connection established")
            
            # Start processing SSE events
            self.sse_task = asyncio.create_task(self._process_sse(response))
            
            # Wait for session ID
            for i in range(10):
                if self.session_id:
                    print(f"Got session ID: {self.session_id}")
                    return True
                print(f"Waiting for session ID... ({i+1}/10)")
                await asyncio.sleep(1)
            
            print("Timeout waiting for session ID")
            return False
            
        except Exception as e:
            print(f"Error connecting to SSE: {str(e)}")
            return False
    
    async def _process_sse(self, response):
        """Process SSE events"""
        try:
            # Track event parsing state
            event_type = None
            event_data = None
            
            # Process events
            async for line_bytes in response.content:
                line = line_bytes.decode('utf-8').strip()
                print(f"SSE > {line}")
                
                if not line:
                    # Empty line marks end of event
                    if event_type and event_data:
                        try:
                            data = json.loads(event_data)
                            await self._handle_event(event_type, data)
                        except json.JSONDecodeError:
                            print(f"Invalid JSON in event data: {event_data}")
                        
                        # Reset event state
                        event_type = None
                        event_data = None
                    continue
                
                # Parse event and data lines
                if line.startswith("event:"):
                    event_type = line.split(":", 1)[1].strip()
                elif line.startswith("data:"):
                    event_data = line.split(":", 1)[1].strip()
        
        except Exception as e:
            print(f"Error processing SSE events: {str(e)}")
        finally:
            print("SSE connection closed")
    
    async def _handle_event(self, event_type: str, data: Dict):
        """Handle SSE event"""
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
        elif event_type == "tool_result":
            print("\n=== TOOL RESULT ===")
            print(f"Request ID: {data.get('request_id')}")
            print(f"Tool: {data.get('tool')}")
            print(f"Result: {json.dumps(data.get('result', {}), indent=2)}")
            print("===================")
        elif event_type == "tool_error":
            print("\n=== TOOL ERROR ===")
            print(f"Request ID: {data.get('request_id')}")
            print(f"Tool: {data.get('tool')}")
            print(f"Error: {data.get('error')}")
            print("===================")
    
    async def send_request(self, method: str, params: Dict = None) -> Optional[Dict]:
        """Send JSON-RPC request to MCP server"""
        if not self.session_id:
            print("No session ID available")
            return None
        
        if params is None:
            params = {}
        
        # Create request ID
        self.request_count += 1
        request_id = self.request_count
        
        # Create request payload
        payload = {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
            "params": params
        }
        
        url = f"{self.server_url}/messages?session_id={self.session_id}"
        
        print(f"Sending request: {method} [ID: {request_id}]")
        print(f"Params: {json.dumps(params, indent=2)}")
        
        try:
            async with self.http_client.post(url, json=payload) as response:
                if response.status == 200:
                    result = await response.json()
                    print(f"Response: {json.dumps(result, indent=2)}")
                    return result
                else:
                    print(f"Request failed: HTTP {response.status}")
                    error_text = await response.text()
                    print(f"Error: {error_text}")
                    return None
        except Exception as e:
            print(f"Error sending request: {str(e)}")
            return None
    
    async def initialize_session(self) -> bool:
        """Initialize session with server"""
        result = await self.send_request("initialize", {
            "protocolVersion": "0.1",
            "capabilities": {},
            "clientInfo": {
                "name": "mcp-demo",
                "version": "1.0.0"
            }
        })
        
        return result is not None and "result" in result
    
    async def get_tools(self) -> List[Dict]:
        """Get list of available tools"""
        # Try REST endpoint first
        if self.session_id:
            url = f"{self.server_url}/tools?session_id={self.session_id}"
            
            try:
                print(f"Getting tools via REST: {url}")
                async with self.http_client.get(url) as response:
                    if response.status == 200:
                        result = await response.json()
                        tools = result.get("result", {}).get("tools", [])
                        print(f"Found {len(tools)} tools via REST")
                        for i, tool in enumerate(tools, 1):
                            print(f"{i}. {tool['name']} - {tool.get('description', 'No description')}")
                        return tools
            except Exception as e:
                print(f"Error getting tools via REST: {str(e)}")
        
        # Fall back to JSON-RPC
        result = await self.send_request("tools/list")
        if result and "result" in result and "tools" in result["result"]:
            tools = result["result"]["tools"]
            print(f"Found {len(tools)} tools via JSON-RPC")
            for i, tool in enumerate(tools, 1):
                print(f"{i}. {tool['name']} - {tool.get('description', 'No description')}")
            return tools
        
        return []
    
    async def test_tools(self) -> bool:
        """Test each tool"""
        # Test calculate tool
        print("\nTesting calculate tool")
        calc_result = await self.send_request("tools/call", {
            "name": "calculate",
            "arguments": {
                "expression": "2 + 3 * 4"
            }
        })
        
        if not calc_result or "result" not in calc_result:
            print("Failed to call calculate tool")
            return False
        
        print("Waiting for calculate result via SSE...")
        await asyncio.sleep(3)
        
        # Test system_info tool
        print("\nTesting system_info tool")
        sys_result = await self.send_request("tools/call", {
            "name": "system_info"
        })
        
        if not sys_result or "result" not in sys_result:
            print("Failed to call system_info tool")
            return False
        
        print("Waiting for system_info result via SSE...")
        await asyncio.sleep(3)
        
        # Test translate tool
        print("\nTesting translate tool")
        trans_result = await self.send_request("tools/call", {
            "name": "translate",
            "arguments": {
                "text": "Hello world!",
                "target_language": "zh"
            }
        })
        
        if not trans_result or "result" not in trans_result:
            print("Failed to call translate tool")
            return False
        
        print("Waiting for translate result via SSE...")
        await asyncio.sleep(3)
        
        return True
    
    async def close(self):
        """Clean up resources"""
        # Cancel SSE task
        if self.sse_task and not self.sse_task.done():
            self.sse_task.cancel()
            try:
                await self.sse_task
            except asyncio.CancelledError:
                pass
        
        # Close HTTP client
        if self.http_client:
            await self.http_client.close()


async def main():
    """Run the MCP demo"""
    demo = MCPDemo()
    
    try:
        await demo.start()
    except KeyboardInterrupt:
        print("\nDemo interrupted by user")
    except Exception as e:
        print(f"\nError in demo: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        await demo.close()


if __name__ == "__main__":
    asyncio.run(main()) 