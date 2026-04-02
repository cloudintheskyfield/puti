"""
@Author: obstacles
@Time:  2025-07-14 17:00
@Description: Test the simplified MCP server integration
"""
import os
import sys
import json
import time
import asyncio
import aiohttp
import logging
import unittest
from typing import Dict, Any, Optional

# 添加项目根目录到路径
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(project_root)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("mcp_integration_test")


class SimplifiedMCPClient:
    """简单MCP客户端用于测试"""
    
    def __init__(self, server_url: str):
        self.server_url = server_url.rstrip('/')
        self.session_id = None
        self.http_session = None
        self.sse_response = None
        self.sse_task = None
        self.sse_events = []
        self.request_id = 0
    
    async def __aenter__(self):
        self.http_session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.sse_task and not self.sse_task.done():
            self.sse_task.cancel()
            try:
                await self.sse_task
            except asyncio.CancelledError:
                pass
        
        if self.http_session:
            await self.http_session.close()
    
    async def connect_sse(self) -> bool:
        """连接到SSE端点并获取会话ID"""
        try:
            sse_url = f"{self.server_url}/sse"
            logger.info(f"Connecting to SSE endpoint: {sse_url}")
            
            self.sse_response = await self.http_session.get(sse_url)
            if self.sse_response.status != 200:
                logger.error(f"Failed to connect to SSE: HTTP {self.sse_response.status}")
                return False
            
            # 开始处理SSE事件
            self.sse_task = asyncio.create_task(self._process_sse_events())
            
            # 等待获取会话ID
            for i in range(10):  # 最多等待10秒
                if self.session_id:
                    logger.info(f"Got session ID: {self.session_id}")
                    return True
                await asyncio.sleep(0.5)
            
            logger.error("Timeout waiting for session ID")
            return False
        
        except Exception as e:
            logger.error(f"SSE connection error: {str(e)}")
            return False
    
    async def _process_sse_events(self):
        """处理SSE事件流"""
        event_type = None
        event_data = ""
        
        try:
            logger.info("Starting SSE event processing")
            async for line in self.sse_response.content:
                line = line.decode('utf-8').strip()
                logger.debug(f"SSE line: {line}")
                
                if not line:
                    # Empty line marks the end of an event
                    if event_type and event_data:
                        try:
                            data_json = json.loads(event_data)
                            logger.info(f"Processing complete event: {event_type} - {event_data}")
                            await self._handle_event(event_type, data_json)
                        except json.JSONDecodeError:
                            logger.error(f"Invalid JSON data: {event_data}")
                        # Reset for next event
                        event_type = None
                        event_data = ""
                    continue
                
                if line.startswith('event:'):
                    event_type = line.split(':', 1)[1].strip()
                    logger.debug(f"Found event type: {event_type}")
                elif line.startswith('data:') and event_type:
                    event_data = line.split(':', 1)[1].strip()
                    logger.debug(f"Found event data: {event_data}")
                
        except Exception as e:
            logger.error(f"SSE event processing error: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
    
    async def _handle_event(self, event_type: str, data: Dict):
        """处理SSE事件"""
        event = {"type": event_type, "data": data}
        self.sse_events.append(event)
        
        if event_type == 'session':
            self.session_id = data.get('session_id')
        
        logger.info(f"Received SSE event: {event_type}")
    
    async def get_tool_list_direct(self) -> Optional[Dict]:
        """直接获取工具列表 (GET /tools)"""
        if not self.session_id:
            logger.error("No session ID available")
            return None
        
        url = f"{self.server_url}/tools?session_id={self.session_id}"
        
        try:
            async with self.http_session.get(url) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    logger.error(f"Failed to get tools: HTTP {response.status}")
                    error_text = await response.text()
                    logger.error(f"Error details: {error_text}")
                    return None
        except Exception as e:
            logger.error(f"Request error: {str(e)}")
            return None
    
    async def get_tool_list_direct_no_session(self) -> Optional[Dict]:
        """直接获取工具列表但不提供会话ID (应该失败)"""
        url = f"{self.server_url}/tools"
        
        try:
            async with self.http_session.get(url) as response:
                return {
                    "status": response.status,
                    "body": await response.text()
                }
        except Exception as e:
            logger.error(f"Request error: {str(e)}")
            return None
    
    async def get_tool_list_direct_invalid_session(self) -> Optional[Dict]:
        """直接获取工具列表但提供无效会话ID (应该失败)"""
        url = f"{self.server_url}/tools?session_id=invalid-session-id"
        
        try:
            async with self.http_session.get(url) as response:
                return {
                    "status": response.status,
                    "body": await response.text()
                }
        except Exception as e:
            logger.error(f"Request error: {str(e)}")
            return None
    
    async def send_jsonrpc_request(self, method: str, params: Dict = None) -> Optional[Dict]:
        """发送JSON-RPC请求"""
        if not self.session_id:
            logger.error("No session ID available")
            return None
        
        if params is None:
            params = {}
        
        url = f"{self.server_url}/messages/?session_id={self.session_id}"
        
        # 生成请求ID
        self.request_id += 1
        req_id = self.request_id
        
        # 构造JSON-RPC请求
        payload = {
            "jsonrpc": "2.0",
            "id": req_id,
            "method": method,
            "params": params
        }
        
        logger.info(f"Sending JSON-RPC request: {method} [ID: {req_id}]")
        
        try:
            async with self.http_session.post(url, json=payload) as response:
                if response.status == 200:
                    result = await response.json()
                    return result
                else:
                    logger.error(f"Request failed: HTTP {response.status}")
                    error_text = await response.text()
                    logger.error(f"Error details: {error_text}")
                    return None
        except Exception as e:
            logger.error(f"Request error: {str(e)}")
            return None
    
    async def wait_for_sse_event(self, event_type: str, timeout: float = 5.0) -> Optional[Dict]:
        """等待特定类型的SSE事件"""
        # 首先检查是否已经收到了该类型的事件
        for event in self.sse_events:
            if event["type"] == event_type:
                return event
        
        # 如果没有，等待新事件
        start_time = time.time()
        while time.time() - start_time < timeout:
            for event in self.sse_events:
                if event["type"] == event_type:
                    return event
            await asyncio.sleep(0.1)
        
        logger.error(f"Timeout waiting for SSE event: {event_type}")
        return None
    
    async def wait_for_tool_result(self, request_id: int, timeout: float = 5.0) -> Optional[Dict]:
        """等待特定请求ID的工具执行结果"""
        start_time = time.time()
        while time.time() - start_time < timeout:
            for event in self.sse_events:
                if event["type"] == "tool_result" and event["data"].get("request_id") == request_id:
                    return event["data"]
            await asyncio.sleep(0.1)
        
        logger.error(f"Timeout waiting for tool result for request ID: {request_id}")
        return None


class TestSimplifiedMCPServer(unittest.TestCase):
    """测试简化版MCP服务器"""
    
    def setUp(self):
        self.server_url = "http://localhost:8001"
        self.loop = asyncio.get_event_loop()
    
    def test_session_validation(self):
        """测试会话验证机制"""
        async def _test():
            async with SimplifiedMCPClient(self.server_url) as client:
                # 测试无会话ID的请求
                result = await client.get_tool_list_direct_no_session()
                self.assertIsNotNone(result)
                self.assertEqual(result["status"], 400)  # 应该返回400错误
                
                # 测试无效会话ID的请求
                result = await client.get_tool_list_direct_invalid_session()
                self.assertIsNotNone(result)
                self.assertEqual(result["status"], 403)  # 应该返回403错误
                
                # 测试正常连接
                connected = await client.connect_sse()
                self.assertTrue(connected)
                self.assertIsNotNone(client.session_id)
                
                # 测试有效会话ID的请求
                result = await client.get_tool_list_direct()
                self.assertIsNotNone(result)
                self.assertIn("result", result)
                self.assertIn("tools", result["result"])
        
        self.loop.run_until_complete(_test())
    
    def test_tool_call_results_via_sse(self):
        """测试工具调用结果通过SSE返回"""
        async def _test():
            async with SimplifiedMCPClient(self.server_url) as client:
                # 连接SSE
                connected = await client.connect_sse()
                self.assertTrue(connected)
                
                # 初始化会话
                init_result = await client.send_jsonrpc_request("initialize", {
                    "protocolVersion": "0.1",
                    "capabilities": {},
                    "clientInfo": {
                        "name": "test-client",
                        "version": "1.0.0"
                    }
                })
                self.assertIsNotNone(init_result)
                self.assertIn("result", init_result)
                
                # 获取工具列表
                tools_result = await client.send_jsonrpc_request("tools/list")
                self.assertIsNotNone(tools_result)
                self.assertIn("result", tools_result)
                self.assertIn("tools", tools_result["result"])
                
                # 调用计算器工具
                calc_result = await client.send_jsonrpc_request("tools/call", {
                    "name": "calculate",
                    "arguments": {
                        "expression": "2 + 3 * 4"
                    }
                })
                self.assertIsNotNone(calc_result)
                self.assertIn("result", calc_result)
                self.assertEqual(calc_result["result"]["status"], "accepted")
                
                # 等待SSE事件返回工具执行结果
                tool_result = await client.wait_for_tool_result(client.request_id)
                self.assertIsNotNone(tool_result)
                self.assertEqual(tool_result["tool"], "calculate")
                self.assertIn("result", tool_result)
                
                # 确认工具执行结果包含计算结果
                result_value = tool_result["result"].get("result", "")
                self.assertIn("Result: ", result_value)
        
        self.loop.run_until_complete(_test())


if __name__ == "__main__":
    unittest.main() 