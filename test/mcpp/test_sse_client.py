"""
@Author: obstacles
@Time:  2023-07-22 10:30
@Description: 测试SSE模式的MCP客户端
"""
import sys
import os
import json
import asyncio
import logging
import time
import uuid
import requests
import aiohttp
from urllib.parse import urljoin

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from puti.constant.client import McpTransportMethod

# 配置日志
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("mcp_sse_test")

class SimpleMCPClient:
    """简易MCP客户端，使用SSE获取会话ID并调用工具"""
    
    def __init__(self, server_url):
        self.server_url = server_url.rstrip('/')
        self.session_id = None
        self.http_session = None
        self.sse_response = None
    
    async def connect(self):
        """连接到SSE端点获取会话ID"""
        self.http_session = aiohttp.ClientSession()
        sse_url = f"{self.server_url}/sse"
        
        logger.info(f"[连接] 连接到SSE端点: {sse_url}")
        
        try:
            # 创建长连接
            self.sse_response = await self.http_session.get(sse_url, timeout=30)
            
            if self.sse_response.status != 200:
                logger.error(f"[连接错误] SSE连接失败，状态码: {self.sse_response.status}")
                return False
            
            logger.info(f"[连接成功] SSE连接已建立")
            
            # 开始接收SSE事件
            asyncio.create_task(self._process_sse_events())
            
            # 等待获取session_id
            for _ in range(10):  # 最多等待10秒
                if self.session_id:
                    logger.info(f"[初始化] 成功获取会话ID: {self.session_id}")
                    return True
                await asyncio.sleep(1)
            
            logger.error("[连接超时] 等待会话ID超时")
            return False
            
        except Exception as e:
            logger.error(f"[连接异常] SSE连接失败: {str(e)}")
            return False
    
    async def _process_sse_events(self):
        """处理SSE事件流"""
        try:
            event_name = None
            
            async for line in self.sse_response.content:
                line = line.decode('utf-8').strip()
                
                if not line:
                    continue
                
                if line.startswith('event:'):
                    event_name = line.split(':', 1)[1].strip()
                elif line.startswith('data:') and event_name:
                    data = line.split(':', 1)[1].strip()
                    try:
                        data_json = json.loads(data)
                        await self._handle_sse_event(event_name, data_json)
                    except json.JSONDecodeError:
                        logger.error(f"[SSE解析错误] 无效的JSON数据: {data}")
                    except Exception as e:
                        logger.error(f"[SSE处理错误] 处理事件 {event_name} 失败: {str(e)}")
        except Exception as e:
            logger.error(f"[SSE处理错误] 处理SSE事件流出错: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
    
    async def _handle_sse_event(self, event_type, data):
        """处理SSE事件"""
        if event_type == 'session':
            self.session_id = data.get('session_id')
            logger.info(f"[SSE事件] 收到会话ID: {self.session_id}")
        elif event_type == 'ping':
            logger.debug(f"[SSE Ping] 收到ping: {data}")
        else:
            logger.info(f"[SSE事件] 收到事件 {event_type}: {data}")
    
    async def initialize(self):
        """初始化会话"""
        if not self.session_id:
            logger.error("[初始化错误] 没有会话ID，请先连接SSE端点")
            return False
        
        url = f"{self.server_url}/messages/?session_id={self.session_id}"
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "0.1",
                "capabilities": {},
                "clientInfo": {
                    "name": "test-client",
                    "version": "1.0"
                }
            }
        }
        
        try:
            logger.info("[初始化] 发送初始化请求")
            async with self.http_session.post(url, json=payload) as response:
                result = await response.json()
                logger.info(f"[初始化] 收到响应: {result}")
                return 'result' in result
        except Exception as e:
            logger.error(f"[初始化错误] 初始化失败: {str(e)}")
            return False
    
    async def list_tools(self):
        """获取工具列表"""
        if not self.session_id:
            logger.error("[工具列表错误] 没有会话ID，请先连接SSE端点")
            return None
        
        url = f"{self.server_url}/messages/?session_id={self.session_id}"
        payload = {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/list",
            "params": {}
        }
        
        try:
            logger.info("[工具列表] 获取工具列表")
            async with self.http_session.post(url, json=payload) as response:
                result = await response.json()
                logger.info(f"[工具列表] 收到响应")
                
                if 'result' in result and 'tools' in result['result']:
                    tools = result['result']['tools']
                    logger.info(f"[工具列表] 找到 {len(tools)} 个工具")
                    return tools
                else:
                    logger.error(f"[工具列表错误] 响应格式错误: {result}")
                    return None
        except Exception as e:
            logger.error(f"[工具列表错误] 获取工具列表失败: {str(e)}")
            return None
    
    async def call_tool(self, tool_name, arguments=None):
        """调用工具"""
        if not self.session_id:
            logger.error("[工具调用错误] 没有会话ID，请先连接SSE端点")
            return None
        
        if arguments is None:
            arguments = {}
        
        url = f"{self.server_url}/messages/?session_id={self.session_id}"
        payload = {
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {
                "name": tool_name,
                "arguments": arguments
            }
        }
        
        try:
            logger.info(f"[工具调用] 调用工具: {tool_name}")
            async with self.http_session.post(url, json=payload) as response:
                result = await response.json()
                logger.info(f"[工具调用] 收到响应")
                return result
        except Exception as e:
            logger.error(f"[工具调用错误] 调用工具失败: {str(e)}")
            return None
    
    async def close(self):
        """关闭连接"""
        if self.http_session:
            await self.http_session.close()
            logger.info("[关闭] 已关闭HTTP会话")

async def test_sse_client():
    """测试SSE模式的MCP客户端"""
    # 设置服务器URL
    server_url = "http://localhost:8001"
    
    logger.info(f"[测试] 开始测试SSE客户端，服务器URL: {server_url}")
    
    # 创建客户端
    client = SimpleMCPClient(server_url)
    
    try:
        # 连接到SSE端点获取会话ID
        connected = await client.connect()
        if not connected:
            logger.error("[测试失败] 无法连接到SSE端点")
            return
        
        # 初始化会话
        initialized = await client.initialize()
        if not initialized:
            logger.error("[测试失败] 无法初始化会话")
            return
        
        # 获取工具列表
        tools = await client.list_tools()
        if not tools:
            logger.error("[测试失败] 无法获取工具列表")
            return
        
        logger.info(f"[工具列表] 可用工具: {[tool['name'] for tool in tools]}")
        
        # 测试工具调用 - 获取服务器时间
        logger.info("\n[测试] 调用 get_server_time 工具...")
        result = await client.call_tool("get_server_time")
        logger.info(f"[结果] get_server_time: {result}")
        
        # 测试工具调用 - 计算表达式
        logger.info("\n[测试] 调用 calculate 工具...")
        result = await client.call_tool("calculate", {"expression": "2 + 2 * 3"})
        logger.info(f"[结果] calculate: {result}")
        
        # 测试工具调用 - 系统信息
        logger.info("\n[测试] 调用 system_info 工具...")
        result = await client.call_tool("system_info")
        logger.info(f"[结果] system_info: {result}")
        
        # 测试工具调用 - 翻译
        logger.info("\n[测试] 调用 translate 工具...")
        result = await client.call_tool("translate", {
            "text": "Hello, world!",
            "target_language": "zh"
        })
        logger.info(f"[结果] translate: {result}")
        
        logger.info("[测试成功] 所有测试都已完成")
        
    except Exception as e:
        logger.error(f"[测试异常] 测试过程中出错: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        # 关闭客户端
        await client.close()


if __name__ == "__main__":
    asyncio.run(test_sse_client()) 