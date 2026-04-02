"""
@Author: obstacles
@Time:  2025-07-11 13:35
@Description:  测试MCP工具客户端
"""
import os
import sys
import json
import time
import logging
import argparse
import re
import requests
import asyncio
import aiohttp
import uuid
from typing import Dict, Any, Optional
from urllib.parse import urljoin, urlparse, parse_qs

# 添加根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# 配置日志
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("mcp_tool_client")

class McpToolClient:
    """MCP工具客户端"""
    
    def __init__(self, base_url: str = "http://127.0.0.1:8000"):
        self.base_url = base_url.rstrip('/')
        self.tools_endpoint = f"{self.base_url}/tools"
        self.sse_endpoint = f"{self.base_url}/sse"
        self.session_id = str(uuid.uuid4())
        self.message_endpoint = f"{self.base_url}/messages/?session_id={self.session_id}"
        self.request_id = 1
        
    def connect(self) -> bool:
        """连接到服务器并获取有效的session_id"""
        logger.info(f"[连接] 尝试连接到服务器: {self.base_url}")
        
        try:
            # 1. 首先获取工具列表
            response = self._http_request("GET", self.tools_endpoint)
            if response.status_code == 200:
                tools_data = response.json()
                tools_count = tools_data.get("count", 0)
                logger.info(f"[连接成功] 获取到 {tools_count} 个工具")
                logger.debug(f"[工具列表] {json.dumps(tools_data, indent=2)}")
            else:
                logger.error(f"[连接失败] 无法获取工具列表, HTTP状态码: {response.status_code}")
                return False
                
            logger.info(f"[会话] 使用会话ID: {self.session_id}")
            return True
        except Exception as e:
            logger.error(f"[连接失败] {str(e)}")
            return False
    
    def initialize(self) -> bool:
        """初始化会话"""
        logger.info("[初始化] 准备发送initialize请求...")
        
        payload = {
            "jsonrpc": "2.0",
            "id": self.request_id,
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
        self.request_id += 1
        
        logger.debug(f"[请求体] {json.dumps(payload, indent=2)}")
        
        response = self._http_request("POST", self.message_endpoint, json=payload)
        
        if response.status_code == 200:
            try:
                response_data = response.json()
                logger.info("[初始化成功] 会话初始化完成")
                logger.debug(f"[响应] {json.dumps(response_data, indent=2)}")
                return True
            except Exception as e:
                logger.warning(f"[解析警告] 无法解析响应内容: {str(e)}")
                return False
        else:
            logger.error(f"[初始化失败] HTTP状态码: {response.status_code}")
            try:
                error_text = response.text
                logger.error(f"[错误响应] {error_text}")
            except Exception:
                pass
            return False
    
    def list_tools(self) -> Dict:
        """获取可用工具列表"""
        logger.info("[工具] 发送工具列表请求...")
        
        payload = {
            "jsonrpc": "2.0",
            "id": self.request_id,
            "method": "tools/list",
            "params": {}
        }
        self.request_id += 1
        
        logger.debug(f"[请求体] {json.dumps(payload, indent=2)}")
        
        response = self._http_request("POST", self.message_endpoint, json=payload)
        
        if response.status_code == 200:
            response_data = response.json()
            tools_data = response_data.get("result", {})
            tools = tools_data.get("tools", [])
            logger.info(f"[工具] 获取到 {len(tools)} 个工具")
            logger.debug(f"[响应] {json.dumps(response_data, indent=2)}")
            return tools_data
        else:
            logger.error(f"[工具列表失败] HTTP状态码: {response.status_code}")
            try:
                error_text = response.text
                logger.error(f"[错误响应] {error_text}")
            except Exception:
                pass
            return {"tools": []}
    
    def call_tool(self, tool_name: str, params: Dict[str, Any] = None) -> Dict:
        """调用指定工具"""
        logger.info(f"[调用工具] 准备调用工具: {tool_name}")
        
        if params is None:
            params = {}
            
        payload = {
            "jsonrpc": "2.0",
            "id": self.request_id,
            "method": "tools/call",
            "params": {
                "name": tool_name,
                "arguments": params
            }
        }
        self.request_id += 1
        
        logger.debug(f"[请求体] {json.dumps(payload, indent=2)}")
        
        response = self._http_request("POST", self.message_endpoint, json=payload)
        
        if response.status_code == 200:
            response_data = response.json()
            result = response_data.get("result", {})
            logger.info(f"[工具结果] 工具 {tool_name} 调用成功")
            logger.debug(f"[响应] {json.dumps(response_data, indent=2)}")
            return result
        else:
            logger.error(f"[工具调用失败] HTTP状态码: {response.status_code}")
            try:
                error_text = response.text
                logger.error(f"[错误响应] {error_text}")
            except Exception:
                pass
            return {"status": "error", "error": f"HTTP error {response.status_code}"}
    
    def _http_request(self, method, url, **kwargs):
        """发送HTTP请求并记录详情"""
        start_time = time.time()
        logger.debug(f"[HTTP请求] {method} {url}")
        
        if "headers" not in kwargs:
            kwargs["headers"] = {}
        
        if "json" in kwargs:
            kwargs["headers"]["Content-Type"] = "application/json"
            
        try:
            response = requests.request(method, url, **kwargs)
            elapsed = time.time() - start_time
            logger.debug(f"[HTTP响应] {method} {url} - 状态码: {response.status_code}, 耗时: {elapsed:.3f}秒")
            return response
        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(f"[HTTP错误] {method} {url} - 错误: {str(e)}, 耗时: {elapsed:.3f}秒")
            raise

def run_auto_tests(client: McpToolClient):
    """运行自动化测试"""
    logger.info("开始自动化测试...")
    
    # 测试 get_server_time 工具
    logger.info("\n测试 get_server_time 工具...")
    result = client.call_tool("get_server_time", {})
    logger.info(f"结果: {result}")
    
    # 测试 calculate 工具
    logger.info("\n测试 calculate 工具...")
    result = client.call_tool("calculate", {"expression": "2 + 2 * 3"})
    logger.info(f"结果: {result}")
    
    # 测试 system_info 工具
    logger.info("\n测试 system_info 工具...")
    result = client.call_tool("system_info", {})
    logger.info(f"结果: {result}")
    
    # 测试 translate 工具
    logger.info("\n测试 translate 工具...")
    result = client.call_tool("translate", {
        "text": "Hello, world!",
        "target_language": "zh"
    })
    logger.info(f"结果: {result}")
    
    logger.info("\n自动化测试完成!")

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="MCP工具客户端测试")
    parser.add_argument("--url", default="http://localhost:8001", help="MCP服务器URL")
    parser.add_argument("--interactive", action="store_true", help="启用交互模式")
    args = parser.parse_args()
    
    # 创建客户端
    client = McpToolClient(args.url)
    
    try:
        # 连接到服务器
        if not client.connect():
            logger.error("无法连接到服务器，退出")
            return 1
            
        # 初始化会话
        if not client.initialize():
            logger.error("会话初始化失败，退出")
            return 1
            
        # 获取工具列表
        tools_info = client.list_tools()
        tools = tools_info.get("tools", [])
        
        if not tools:
            logger.warning("没有可用的工具")
            
        # 运行自动化测试
        run_auto_tests(client)
            
        return 0
    except KeyboardInterrupt:
        logger.info("用户中断，退出")
        return 0
    except Exception as e:
        logger.error(f"发生错误: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return 1

if __name__ == "__main__":
    sys.exit(main()) 