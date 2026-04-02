"""
@Author: obstacles
@Time:  2025-07-11 11:11
@Description:  MCP Server supporting both stdio and SSE transport methods
"""
import click
import anyio
import uvicorn
import importlib
import pkgutil
import inspect
import json
import os
import datetime
import logging
import time
import traceback
import sys
import uuid
from collections import defaultdict
import asyncio

from mcp.server import Server
from mcp.server.stdio import stdio_server
from typing import Union, Optional, List, Dict, Type, Any, Callable
from mcp import types
from puti.constant.llm import RoleType
from mcp.server.sse import SseServerTransport
from starlette.applications import Starlette
from starlette.responses import Response, JSONResponse
from starlette.routing import Mount, Route
from starlette.requests import Request
from puti.constant.api import McpTransportMethod
from puti.llm.tools import BaseTool
from puti.logs import logger_factory, LoggerFactory
from puti.llm.tools.project_analyzer import ProjectAnalyzer
from puti.llm.tools.web_search import WebSearch
from puti.llm.nodes import OpenAINode

# 设置日志器
lgr = logger_factory.llm

# 会话状态存储
session_states = {}

# 工具类定义
class GetServerTime(BaseTool):
    name: str = "get_server_time"
    desc: str = "Get the current server time"
    
    async def run(self, *args, **kwargs) -> Dict[str, str]:
        format = kwargs.get("format", "iso")
        now = datetime.datetime.now()
        if format == "iso":
            time_str = now.isoformat()
        elif format == "human":
            time_str = now.strftime("%Y-%m-%d %H:%M:%S")
        else:
            time_str = str(now)
        
        return {"time": time_str, "format": format}

class Calculator(BaseTool):
    name: str = "calculator"
    desc: str = "Perform basic arithmetic operations"
    
    async def run(self, *args, **kwargs) -> Dict[str, Any]:
        operation = kwargs.get("operation")
        a = kwargs.get("a")
        b = kwargs.get("b")

        if operation is None or a is None or b is None:
            return {"error": "Missing required arguments. 'operation', 'a', and 'b' are required."}

        result = None
        if operation == "add":
            result = a + b
        elif operation == "subtract":
            result = a - b
        elif operation == "multiply":
            result = a * b
        elif operation == "divide":
            if b == 0:
                return {"error": "Division by zero"}
            result = a / b
        else:
            return {"error": f"Unknown operation: {operation}"}
        
        return {"result": result, "operation": operation, "a": a, "b": b}

# 辅助函数
async def get_system_info(**kwargs) -> Dict[str, Any]:
    """获取系统信息"""
    import platform
    
    return {
        "os": platform.system(),
        "os_version": platform.version(),
        "python_version": platform.python_version(),
        "hostname": platform.node()
    }

async def translate_text(text: str, target_language: str, **kwargs) -> Dict[str, str]:
    """模拟翻译功能"""
    return {
        "original_text": text,
        "translated_text": f"[Translated to {target_language}]: {text}",
        "target_language": target_language
    }

def create_messages(context: Optional[str] = None, topic: Optional[str] = None) -> List[types.PromptMessage]:
    """创建消息列表"""
    messages = [
        types.PromptMessage(
            role="system",
            content=[types.TextContent(text="You are a helpful assistant.")]
        )
    ]
    
    if context:
        messages.append(types.PromptMessage(
            role="system",
            content=[types.TextContent(text=f"Context: {context}")]
        ))
    
    if topic:
        messages.append(types.PromptMessage(
            role="user",
            content=[types.TextContent(text=f"Let's talk about {topic}.")]
        ))
    
    return messages

# API请求/响应文档
API_DOCUMENTATION = """
# MCP服务器API文档

## 传输方式

本服务器支持两种传输方式：
1. SSE (Server-Sent Events) - 默认方式，支持实时通信
2. STDIO - 标准输入/输出方式，适用于命令行工具

## SSE模式下的端点

1. **SSE连接端点**: `/sse`
   - 方法: GET
   - 功能: 建立SSE连接，获取会话ID和事件流
   - 返回: 
     - 会话ID事件 (event: session_created)
     - 各种通知事件
     - 工具执行结果事件

2. **消息处理端点**: `/messages/?session_id={session_id}`
   - 方法: POST
   - 功能: 处理JSON-RPC请求
   - 需要参数: session_id (URL参数)
   - 请求体: JSON-RPC 2.0格式请求
   - 响应: JSON-RPC 2.0格式响应

3. **工具列表端点**: `/tools?session_id={session_id}`
   - 方法: GET
   - 功能: 获取可用工具列表
   - 需要参数: session_id (URL参数)
   - 响应: JSON格式的工具列表

## JSON-RPC请求类型

1. **初始化**: 
   ```json
   {
     "jsonrpc": "2.0",
     "method": "initialize",
     "params": {},
     "id": "1"
   }
   ```
   - 功能: 初始化会话
   - 返回: 服务器能力和版本信息

2. **工具列表**: 
   ```json
   {
     "jsonrpc": "2.0",
     "method": "tools/list",
     "params": {},
     "id": "2"
   }
   ```
   - 功能: 获取可用工具列表
   - 返回: 工具列表和描述

3. **工具调用**: 
   ```json
   {
     "jsonrpc": "2.0",
     "method": "tools/call",
     "params": {
       "name": "工具名称",
       "参数1": "值1",
       "参数2": "值2"
     },
     "id": "3"
   }
   ```
   - 功能: 调用指定工具
   - 返回: 工具执行结果 (异步方式通过SSE返回)

4. **提示列表**: 
   ```json
   {
     "jsonrpc": "2.0",
     "method": "prompts/list",
     "params": {},
     "id": "4"
   }
   ```
   - 功能: 获取可用提示列表
   - 返回: 提示列表和描述

5. **获取提示**: 
   ```json
   {
     "jsonrpc": "2.0",
     "method": "prompts/get",
     "params": {
       "name": "提示名称",
       "arguments": {
         "参数1": "值1",
         "参数2": "值2"
       }
     },
     "id": "5"
   }
   ```
   - 功能: 获取指定提示的内容
   - 返回: 提示内容和描述
"""

@click.command()
@click.option('--port', default=8000, help='Server port.')
@click.option('--transport', type=click.Choice(McpTransportMethod.to_list()), default='sse', help='Transport protocol.')
@click.option('--debug', is_flag=True, help='Enable debug mode with more verbose logging.')
def main(port: int, transport: str, debug: bool) -> int:
    """
    启动MCP服务器
    
    支持两种传输模式：
    - sse: 使用Server-Sent Events进行通信（默认）
    - stdio: 使用标准输入/输出进行通信
    """
    # 清空会话状态
    session_states.clear()
    lgr.info("[初始化] 清空会话状态")
    
    if debug:
        LoggerFactory._define_loggers(print_level='DEBUG')
        lgr.info("[配置] 启用调试模式")
    else:
        LoggerFactory._define_loggers(print_level='INFO')
        
    lgr.info(f"[启动] 启动MCP服务器 transport={transport}, port={port}")
    lgr.info(f"[时间] 当前时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    app = Server('mcp-server')
    llm_node = OpenAINode()
    project_analyzer_tool = ProjectAnalyzer()
    web_search_tool = WebSearch()

    @app.handler("tools/project_structure_analyzer")
    async def project_structure_analyzer(path: Optional[str] = None, max_depth: int = 3, **kwargs) -> dict:
        lgr.info(f"Executing tool: project_structure_analyzer with path='{path}' and max_depth={max_depth}")
        response = await project_analyzer_tool.run(path=path, max_depth=max_depth, **kwargs)
        return response.to_dict()

    @app.handler("tools/web_search")
    async def web_search(query: str, num_results: int = 3, **kwargs) -> dict:
        lgr.info(f"Executing tool: web_search with query='{query}' and num_results={num_results}")
        response = await web_search_tool.run(llm=llm_node, query=query, num_results=num_results, **kwargs)
        return response.to_dict()

    @app.list_prompts()
    async def list_prompts() -> List[types.Prompt]:
        lgr.info("[MCP请求] 接收到list_prompts请求")
        return [
            types.Prompt(
                name='simple',
                title='Simple Assistant Prompt',
                description='A simple prompt that can take optional context and topic parameters.',
                arguments=[
                    types.PromptArgument(
                        name='context',
                        description='Optional context for the assistant.',
                        required=False
                    ),
                    types.PromptArgument(
                        name='topic',
                        description='Specific topic to focus on',
                        required=False
                    )
                ]
            )
        ]

    @app.get_prompt()
    async def get_prompt(name: str, arguments: Optional[Dict[str, str]]) -> types.GetPromptResult:
        lgr.info(f"[MCP请求] 接收到get_prompt请求: name={name}")
        return types.GetPromptResult(
            messages=create_messages(context=arguments.get('context'), topic=arguments.get('topic')),
            description='A simple prompt'
        )
    
    if transport == McpTransportMethod.SSE.val:
        sse = SseServerTransport('/messages/')

        async def handle_sse(request: Request):
            """
            处理SSE连接请求
            
            建立SSE连接，生成会话ID，并设置事件流
            """
            client_host = request.client.host if request.client else "未知"
            client_info = f"{client_host}:{request.client.port}" if request.client else "未知"
            lgr.info(f"[SSE连接] 新的SSE连接请求: {client_info}")
            lgr.debug(f"[SSE请求头] {dict(request.headers)}")
            
            # 生成会话ID
            session_uuid = str(uuid.uuid4())
            lgr.info(f"[SSE连接] 为客户端 {client_info} 创建会话ID: {session_uuid}")
            
            # 预先初始化会话状态
            session_states[session_uuid] = {
                "initialized": False, 
                "connection_time": time.time(),
                "client_info": client_info
            }
            
            start_time = time.time()
            try:
                async with sse.connect_sse(request.scope, request.receive, request._send) as streams:
                    lgr.info(f"[SSE连接] 成功建立SSE连接: {client_info}, 会话ID: {session_uuid}")
                    
                    # 发送会话ID作为SSE事件
                    await streams[1].send({
                        "type": "event",
                        "data": {
                            "event": "session_created",
                            "data": json.dumps({
                                "session_id": session_uuid,
                                "message_endpoint": f"/messages/?session_id={session_uuid}",
                                "tools_endpoint": f"/tools?session_id={session_uuid}",
                                "documentation": "使用上述endpoints进行交互，所有请求都需要提供session_id参数"
                            })
                        }
                    })
                    
                    # 发送API文档事件
                    await streams[1].send({
                        "type": "event",
                        "data": {
                            "event": "api_documentation",
                            "data": json.dumps({
                                "documentation": API_DOCUMENTATION
                            })
                        }
                    })
                    
                    # 运行MCP服务器
                    await app.run(streams[0], streams[1], app.create_initialization_options())
                    
                    elapsed = time.time() - start_time
                    lgr.info(f"[SSE连接] SSE连接关闭: {client_info}，持续时间: {elapsed:.1f}秒")
                return Response()
            except Exception as e:
                elapsed = time.time() - start_time
                lgr.error(f"[SSE错误] SSE连接处理出错: {client_info}, 错误: {str(e)}，持续时间: {elapsed:.1f}秒")
                lgr.error(f"[SSE错误堆栈] {traceback.format_exc()}")
                return Response(status_code=500)
        
        # 拦截并处理POST消息请求
        async def handle_post_message(request):
            """
            处理JSON-RPC消息请求
            
            验证会话ID，解析请求，处理各种方法调用
            """
            client_host = request.client.host if request.client else "未知"
            client_info = f"{client_host}:{request.client.port}" if request.client else "未知"
            lgr.info(f"[HTTP请求] POST /messages/ - 客户端: {client_info}")
            
            # 解析会话ID
            session_id = request.query_params.get('session_id')
            if not session_id:
                lgr.error(f"[HTTP错误] 缺少session_id参数")
                return JSONResponse(
                    status_code=400,
                    content={"jsonrpc": "2.0", "error": {"code": -32602, "message": "Missing session_id parameter"}, "id": None}
                )
            
            # 验证会话ID
            if session_id not in session_states:
                lgr.error(f"[验证失败] 无效的会话ID: {session_id}")
                return JSONResponse(
                    status_code=401,
                    content={"jsonrpc": "2.0", "error": {"code": -32600, "message": "Invalid session ID"}, "id": None}
                )
            
            session_uuid = session_id
            session_states[session_uuid]['last_activity'] = time.time()
            
            # 读取请求体
            try:
                body = await request.body()
                data = json.loads(body)
                request_id = data.get('id', 'unknown')
                lgr.debug(f"接收到JSON-RPC请求 [ID: {request_id}]: {body.decode('utf-8')}")
                
                # 检查是否是有效的JSON-RPC请求
                if not isinstance(data, dict) or 'jsonrpc' not in data or data.get('jsonrpc') != '2.0' or 'method' not in data:
                    lgr.error(f"[JSON-RPC错误] 无效的JSON-RPC请求格式")
                    return JSONResponse(
                        status_code=400,
                        content={"jsonrpc": "2.0", "error": {"code": -32600, "message": "Invalid Request"}, "id": request_id}
                    )
                
                # 检查是否是初始化请求
                if data.get('method') == 'initialize':
                    lgr.info(f"[初始化] 收到初始化请求 [ID: {request_id}]: 会话ID={session_uuid}")
                    # 标记会话为已初始化
                    session_states[session_uuid]['initialized'] = True
                    session_states[session_uuid]['last_activity'] = time.time()
                    lgr.info(f"[初始化] 会话 {session_uuid} 已初始化")
                    
                    # 对于初始化请求，我们可以直接返回成功响应
                    # 这样可以确保客户端知道初始化已完成
                    return JSONResponse(
                        content={
                            "jsonrpc": "2.0",
                            "result": {
                                "capabilities": {
                                    "protocolVersion": "0.1",
                                    "serverInfo": {
                                        "name": "puti-mcp-server",
                                        "version": "1.0.0"
                                    }
                                }
                            },
                            "id": request_id
                        }
                    )
                elif data.get('method') == 'tools/list':
                    # 工具列表请求
                    if not session_states[session_uuid].get('initialized', False):
                        lgr.warning(f"[验证失败] 会话 {session_uuid} 尚未初始化，但尝试获取工具列表 [ID: {request_id}]")
                        return JSONResponse(
                            status_code=400,
                            content={"jsonrpc": "2.0", "error": {"code": -32600, "message": "Session not initialized"}, "id": request_id}
                        )
                    
                    lgr.info(f"[工具列表] 收到工具列表请求 [ID: {request_id}]: 会话ID={session_uuid}")
                    session_states[session_uuid]['last_activity'] = time.time()
                elif data.get('method') == 'tools/call':
                    # 工具调用请求
                    if not session_states[session_uuid].get('initialized', False):
                        lgr.warning(f"[验证失败] 会话 {session_uuid} 尚未初始化，但尝试调用工具 [ID: {request_id}]")
                        return JSONResponse(
                            status_code=400,
                            content={"jsonrpc": "2.0", "error": {"code": -32600, "message": "Session not initialized"}, "id": request_id}
                        )
                    
                    tool_name = data.get('params', {}).get('name', '未知')
                    lgr.info(f"[工具调用] 收到工具调用请求 [ID: {request_id}]: 工具={tool_name}, 会话ID={session_uuid}")
                    session_states[session_uuid]['last_activity'] = time.time()
                else:
                    # 其他请求
                    if not session_states[session_uuid].get('initialized', False):
                        lgr.warning(f"[验证失败] 会话 {session_uuid} 尚未初始化，但尝试发送请求 [ID: {request_id}]: method={data.get('method')}")
                        return JSONResponse(
                            status_code=400,
                            content={"jsonrpc": "2.0", "error": {"code": -32600, "message": "Session not initialized"}, "id": request_id}
                        )
                    
                    lgr.info(f"[请求] 收到请求 [ID: {request_id}]: method={data.get('method')}, 会话ID={session_uuid}")
                    session_states[session_uuid]['last_activity'] = time.time()
            except json.JSONDecodeError:
                lgr.error(f"[HTTP错误] 无效的JSON格式")
                return JSONResponse(
                    status_code=400,
                    content={"jsonrpc": "2.0", "error": {"code": -32700, "message": "Parse error"}, "id": None}
                )
            except Exception as e:
                lgr.error(f"[HTTP错误] 处理请求时出错: {str(e)}")
                lgr.error(f"[HTTP错误堆栈] {traceback.format_exc()}")
                return JSONResponse(
                    status_code=500,
                    content={"jsonrpc": "2.0", "error": {"code": -32603, "message": f"Internal error: {str(e)}"}, "id": None}
                )
            
            # 继续处理请求（默认情况）
            lgr.info(f"[请求] 处理请求 [ID: {request_id}]: 会话ID={session_uuid}")
            return await sse.handle_post_message(request)
        
        # 返回可用工具的HTTP端点
        async def get_available_tools(request: Request):
            """
            获取可用工具列表的API端点
            
            验证会话ID，并返回所有可用工具的列表
            """
            client_host = request.client.host if request.client else "未知"
            client_info = f"{client_host}:{request.client.port}" if request.client else "未知"
            lgr.info(f"[工具列表] 接收到工具列表请求: {client_info}")
            
            # 解析会话ID
            session_id = request.query_params.get('session_id')
            if not session_id:
                lgr.error(f"[HTTP错误] 缺少session_id参数")
                return JSONResponse(
                    status_code=400,
                    content={"error": "Missing session_id parameter"}
                )
            
            # 验证会话ID
            if session_id not in session_states:
                lgr.error(f"[验证失败] 无效的会话ID: {session_id}")
                return JSONResponse(
                    status_code=401,
                    content={"error": "Invalid session ID"}
                )
            
            # 检查会话是否初始化
            if not session_states[session_id].get('initialized', False):
                lgr.warning(f"[验证失败] 会话 {session_id} 尚未初始化，但尝试获取工具列表")
                return JSONResponse(
                    status_code=400,
                    content={"error": "Session not initialized"}
                )
            
            session_states[session_id]['last_activity'] = time.time()
            
            response_data = {
                "tools": [{"name": "project_structure_analyzer", "description": "Analyze the project structure of a given path."},
                          {"name": "web_search", "description": "Perform a web search for a given query."}],
                "count": 2,
                "server_time": datetime.datetime.now().isoformat()
            }
            
            lgr.info(f"[工具列表] 返回 {response_data['count']} 个工具")
            return JSONResponse(response_data)

        # 定义路由
        routes = [
            Route("/sse", endpoint=handle_sse, methods=["GET"]),
            Route("/tools", endpoint=get_available_tools, methods=["GET"]),
            Route("/messages/", endpoint=handle_post_message, methods=["POST"]),
        ]
        
        # 尝试导入中间件模块
        try:
            from starlette.middleware import Middleware
            from starlette.middleware.base import BaseHTTPMiddleware
            
            # 添加日志中间件
            async def logging_middleware(request: Request, call_next):
                """记录HTTP请求和响应的中间件"""
                start_time = time.time()
                client_host = request.client.host if request.client else "未知"
                client_info = f"{client_host}:{request.client.port}" if request.client else "未知"
                
                lgr.info(f"[HTTP请求] {request.method} {request.url.path} - 客户端: {client_info}")
                
                try:
                    response = await call_next(request)
                    elapsed = time.time() - start_time
                    lgr.info(f"[HTTP响应] {request.method} {request.url.path} - 状态码: {response.status_code}, 耗时: {elapsed:.3f}秒")
                    return response
                except Exception as e:
                    elapsed = time.time() - start_time
                    lgr.error(f"[HTTP错误] {request.method} {request.url.path} - 错误: {str(e)}, 耗时: {elapsed:.3f}秒")
                    lgr.error(f"[HTTP错误堆栈] {traceback.format_exc()}")
                    return JSONResponse(
                        status_code=500,
                        content={"error": "Internal Server Error"}
                    )
                    
            middleware = [
                Middleware(BaseHTTPMiddleware, dispatch=logging_middleware)
            ]
            
            starlette_app = Starlette(
                debug=debug,
                routes=routes,
                middleware=middleware
            )
        except ImportError:
            lgr.warning("[配置警告] 无法导入Starlette中间件，将不使用日志中间件")
            starlette_app = Starlette(
                debug=debug,
                routes=routes
            )
        
        lgr.info(f"[启动] MCP服务器已启动，提供 {response_data['count']} 个工具")
        lgr.info(f"[端点] SSE端点: http://127.0.0.1:{port}/sse")
        lgr.info(f"[端点] 工具列表端点: http://127.0.0.1:{port}/tools")
        lgr.info(f"[端点] 消息端点: http://127.0.0.1:{port}/messages/")
        
        # 输出API文档信息
        lgr.info("[文档] API接口文档:")
        for line in API_DOCUMENTATION.split('\n'):
            if line.strip() and not line.startswith('#'):
                lgr.info(f"[文档] {line}")
        
        # 使用指定的参数启动服务器
        try:
            config = uvicorn.Config(
                app=starlette_app, 
                host="0.0.0.0", 
                port=port,
                log_level="debug" if debug else "info"
            )
            server = uvicorn.Server(config)
            lgr.info(f"[启动] 使用SSE传输模式启动MCP服务器，监听地址: 0.0.0.0:{port}")
            server.run()
        except Exception as e:
            lgr.error(f"[启动错误] 服务器启动失败: {str(e)}")
            lgr.error(traceback.format_exc())
    else:
        # STDIO模式
        lgr.info(f"[启动] 使用STDIO传输模式启动MCP服务器")
        lgr.info("[文档] STDIO模式使用标准输入/输出与MCP服务器交互")
        lgr.info("[文档] 支持的JSON-RPC请求:")
        lgr.info(f"  - initialize: 初始化会话")
        lgr.info(f"  - tools/list: 列出可用工具")
        lgr.info(f"  - tools/call: 调用工具")
        lgr.info(f"  - prompts/list: 列出可用提示")
        lgr.info(f"  - prompts/get: 获取提示内容")
        
        # 定义运行函数
        async def arun():
            """运行STDIO模式的MCP服务器"""
            lgr.info("[STDIO] 等待STDIO连接...")
            async with stdio_server() as streams:
                lgr.info("[STDIO] STDIO连接已建立")
                lgr.info("[STDIO] 可以开始发送JSON-RPC请求")
                await app.run(streams[0], streams[1], app.create_initialization_options())
                lgr.info("[STDIO] STDIO连接已关闭")

        anyio.run(arun)
    
    lgr.info("[关闭] MCP服务器已关闭")
    return 0


if __name__ == "__main__":
    try:
        lgr.info("[启动] 开始启动MCP服务器")
        main()
    except Exception as e:
        lgr.error(f"服务器启动失败: {str(e)}")
        lgr.error(traceback.format_exc())
        sys.exit(1)
