"""
@Author: obstacles
@Time:  2025-07-12 16:00
@Description: MCP 服务器工具集成辅助模块
"""
import inspect
import importlib
import pkgutil
import asyncio
import logging
from typing import List, Dict, Any, Type, Optional, Callable, Set

from puti.llm.tools import BaseTool, ToolArgs
from puti.logs import logger_factory

# 设置日志器
lgr = logger_factory.llm


class ToolRegistryError(Exception):
    """工具注册过程中的错误"""
    pass


class MCPToolRegistry:
    """MCP工具注册器
    
    用于管理和注册BaseTool到MCP服务器
    """
    
    def __init__(self):
        self.tools: Dict[str, Type[BaseTool]] = {}
        self.registered_tool_names: Set[str] = set()
        
    def register_tool_class(self, tool_cls: Type[BaseTool]) -> bool:
        """注册单个工具类
        
        Args:
            tool_cls: 要注册的工具类 (BaseTool的子类)
            
        Returns:
            bool: 是否成功注册
        """
        # 检查类型
        if not inspect.isclass(tool_cls) or not issubclass(tool_cls, BaseTool):
            lgr.warning(f"忽略 {tool_cls.__name__}: 不是BaseTool的子类")
            return False
            
        # 创建实例以获取名称
        try:
            tool_instance = tool_cls()
            tool_name = tool_instance.name
            
            if tool_name in self.tools:
                lgr.warning(f"工具 '{tool_name}' 已存在，将被替换")
                
            self.tools[tool_name] = tool_cls
            lgr.debug(f"注册工具类 '{tool_name}'")
            return True
        except Exception as e:
            lgr.error(f"实例化工具 {tool_cls.__name__} 失败: {str(e)}")
            return False
            
    def register_tool_classes(self, tool_classes: List[Type[BaseTool]]) -> int:
        """注册多个工具类
        
        Args:
            tool_classes: 要注册的工具类列表
            
        Returns:
            int: 成功注册的工具数量
        """
        success_count = 0
        for tool_cls in tool_classes:
            if self.register_tool_class(tool_cls):
                success_count += 1
        return success_count
        
    def discover_tools_from_module(self, module_path: str) -> int:
        """从指定模块发现并注册工具
        
        Args:
            module_path: 模块路径，例如 "puti.llm.tools.common"
            
        Returns:
            int: 发现并注册的工具数量
        """
        try:
            module = importlib.import_module(module_path)
            count = 0
            
            for name, obj in inspect.getmembers(module):
                if inspect.isclass(obj) and issubclass(obj, BaseTool) and obj is not BaseTool:
                    if self.register_tool_class(obj):
                        count += 1
                        
            return count
        except ImportError as e:
            lgr.error(f"导入模块 {module_path} 失败: {str(e)}")
            return 0
            
    def discover_all_tools(self) -> int:
        """发现并注册puti.llm.tools包中的所有工具
        
        Returns:
            int: 发现并注册的工具数量
        """
        from puti.llm import tools
        
        count = 0
        for _, module_name, _ in pkgutil.iter_modules(tools.__path__):
            if module_name == '__init__' or module_name == 'mcp_integration':
                continue
                
            module_path = f"puti.llm.tools.{module_name}"
            count += self.discover_tools_from_module(module_path)
            
        return count
        
    def register_to_mcp_server(self, server) -> Dict[str, Any]:
        """将所有注册的工具注册到MCP服务器
        
        Args:
            server: SimplifiedMCPServer实例
            
        Returns:
            Dict[str, Any]: 注册的工具信息
        """
        if not self.tools:
            lgr.warning("没有工具可注册到MCP服务器")
            return {"tools": [], "count": 0}
            
        registered_tools = []
        
        for tool_name, tool_cls in self.tools.items():
            if tool_name in self.registered_tool_names:
                lgr.debug(f"工具 '{tool_name}' 已注册到MCP服务器，跳过")
                continue
                
            try:
                # 实例化工具
                tool_instance = tool_cls()
                
                # 提取工具信息
                tool_schema = {
                    "name": tool_instance.name,
                    "description": tool_instance.desc
                }
                
                # 如果工具有参数定义，添加到schema
                if hasattr(tool_instance, 'param') and isinstance(tool_instance.param, dict):
                    if 'function' in tool_instance.param and 'parameters' in tool_instance.param['function']:
                        tool_schema["parameters"] = tool_instance.param['function']['parameters']
                
                # 创建工具处理函数
                async def tool_handler_factory(instance):
                    async def handler(**kwargs):
                        try:
                            result = await instance.run(**kwargs)
                            return result
                        except Exception as e:
                            lgr.error(f"工具执行错误: {str(e)}")
                            return {"error": str(e)}
                    return handler
                
                # 创建处理函数
                handler = asyncio.run(tool_handler_factory(tool_instance))
                
                # 注册工具到MCP服务器
                server.register_tool(
                    name=tool_instance.name,
                    handler=handler,
                    description=tool_instance.desc,
                    parameter_schema=tool_schema.get("parameters", {})
                )
                
                self.registered_tool_names.add(tool_name)
                registered_tools.append(tool_schema)
                lgr.info(f"工具 '{tool_name}' 已注册到MCP服务器")
            except Exception as e:
                lgr.error(f"注册工具 '{tool_name}' 失败: {str(e)}")
                
        return {
            "tools": registered_tools,
            "count": len(registered_tools)
        }


# 全局工具注册表实例
mcp_tools = MCPToolRegistry()


def register_tools_to_mcp(server, tool_classes: Optional[List[Type[BaseTool]]] = None) -> Dict[str, Any]:
    """注册工具到MCP服务器的便捷函数
    
    Args:
        server: SimplifiedMCPServer实例
        tool_classes: 要注册的工具类列表，如果为None则自动发现所有工具
        
    Returns:
        Dict[str, Any]: 注册的工具信息
    """
    if tool_classes:
        mcp_tools.register_tool_classes(tool_classes)
    else:
        mcp_tools.discover_all_tools()
        
    return mcp_tools.register_to_mcp_server(server)


async def create_mcp_tool_handler(func: Callable) -> Callable:
    """从普通函数创建MCP工具处理函数
    
    Args:
        func: 要包装的异步函数
        
    Returns:
        Callable: 生成的工具处理函数
    """
    if not asyncio.iscoroutinefunction(func):
        raise ValueError("func必须是异步函数(使用async def定义)")
    
    async def handler(**kwargs):
        try:
            result = await func(**kwargs)
            return {"result": result}
        except Exception as e:
            lgr.error(f"工具执行错误: {str(e)}")
            return {"error": str(e)}
    
    return handler


def create_mcp_tool(name: str, description: str, func: Callable) -> Type[BaseTool]:
    """从普通函数创建MCP工具
    
    Args:
        name: 工具名称
        description: 工具描述
        func: 要包装的异步函数
        
    Returns:
        Type[BaseTool]: 生成的工具类
    """
    if not asyncio.iscoroutinefunction(func):
        raise ValueError("func必须是异步函数(使用async def定义)")
        
    # 存储外部变量为局部变量以在类内部使用
    tool_name = name
    tool_description = description
    tool_func = func
    
    # 动态创建工具类
    class DynamicTool(BaseTool):
        name: str = tool_name
        desc: str = tool_description
        
        async def run(self, *args, **kwargs) -> Any:
            return await tool_func(*args, **kwargs)
            
    return DynamicTool


# 启动MCP服务器的便捷函数
def start_mcp_server(port: int = 8001, debug: bool = False, tool_classes: Optional[List[Type[BaseTool]]] = None):
    """启动MCP服务器并注册工具
    
    Args:
        port: 服务器端口
        debug: 是否启用调试模式
        tool_classes: 要注册的工具类列表，如果为None则自动发现所有工具
    """
    from puti.mcpp.simplified_mcp_server import server, main
    
    # 注册工具
    if tool_classes:
        register_tools_to_mcp(server, tool_classes)
    else:
        # 注册一些内置工具
        from puti.llm.tools.calculator import CalculatorTool
        from puti.llm.tools.common import EchoTool
        
        register_tools_to_mcp(server, [CalculatorTool, EchoTool])
    
    # 启动服务器
    main(port=port, debug=debug) 