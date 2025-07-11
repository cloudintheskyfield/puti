"""
@Author: obstacles
@Time:  2025-07-11 11:11
@Description:  Using anthropic mcp
"""
import click
import anyio
import uvicorn

from mcp.server import Server
from typing import Union, Optional, List, Dict
from mcp import types
from puti.constant.llm import RoleType
from mcp.server.sse import SseServerTransport
from starlette.applications import Starlette
from starlette.responses import Response
from starlette.routing import Mount, Route
from puti.constant.api import McpTransportMethod
from mcp.server.stdio import stdio_server


def create_messages(context: Optional[str], topic: Optional[str]) -> List[types.PromptMessage]:
    message = []
    if context:
        message.append(types.PromptMessage(
            role=RoleType.USER.val,
            content=types.TextContent(type='text', text=f'Here is some relevant context: {context}')
        ))
    prompt = 'Please help me with'
    if topic:
        prompt += f' {topic}'
    else:
        prompt += 'whatever questions I may have.'
    message.append(types.PromptMessage(role=RoleType.USER.val, context=types.TextContent(type='text', text=prompt)))
    return message


@click.command()
@click.option('--port', default=8000, help='Server port.')
@click.option('--transport', type=click.Choice(McpTransportMethod.to_list()), default='sse', help='Transport protocol.')
def main(port: int, transport: str) -> int:
    app = Server('mcp-server')

    @app.list_prompts()
    async def list_prompts() -> List[types.Prompt]:
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
        return types.GetPromptResult(
            messages=create_messages(context=arguments.get('context'), topic=arguments.get('topic')), description='A simple prompt'
        )

    if transport == McpTransportMethod.SSE.val:
        sse = SseServerTransport('/messages/')

        async def handle_sse(request):
            async with sse.connect_sse(request.scope, request.receive, request._send) as streams:
                await app.run(streams[0], streams[1], app.create_initialization_options())
            return Response()

        starlette_app = Starlette(debug=True, routes=[
                Route("/sse", endpoint=handle_sse),
                Mount("/messages/", app=sse.handle_post_message),
            ],
        )
        print(starlette_app.routes)
        uvicorn.run(starlette_app, host="0.0.0.0", port=port, log_level='info')
    else:
        async def arun():
            async with stdio_server() as streams:
                await app.run(streams[0], streams[1], app.create_initialization_options())

        anyio.run(arun)
    return 0


if __name__ == "__main__":
    main()

