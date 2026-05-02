import asyncio
from src.nodes.base_node import Node


def test_node_start_stop():
    n = Node('test')
    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(n.start())
        assert n.is_running()
        loop.run_until_complete(n.stop())
        assert not n.is_running()
    finally:
        loop.close()
