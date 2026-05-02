import asyncio
import pytest

from src.queue.queue_node import QueueNode


@pytest.mark.asyncio
async def test_publish_consume_ack():
    qn = QueueNode('n1', base_url='http://localhost:8000')

    mid = await qn.publish('q1', {'text': 'hello'})
    assert mid is not None

    res = await qn.consume('q1', consumer='c1')
    assert res is not None
    mid2, msg = res
    assert mid2 == mid
    assert msg['text'] == 'hello'

    ok = await qn.ack('q1', consumer='c1', mid=mid)
    assert ok
