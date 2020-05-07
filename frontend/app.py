import asyncio
import uuid

from aiohttp.web import HTTPBadRequest
from aiohttp.web import Application, run_app
from aiohttp.web_exceptions import HTTPRequestTimeout
from aiohttp.web_routedef import RouteTableDef
from aiohttp.web import Response

from .kafka_utils import Client

routes = RouteTableDef()
loop = asyncio.get_event_loop()
responses = {}


class KafkaClient(Client):
    async def on_message(self, msg, key):
        """
        Check if response is ready. Set result to future
        :param msg: message received from consumer
        :param key: unique key of message
        :return: None
        """
        fut = responses.get(key)
        if fut:
            fut.set_result(msg)

    async def run(self):
        """
        Start producer and consumer. Start consuming messages
        :return: None
        """
        await self.start()
        await self.run_consumer()


kafka_client = KafkaClient(loop=loop, topic_consumer='indbound', topic_producer='outbound')


@routes.post('/urls')
async def post(request):
    data = await request.json()
    if not data.get('chars'):
        raise HTTPBadRequest()
    msg = data['chars']
    key = str(uuid.uuid4())

    fut = loop.create_future()
    responses[key] = fut

    await kafka_client.send_message(msg=msg, key=key)
    try:
        result = await asyncio.wait_for(fut, 5)
    except asyncio.TimeoutError:
        fut.cancel()
        responses.pop(key, None)
        raise HTTPRequestTimeout()

    return Response(status=201, body=result, content_type='application/json')

if __name__ == '__main__':
    loop.create_task(kafka_client.run())
    app = Application()
    app.add_routes(routes)
    run_app(app)
