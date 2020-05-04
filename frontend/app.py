import asyncio
import uuid

from aiohttp.web import HTTPBadRequest
from aiohttp.web import Application, run_app
from aiohttp.web_routedef import RouteTableDef
from aiohttp.web import Response

from .kafka_utils import Client

routes = RouteTableDef()
loop = asyncio.get_event_loop()

topic = f'outbound-{uuid.uuid4()}'  # unique topic for every client
kafka_client = Client(loop, topic)


@routes.post('/urls')
async def post(request):
    data = await request.json()
    if not data.get('chars'):
        raise HTTPBadRequest()
    msg = data['chars']
    result = await kafka_client.send_and_wait(topic='indbound', msg=msg)
    return Response(status=201, body=result, content_type='application/json')

if __name__ == '__main__':
    loop.create_task(kafka_client.start())
    app = Application()
    app.add_routes(routes)
    run_app(app)
