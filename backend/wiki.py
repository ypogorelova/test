import aiohttp
import asyncio
import json

from json import JSONDecodeError

from .db import get_record, insert_record, close_db, init_db
from .kafka_utils import Client

GET_URL = 'https://en.wikipedia.org/w/api.php?action=query&format=json&generator=prefixsearch&gpssearch={}'
SEARCH_URL = 'https://en.wikipedia.org/wiki/{}'


async def get(chars: str):
    """
    Send aiohttp get request
    :param url: url for get request
    :param chars: input chars for search result
    :return: Response
    """
    url = GET_URL.format(chars)
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            return Response(await response.text())


class Response:
    def __init__(self, response):
        self.response = response

    @property
    def response_json(self):
        """
        Convert response text into json
        :return: response in json format
        """
        try:
            return json.loads(self.response)
        except JSONDecodeError:
            return {}

    @property
    def search_results(self):
        """
        Formats result from wiki with titles and urls
        :return: dict
        """
        if self.response_json and self.response_json.get('query'):
            return {
                'search_result': [
                    {
                        'title': i['title'], 'url': SEARCH_URL.format(i['title'])
                    } for i in self.response_json['query']['pages'].values()
                ]
            }


class KafkaClient(Client):
    async def on_message(self, msg, key):
        cached = await get_record(msg)
        if cached:
            rec = cached
        else:
            response = await get(msg)
            rec = response.search_results
            await insert_record(chars=msg, urls=response.search_results)

        await self.send_message(topic=key.decode("ascii"), msg=rec)


async def main(loop):
    await init_db()
    kafka = KafkaClient(loop, 'indbound')
    await kafka.start()
    try:
        await kafka.run_consumer()
    finally:
        await kafka.shutdown()
    await close_db()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))
