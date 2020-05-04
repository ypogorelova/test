import json
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer


class Client:
    def __init__(self, loop, topic_consumer):
        bootstrap_servers = 'kafka:9092'
        self.consumer = AIOKafkaConsumer(
            topic_consumer,
            loop=loop, bootstrap_servers=bootstrap_servers)

        self.producer = AIOKafkaProducer(
            loop=loop, bootstrap_servers=bootstrap_servers
        )
        self.topic_consumer = topic_consumer

    async def start(self):
        await self.consumer.start()
        await self.producer.start()

    async def on_message(self, msg, key):
        pass

    async def run_consumer(self):
        """
        Runs consumer and calls on_message coroutine on every consumed message
        :return: None
        """
        async for msg in self.consumer:
            await self.on_message(msg.value.decode(), msg.key)

    async def send_message(self, topic, msg, key=None):
        """
        Runs producer and send message
        :param topic: producer topic
        :param msg: msg to send
        :param key: message key
        :return: None
        """
        if key:
            key = key.encode("ascii")
        await self.producer.send(topic, json.dumps(msg).encode("ascii"), key)

    async def send_and_wait(self, topic, msg):
        """
        Send message via producer and receive corresponding message from consumer
        :param topic: producer topic
        :param msg: message to be send by producer
        :return: message from consumer
        """
        await self.send_message(topic=topic, msg=msg, key=self.topic_consumer)
        async for mes in self.consumer:
            return mes.value.decode()
