import asyncio


class MessageQueue:
    def __init__(self):
        self.queues = asyncio.Queue()

    async def put(self, message):
        await self.queues.put(message)

    async def get(self):
        return await self.queues.get()
