from .messages import MessageQueue


class Broker:
    def __init__(self):
        self.queues = {}
        self.acks = {}

    async def publish(self, message):
        queue_name = message["queue"]

        if queue_name not in self.queues:
            self.queues[queue_name] = MessageQueue()

        await self.queues[queue_name].put(message)

    async def consume(self, queue_name, worker):
        if queue_name not in self.queues:
            self.queues[queue_name] = MessageQueue()

        task = await self.queues[queue_name].get()

        self.acks[task["task_id"]] = {"task": task, "worker": worker}

        return task

    async def ack(self, task_id):
        if task_id in self.acks:
            del self.acks[task_id]

    async def requeue(self, worker):
        for ack in list(self.acks.values()):
            if ack["worker"] == worker:
                task = ack["task"]
                queue_name = task["queue"]

                await self.queues[queue_name].put(task)

                del self.acks[task["task_id"]]
