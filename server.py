import asyncio
from broker import Broker, Protocol

broker = Broker()


async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    addr = writer.get_extra_info("peername")
    print(f"[+] Client connected: {addr}")

    try:
        while True:
            data = await reader.readline()
            if not data:
                break

            message = Protocol.decode(data)

            if message["type"] == "CONSUME":
                task = await broker.consume(message["queue"], writer)
                writer.write(Protocol.encode(task))
                await writer.drain()
                print(f"[-] Task {task.get('task_id')} dispatched to {addr}")

            elif message["type"] == "PUBLISH":
                await broker.publish(message)
                writer.write(Protocol.encode({"status": "OK"}))
                await writer.drain()

            elif message["type"] == "ACK":
                await broker.ack(message["task_id"])
                writer.write(Protocol.encode({"status": "COMPLETED"}))
                await writer.drain()

    except Exception as e:
        print(f"[!] Connection error with {addr}: {e}")

    finally:
        await broker.requeue(writer)
        print(f"[x] Connection closed: {addr}")
        writer.close()
        await writer.wait_closed()


async def main():
    server = await asyncio.start_server(handler, "127.0.0.1", 8888)
    addrs = ", ".join(str(sock.getsockname()) for sock in server.sockets)
    print(f"=== Message broker started on {addrs} ===")

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
