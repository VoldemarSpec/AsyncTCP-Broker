import asyncio
import json


async def send_command(writer, command):
    writer.write((json.dumps(command) + "\n").encode())
    await writer.drain()


async def run_test():
    print("=== STEP 1: Producer publishes task #999 ===")
    r_prod, w_prod = await asyncio.open_connection("127.0.0.1", 8888)
    await send_command(
        w_prod,
        {
            "type": "PUBLISH",
            "queue": "crash_test",
            "task_id": 999,
            "payload": "important_data.csv",
        },
    )
    print(f"[Producer] Broker response: {(await r_prod.readline()).decode().strip()}")
    w_prod.close()
    await w_prod.wait_closed()

    print("\n=== STEP 2: Worker 1 connects and consumes a task ===")
    r1, w1 = await asyncio.open_connection("127.0.0.1", 8888)
    await send_command(w1, {"type": "CONSUME", "queue": "crash_test"})

    task_for_worker_1 = await r1.readline()
    print(f"[Worker 1] Got task: {task_for_worker_1.decode().strip()}")

    print("[Worker 1] CRASH! Power lost! 💥 (Disconnecting without ACK)")
    w1.close()
    await w1.wait_closed()

    await asyncio.sleep(1)

    print("\n=== STEP 3: Worker 2 to the rescue ===")
    r2, w2 = await asyncio.open_connection("127.0.0.1", 8888)
    await send_command(w2, {"type": "CONSUME", "queue": "crash_test"})

    task_for_worker_2 = await r2.readline()
    print(f"[Worker 2] Got task: {task_for_worker_2.decode().strip()}")

    print("[Worker 2] Task completed! Sending ACK.")
    await send_command(w2, {"type": "ACK", "task_id": 999})
    print(f"[Worker 2] Broker response: {(await r2.readline()).decode().strip()}")

    w2.close()
    await w2.wait_closed()


if __name__ == "__main__":
    asyncio.run(run_test())
