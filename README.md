# AsyncTCP-Broker

A small asynchronous message broker built with Python, `asyncio`, and raw TCP connections.
The project demonstrates a basic task queue model: a producer publishes a task, a worker
consumes it, and then acknowledges its completion. If a worker disconnects before sending
an `ACK`, the task is returned to the queue and can be delivered to another worker.

The project has no external dependencies and is primarily intended for learning about
asynchronous TCP, queues, and message redelivery.

## Features

- multiple named queues stored in the process memory;
- a JSON Lines protocol over TCP: one JSON object per line;
- task publishing, consumption, and acknowledgement;
- redelivery of unacknowledged tasks after a worker connection closes;
- a minimal public Python API exposed through the `broker` package.

## Requirements

- Python 3.8+;
- the Python standard library.

No package installation is required.

## Quick Start

Open two terminals in the project root.

Start the broker in the first terminal:

```powershell
python server.py
```

В Windows также можно использовать:

```powershell
py server.py
```

By default, the server listens on `127.0.0.1:8888`.

Run the demonstration scenario in the second terminal:

```powershell
python test.py
```

The scenario performs the following steps:

1. publishes task `task_id=999` to the `crash_test` queue;
2. delivers it to the first worker;
3. disconnects the worker without an acknowledgement;
4. delivers the same task to a second worker;
5. acknowledges the task with `ACK`.

## Protocol

Each command is sent as a UTF-8 JSON object terminated by a newline character
(`\n`). Broker responses are also JSON objects terminated by a newline.

### Publishing a Task

The client sends:

```json
{ "type": "PUBLISH", "queue": "jobs", "task_id": 1, "payload": "report.csv" }
```

The broker responds:

```json
{ "status": "OK" }
```

The `payload` field is not interpreted by the broker and may contain any JSON value.
The `queue` field identifies the queue, while `task_id` is used to acknowledge the task.

### Consuming a Task

The client sends:

```json
{ "type": "CONSUME", "queue": "jobs" }
```

If the queue contains a task, the broker sends back the original task object:

```json
{ "type": "PUBLISH", "queue": "jobs", "task_id": 1, "payload": "report.csv" }
```

If the queue is empty, the `CONSUME` request waits until the next task becomes available.

### Acknowledging a Task

After completing the task, the worker sends:

```json
{ "type": "ACK", "task_id": 1 }
```

The response is:

```json
{ "status": "COMPLETED" }
```

`ACK` removes the task from the list of unacknowledged tasks. If the worker connection
closes before acknowledgement, the broker puts the task associated with that connection
back into its queue.

## Project Structure

```text
server.py          TCP server and command handler
test.py            demonstration integration scenario
broker/
	broker.py        queues, delivery, ACK, and redelivery
	messages.py      asyncio.Queue wrapper
	protocol.py      JSON encoding and decoding
	__init__.py      exports Broker and Protocol
```

## Python API

```python
from broker import Broker, Protocol

broker = Broker()
await broker.publish(message)
task = await broker.consume("jobs", worker)
await broker.ack(task["task_id"])
await broker.requeue(worker)
```

`Protocol.encode(data)` serializes an object as JSON Lines, while `Protocol.decode(data)`
decodes received bytes back into a Python object.

## Limitations and Important Notes

This is an educational, local implementation rather than a production-ready broker:

- queues and unacknowledged tasks are stored only in memory;
- all tasks are lost when the process restarts;
- the server is bound to `127.0.0.1`, with no TLS or authentication;
- message and queue sizes are not limited;
- there are no visibility or lease timeouts for stalled workers;
- `ACK` does not verify that the task belongs to the worker sending it;
- an unknown `task_id` currently still receives a `COMPLETED` response;
- invalid JSON, unknown commands, or missing fields have no dedicated error format
  and may cause the connection to close;
- `task_id` values must be unique because they are used as unacknowledged-task keys;
- `test.py` is a manual demonstration and contains no automated assertions.

## Possible Improvements

- add command validation and a consistent error format;
- verify which worker sends an `ACK`;
- generate or validate unique task identifiers;
- add a visibility timeout and redelivery for stalled workers;
- implement persistent storage or integrate with an external database;
- configure host and port through command-line arguments or environment variables;
- add TLS and authentication for remote clients;
- replace the demonstration scenario with automated tests;
- add graceful shutdown and queue inspection commands.

## License

See [LICENSE](LICENSE).
