
import ssl
import pika
import common
import queue
import platform
import threading

CreateConnection = common.CreateConnection
OpenQueue = common.OpenQueue


class BoundChannel(object):
    def __init__(self, host, port, virtual_host, username, password, exchange_name, routing_key, queue_name):
        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                port=port,
                virtual_host=virtual_host,
                host=host,
                credentials=pika.PlainCredentials(username=username, password=password),
                ssl_options=pika.SSLOptions(context=ssl.SSLContext(), server_hostname=platform.node())
            ),
        )
        self._channel = self._connection.channel()
        result = self._channel.queue_declare(queue=queue_name, exclusive=True)
        self._queue_name = result.method.queue
        self._channel.queue_bind(exchange=exchange_name,
                                 queue=queue_name,
                                 routing_key=routing_key)

    def consume(self, callback):
        self._channel.basic_consume(
            queue=self._queue_name,
            on_message_callback=callback,
            auto_ack=True)

        self._channel.start_consuming()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._connection.close()


class QueueReceiveBuffer(object):
    _buffer_size = 1000

    def __init__(self, host, port, virtual_host, username, password, exchange_name, routing_key, queue_name):
        self._host = host
        self._port = port
        self._virtual_host = virtual_host
        self._username = username
        self._password = password
        self._exchange_name = exchange_name
        self._routing_key = routing_key
        self._queue_name = queue_name
        self._messages = queue.Queue(maxsize=self._buffer_size)
        self._channel = None
        self._thread = None
        self._start()

    def _start(self):
        self._thread = threading.Thread(target=self._execute, daemon=True)
        self._thread.start()

    def _execute(self):
        with BoundChannel(self._host, self._port, self._virtual_host, self._username, self._password,
                          self._exchange_name, self._routing_key, self._queue_name) as channel:
            channel.consume(callback=self._handle_message)

    def _handle_message(self, ch, method, properties, body):
        if self.qsize() >= self._buffer_size:
            self._messages.get()
        self._messages.put(body.decode())

    def get(self):
        return self._messages.get(block=True, timeout=2)

    def qsize(self):
        return self._messages.qsize()


_buffer: QueueReceiveBuffer = None


def ReadOneFromBoundQueue():
    global _buffer
    return _buffer.get()


def GetExchangeBoundQueue(host, port, virtual_host, username, password, exchange_name, routing_key, queue_name):
    global _buffer
    _buffer = QueueReceiveBuffer(host, port, virtual_host, username, password,
                                 exchange_name, routing_key, queue_name)


if __name__ == '__main__':
    GetExchangeBoundQueue(port=5673, username='tmri4', password='tmri4', virtual_host='tmri4', host='rabbit.chintal.in',
                          exchange_name='i4.topic', routing_key='#', queue_name='')
    while True:
        print(ReadOneFromBoundQueue())