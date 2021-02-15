
import ssl
import pika
import platform

_connection = None


def CreateConnection(host, port, virtual_host, username, password):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            port=port,
            virtual_host=virtual_host,
            host=host,
            credentials=pika.PlainCredentials(username=username, password=password),
            ssl_options=pika.SSLOptions(context=ssl.SSLContext(), server_hostname=platform.node())
        ),
    )
    channel = connection.channel()
    global _connection
    if _connection:
        raise KeyError
    _connection = channel


def OpenQueue(queue_name):
    global _connection
    _connection.queue_declare(queue=queue_name)


def SendOnQueue(queue_name, message):
    global _connection
    _connection.basic_publish(
        exchange='', routing_key=queue_name, body=message)


if __name__ == '__main__':
    CreateConnection(port=5673, username='tmri4', password='tmri4', virtual_host='tmri4', host='localhost')
    OpenQueue('test_python')
    SendOnQueue('test_python', 'Python Test Message')
