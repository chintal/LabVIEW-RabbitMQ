
import ssl
import pika
import platform

rabbitmq_connection: pika.adapters.blocking_connection.BlockingChannel = None


def CreateConnection(host, port, virtual_host, username, password):
    global rabbitmq_connection
    if rabbitmq_connection:
        raise KeyError
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            port=port,
            virtual_host=virtual_host,
            host=host,
            credentials=pika.PlainCredentials(username=username, password=password),
            #ssl_options=pika.SSLOptions(context=ssl.SSLContext(), server_hostname=platform.node())
        ),
    )
    channel = connection.channel()
    rabbitmq_connection = channel


def OpenQueue(queue_name, exclusive):
    global rabbitmq_connection
    result = rabbitmq_connection.queue_declare(queue=queue_name, exclusive=exclusive)
    return result.method.queue


def OpenExchange(exchange_name):
    global rabbitmq_connection
    rabbitmq_connection.exchange_declare(exchange=exchange_name, exchange_type='topic', durable=True)
