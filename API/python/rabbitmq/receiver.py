
import pika
import common

CreateConnection = common.CreateConnection
OpenQueue = common.OpenQueue


def ReadOneFromQueue(queue_name):
    method_frame, header_frame, body = common.rabbitmq_connection.basic_get(queue_name)
    if method_frame and isinstance(method_frame, pika.spec.Basic.GetOk):
        common.rabbitmq_connection.basic_ack(method_frame.delivery_tag)
        return body.decode()


def ReadFromQueue(queue_name, batch_size):
    available = 0
    messages = []
    method_frame, header_frame, body = common.rabbitmq_connection.basic_get(queue_name)
    if method_frame and isinstance(method_frame, pika.spec.Basic.GetOk):
        common.rabbitmq_connection.basic_ack(method_frame.delivery_tag)
        messages.append(body.decode())
        available = method_frame.message_count
        if available > batch_size - 1:
            available = batch_size - 1
    for i in range(available):
        messages.append(ReadOneFromQueue(queue_name))
    return messages


def BindQueueToExchange(exchange_name, queue_name, routing_key):
    common.rabbitmq_connection.queue_bind(exchange=exchange_name,
                                          queue=queue_name,
                                          routing_key=routing_key)


if __name__ == '__main__':
    CreateConnection(port=5672, username='tmri4', password='tmri4', virtual_host='tmri4', host='localhost')
    OpenQueue('test_python')
    data = ReadOneFromQueue('test_python')
    print(data)
    data = ReadFromQueue('test_python', 2)
    print(data)
