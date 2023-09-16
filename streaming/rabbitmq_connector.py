import pika

class RabbitMqConnector:
    def __init__(self):
        credentials = pika.PlainCredentials("knhwqmwo", "LZFvwY28zMSnuAOx69jSxLo0pxogyma-")
        self.__parameters = pika.ConnectionParameters("jackal-01.rmq.cloudamqp.com", 5672, "knhwqmwo", credentials)
        self.__connection = None

    def __get_connection(self):
        if self.__connection is None:
            self.__connection = pika.BlockingConnection(self.__parameters)

        return self.__connection

    def send(self, queue, message):
        channel = self.__get_connection().channel()
        channel.queue_declare(queue=queue, durable=True)

        channel.basic_publish(exchange="", routing_key=queue, body=message)
        print(f"Sent message to {queue}. \nMessage: {message}\n\n")
        
        self.__get_connection().close()
        self.__connection = None

    def receive(self, callback):
        channel = self.__get_connection().channel()
        channel.queue_declare(queue="bancos", durable=True)
        channel.queue_declare(queue="reclamacoes", durable=True)
        channel.queue_declare(queue="empregados", durable=True)

        channel.basic_consume(queue="bancos", on_message_callback=callback, auto_ack=True)
        channel.basic_consume(queue="reclamacoes", on_message_callback=callback, auto_ack=True)
        channel.basic_consume(queue="empregados", on_message_callback=callback, auto_ack=True)

        print("Waiting for message in MQ. Ctrl+C to exit")
        channel.start_consuming()