from rabbitmq_connector import RabbitMqConnector
from banco import Banco
from empgregado import Empregado
from reclamacao import Reclamacao
import json
import time

rabbit = RabbitMqConnector()

banco  = json.dumps(Banco().__dict__)
empregado  = json.dumps(Empregado().__dict__)
reclamacao  = json.dumps(Reclamacao().__dict__)


def send_requests(n):
    i = 0
    while i < n:
        if i % 3 == 0:
            rabbit.send("bancos", banco)
        elif i % 3 == 1:
            rabbit.send("empregados", empregado)
        elif i % 3 == 2:
            rabbit.send("reclamacoes", reclamacao)

        i = i + 1
        time.sleep(5)


if __name__ == "__main__":
    send_requests(6)