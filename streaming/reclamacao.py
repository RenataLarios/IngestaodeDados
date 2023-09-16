import random

class Reclamacao:
    def __init__(self):
        self.ano = 2023
        self.trimestre = 4
        self.instituicao_financeira = "BANCO_TESTE_INGESTAO"
        self.categoria = "S4"
        self.tipo = "Banco/financeira"
        self.cnpj_if = 12345678910
        self.indice= self.__random_float(2, 100)
        self.qtd_reclamacoes_reguladas_procedentes = self.__random(500)
        self.qtd_reclamacoes_reguladas_outras = self.__random(500)
        self.qtd_reclamacoes_nao_reguladas = self.__random(500)
        self.qtd_total_reclamacoes = (self.qtd_reclamacoes_reguladas_procedentes + self.qtd_reclamacoes_reguladas_outras + self.qtd_reclamacoes_nao_reguladas)
        self.qtd_total_clientes_ccs = self.__random(10000)
        self.qtd_total_clientes_scr = self.__random(10000)
        self.qtd_total_clientes = self.qtd_total_clientes_ccs + self.qtd_total_clientes_scr

        
    def __random(self, n):
        return random.randrange(n)
    
    def __random_float(self, n, m=10):
        return round(random.random() * m, n)