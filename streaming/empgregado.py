import random

class Empregado:
    def __init__(self):
        self.employer_name = "TESTE EMPREGADO INGESTAO"
        self.reviews_count = self.__random(1000)
        self.culture_count = self.__random(1000)
        self.salaries_count = self.__random(1000)
        self.benefits_count = self.__random(1000)
        self.employer_website = "WEBSITE TESTE EMPREGADO INGESTAO"
        self.employer_headquarters = "HEADQUARTERS TESTE EMPREGADO INGESTAO"
        self.employer_founded = 2000
        self.employer_industry = "INDUSTRIA TESTE EMPREGADO INGESTAO"
        self.employer_revenue = "Desconhecida"
        self.url = "https://glassdor.com"
        self.cultura_e_valores = self.__random_float(1, limit=5)
        self.diversidade_e_inclusao = self.__random_float(1, limit=5)
        self.qualidade_de_vida = self.__random_float(1, limit=5)
        self.alta_lideranca = self.__random_float(1, limit=5)
        self.remuneracao_e_beneficios = self.__random_float(1, limit=5)
        self.oportunidade_de_carreira = self.__random_float(1, limit=5)
        self.geral = (self.cultura_e_valores + self.diversidade_e_inclusao + self.qualidade_de_vida + self.alta_lideranca + self.remuneracao_e_beneficios + self.oportunidade_de_carreira) / 6
        self.pct_recomendam_outras_pessoas = self.__random_float(1, 100)
        self.pct_perspectiva_positiva_da_empresa = self.__random_float(100)
        self.cnpj = "12345678910"
        self.segmento = "S4"
        self.nome = "BANCO_TESTE_INGESTAO"
        self.match_percent = self.__random(100)

    
    def __random(self, n):
        return random.randrange(n)
    
    def __random_float(self, n, m=10, limit=None):
        rand = round(random.random() * m, n)

        if limit is not None:
            while rand >= limit:
                rand = round(random.random() * m, n)

        return rand