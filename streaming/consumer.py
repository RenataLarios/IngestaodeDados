from rabbitmq_connector import RabbitMqConnector
from mysql_connector import MySQLConnector
import json

connector = MySQLConnector()

def banco_stmt(body):
    params = {
        "segmento": body.get("segmento"),
        "cnpj": body.get("cnpj"),
        "nome": body.get("nome")
    }
    
    query = "INSERT INTO tb_bancos (segmento, cnpj, nome) VALUES (%(segmento)s, %(cnpj)s, %(nome)s)"

    return query, params

def reclamacoes_stmt(body):
    params = {
        "ano": body.get("ano"),
        "trimestre": body.get("trimestre"),
        "categoria": body.get("categoria"),
        "tipo": body.get("tipo"),
        "cnpj_if": body.get("cnpj_if"),
        "instituicao_financeira": body.get("instituicao_financeira"),
        "indice": body.get("indice"),
        "qtd_reclamacoes_reguladas_procedentes": body.get("qtd_reclamacoes_reguladas_procedentes"),
        "qtd_reclamacoes_reguladas_outras": body.get("qtd_reclamacoes_reguladas_outras"),
        "qtd_reclamacoes_nao_reguladas": body.get("qtd_reclamacoes_nao_reguladas"),
        "qtd_total_reclamacoes": body.get("qtd_total_reclamacoes"),
        "qtd_total_clientes_ccs": body.get("qtd_total_clientes_ccs"),
        "qtd_total_clientes_scr": body.get("qtd_total_clientes_scr"),
        "qtd_total_clientes": body.get("qtd_total_clientes")
    }
    query = "INSERT INTO tb_reclamacoes(`Ano`, `Trimestre`, `Categoria`, `Tipo`, `CNPJ IF`, `Instituição financeira`, `Índice`, `Quantidade de reclamações reguladas procedentes`, `Quantidade de reclamações reguladas - outras`, `Quantidade de reclamações n�o reguladas`, \
            `Quantidade total de reclamações`, `Quantidade total de clientes � CCS e SCR`, `Quantidade de clientes � CCS`, `Quantidade de clientes � SCR`) \
            VALUES (%(ano)s, %(trimestre)s, %(categoria)s, %(tipo)s, %(cnpj_if)s, %(instituicao_financeira)s, %(indice)s, %(qtd_reclamacoes_reguladas_procedentes)s, \
            %(qtd_reclamacoes_reguladas_outras)s, %(qtd_reclamacoes_nao_reguladas)s, %(qtd_total_reclamacoes)s, %(qtd_total_clientes_ccs)s, %(qtd_total_clientes_scr)s, \
            %(qtd_total_clientes)s)"
    
    return query, params

def empregados_stmt(body):
    params = {
        "employer_name": body.get("employer_name"),
        "reviews_count": body.get("reviews_count"),
        "culture_count": body.get("culture_count"),
        "salaries_count": body.get("salaries_count"),
        "benefits_count": body.get("benefits_count"),
        "employer_website": body.get("employer_website"),
        "employer_headquarters": body.get("employer_headquarters"),
        "employer_founded": body.get("employer_founded"),
        "employer_industry": body.get("employer_industry"),
        "employer_revenue": body.get("employer_revenue"),
        "url": body.get("url"),
        "cultura_e_valores": body.get("cultura_e_valores"),
        "diversidade_e_inclusao": body.get("diversidade_e_inclusao"),
        "qualidade_de_vida": body.get("qualidade_de_vida"),
        "alta_lideranca": body.get("alta_lideranca"),
        "remuneracao_e_beneficios": body.get("remuneracao_e_beneficios"),
        "oportunidade_de_carreira": body.get("oportunidade_de_carreira"),
        "geral": body.get("geral"),
        "pct_recomendam_outras_pessoas": body.get("pct_recomendam_outras_pessoas"),
        "pct_perspectiva_positiva_da_empresa": body.get("pct_perspectiva_positiva_da_empresa"),
        "cnpj": body.get("cnpj"),
        "segmento": body.get("segmento"),
        "nome": body.get("nome"),
        "match_percent": body.get("match_percent")
    }
    query = "INSERT INTO tb_empregados (`employer_name`, `reviews_count`, `culture_count`, `salaries_count`, `benefits_count`, `employer-website`, \
            `employer-headquarters`, `employer-founded`, `employer-industry`, `employer-revenue`, `url`, `Geral`, `Cultura e valores`, \
            `Diversidade e inclusão`, `Qualidade de vida`, `Alta liderança`, `Remuneração e benefícios`, `Oportunidades de carreira`, \
            `Recomendam para outras pessoas(%)`, `Perspectiva positiva da empresa(%)`, `CNPJ`, `Segmento`, `Nome`, `match_percent`) \
            VALUES (%(employer_name)s, %(reviews_count)s, %(culture_count)s, %(salaries_count)s, %(benefits_count)s, %(employer_website)s, %(employer_headquarters)s, \
            %(employer_founded)s, %(employer_industry)s, %(employer_revenue)s, %(url)s, %(cultura_e_valores)s, %(diversidade_e_inclusao)s, %(qualidade_de_vida)s, \
            %(alta_lideranca)s, %(remuneracao_e_beneficios)s, %(oportunidade_de_carreira)s, %(geral)s, %(pct_recomendam_outras_pessoas)s, \
            %(pct_perspectiva_positiva_da_empresa)s, %(cnpj)s, %(segmento)s, %(nome)s, %(match_percent)s)"
    
    
    return query, params


def process(queue, message):
    body = json.loads(message.decode())
    print(f"{queue}: {body}")
    if queue == "bancos":
        query, params = banco_stmt(body)
        connector.execute(query, params)
    elif queue == "reclamacoes":
        query, params = reclamacoes_stmt(body)
        connector.execute(query, params)
    elif queue == "empregados":
        query, params = empregados_stmt(body)
        connector.execute(query, params)
    print(f"Record save\n\n")

def callback(ch, method, properties, body):
    queue = method.routing_key
    process(queue, body)
    

rabbit = RabbitMqConnector()
rabbit.receive(callback)