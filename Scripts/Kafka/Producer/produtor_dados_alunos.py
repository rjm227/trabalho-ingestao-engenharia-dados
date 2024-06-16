from kafka import KafkaProducer
import json
from faker import Faker
from datetime import datetime
from dateutil.relativedelta import relativedelta

server = '10.0.0.4:9092'  # Substitua pelo endereço IP e porta corretos do Apache Kafka
topic = 'dados-alunos' # Substituia pelo nome do topico

def gerar_dados_fake():
    # Função para gerar dados médicos fake usando a biblioteca Faker

    fake = Faker('pt_BR')

    dados_fake = []
    for _ in range(100):
        data_nascimento = fake.date_between_dates(date_start=datetime(1980,1,1), date_end=datetime(2006,12,31)).strftime('%d/%m/%Y')
        data_ingresso = data_nascimento + relativedelta(years=18)
        codigo_curso = fake.random_int(min=1, max=5)
        previsao_conclusao = calcula_previsao_conclusao(codigo_curso, data_ingresso)
        dado_fake = {
            'Matricula': fake.random_int(min=100000, max=999999),
            'Nome': fake.name(),
            'Sexo': fake.random_element(elements=('M', 'F')),
            'CadastroGeralPessoa': fake.cpf(),
            'DataNascimento': data_nascimento,
            'Email': fake.email(),
            'Endereco': fake.address(),
            'TelefoneCelular': fake.phone_number(),
            'Curso': codigo_curso,
            'DataIngresso': data_ingresso,
            'DataPrevistaDeConclusao': previsao_conclusao,
            'DataConclusao': calcula_data_conclusao(previsao_conclusao),
            'StatusMatricula': fake.random_element(elements=('Ativo', 'Trancado', 'Cancelado'))
        }
        dados_fake.append(dado_fake)
    
    return dados_fake

def enviar_dados_kafka(dados):
    producer = KafkaProducer(bootstrap_servers=server, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    
    for dado in dados:
        producer.send(topic, dado)
    
    producer.flush()

dados_fake = gerar_dados_fake()
enviar_dados_kafka(dados_fake)


def calcula_data_conclusao(data_prevista_conclusao):
    return ""

def calcula_previsao_conclusao(codigo_curso, data_ingresso):
    get_dados_curso()


def get_dados_curso():
    return ""
#     return [
#             {"CodigoCurso": 1: {"NomeCurso": "ENGENHARIA DA COMPUTAÇÃO","Periodos": 10,"CargaHorariaPrevistaPorDisciplina": 92,"DisciplinasPorPeriodo": 8,"MediaDeAlunosPorTurma": 50}},
#             {"CodigoCurso": 2: {"NomeCurso": "CIÊNCIA DA COMPUTAÇÃO",,"Periodos": 8,"CargaHorariaPrevistaPorDisciplina": 64,"DisciplinasPorPeriodo": 8,"MediaDeAlunosPorTurma": 62}},
#             "CodigoCurso": 3: {"NomeCurso": "SISTEMA DE INFORMAÇÕES","Periodos": 8,"CargaHorariaPrevistaPorDisciplina": 64,"DisciplinasPorPeriodo": 7,"MediaDeAlunosPorTurma": 48}},
#             "CodigoCurso": 4: {"NomeCurso": "JOGOS DIGITAIS","Periodos": 6,"CargaHorariaPrevistaPorDisciplina": 48,"DisciplinasPorPeriodo": 6,"MediaDeAlunosPorTurma": 32}},
#             "CodigoCurso": 5: {"NomeCurso": "TECNÓLOGO EM REDES DIGITAIS","Periodos": 4,"CargaHorariaPrevistaPorDisciplina": 48,"DisciplinasPorPeriodo": 6,"MediaDeAlunosPorTurma": 28}}
#         ]