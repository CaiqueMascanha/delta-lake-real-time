import json
import random
import uuid
from faker import Faker
from datetime import datetime
from confluent_kafka import Producer
import time

conf = {
    'bootstrap.servers': 'localhost:9094'
}
TOPICO = 'sales_cars'

producer = Producer(conf)

# Inicializa o Faker para Português do Brasil (para gerar estados brasileiros)
fake = Faker('pt_BR')

# Dicionário de modelos de carros baseados nas marcas permitidas
# Adicione mais modelos se desejar
CAR_MODELS = {
    "Toyota": ["Corolla", "Hilux", "Yaris", "RAV4", "SW4", "Corolla Cross"],
    "Honda": ["Civic", "HR-V", "CR-V", "City", "Fit", "WR-V"],
    "Mitsubishi": ["L200 Triton", "Pajero Sport", "Eclipse Cross", "Outlander", "ASX"]
}

# --- Definição das 5 lojas de vendas ---
DEALER_SHOPS = [
    {"dealerId": "10001", "dealerName": "AutoBraz Veículos"},
    {"dealerId": "10002", "dealerName": "Carros S.A."},
    {"dealerId": "10003", "dealerName": "Horizonte Motors"},
    {"dealerId": "10004", "dealerName": "Imperial Automóveis"},
    {"dealerId": "10005", "dealerName": "SulCar Multimarcas"}
]

def generate_car_transaction():
    """
    Gera um único registro de transação de carro fictício.
    """
    
    # 1. Escolhe a marca (make)
    make = random.choice(list(CAR_MODELS.keys()))
    
    # 2. Escolhe o modelo (model) baseado na marca
    model = random.choice(CAR_MODELS[make])
    
    # 3. Gera o ano (year)
    # Gera um ano entre 2015 e o ano atual
    current_year = datetime.now().year
    year = str(random.randint(2015, current_year))
    
    # 4. Gera o timestamp da venda (saleTimestamp)
    # Gera uma data/hora nos últimos 5 anos e formata como YYYYMMDD
    sale_date = fake.date_time_between(start_date='-5y', end_date='now')
    sale_timestamp = sale_date.strftime("%Y%m%d")
    
    # 5. Escolhe uma das 5 concessionárias definidas
    selected_dealer = random.choice(DEALER_SHOPS)
    dealer_id = selected_dealer["dealerId"]
    dealer_name = selected_dealer["dealerName"]
    
    # 6. Gera o estado (state) - Usando o provider pt_BR
    # CORREÇÃO: O método correto é state()
    state = fake.state()
    
    # 7. Gera o preço (price)
    # Preço fictício entre 50.000 e 250.000
    price = str(random.randint(50000, 250000))
    
    # 8. Gera o ID da transação (transactionId)
    transaction_id = str(uuid.uuid4())
    
    # Monta o dicionário final
    transaction_data = {
        "transactionId": transaction_id,
        "make": make,
        "model": model,
        "year": year,
        "saleTimestamp": sale_timestamp,
        "dealerId": dealer_id,
        "dealerName": dealer_name,
        "state": state,
        "price": price
    }
    
    return transaction_data

def delivery_report(err, msg):
    if err is not None:
        print(f"Erro ao entregar mensagem: {err}")
    else:
        print(f"Mensagem entregue em {msg.topic()} [{msg.partition()}]")

# --- Execução Principal ---
if __name__ == "__main__":
 
    # Vamos enviar 50 mensagens para ter um volume bom de teste
    transactions_list = []
    print(f"Enviando 50 mensagens para o tópico: {TOPICO}...")
 
    for _ in range(120):
        cars = generate_car_transaction()
        transactions_list.append(cars)
        
        producer.produce(
            TOPICO,
            key=str(cars['transactionId']).encode('utf-8'),
            value=json.dumps(cars).encode('utf-8'), 
            callback=delivery_report
        )
        print('Aguardando 5 segundos...')
        time.sleep(5)
        # Não precisa do poll(0) dentro do loop, 
        # o flush() no final é mais eficiente para isso.
 
    # 1. Poll para processar os callbacks (delivery_reports)
    producer.poll(1) 
 
    # 2. Flush - ESSENCIAL: 
    # Bloqueia o script até que todas as mensagens sejam enviadas.
    print("Aguardando envio de todas as mensagens...")
    producer.flush()
    print("Todas as mensagens foram enviadas com sucesso!")


