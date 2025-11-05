import faker
import random
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
import time
import pytz

# 1. Inicializa o Faker para o Brasil
fake = faker.Faker('pt_BR')
tz_br = pytz.timezone('America/Sao_Paulo')

# 2. Catálogo de Carros Realista (Novos 2025 e Seminovos Populares)
car_catalog = [
    # Mitsubishi
    {"marca": "Mitsubishi", "modelo": "Lancer GT 2.0", "ano_modelo": 2015, "preco_base": 66000.00, "tipo": "Seminovo"},
    {"marca": "Mitsubishi", "modelo": "Lancer HL-T 2.0", "ano_modelo": 2018, "preco_base": 72000.00, "tipo": "Seminovo"},
    {"marca": "Mitsubishi", "modelo": "Lancer Evolution X 2.0T", "ano_modelo": 2014, "preco_base": 220000.00, "tipo": "Seminovo"},
    {"marca": "Mitsubishi", "modelo": "Eclipse Cross Rush 1.5T", "ano_modelo": 2025, "preco_base": 170000.00, "tipo": "Novo"},
    {"marca": "Mitsubishi", "modelo": "L200 Triton Sport HPE", "ano_modelo": 2025, "preco_base": 280000.00, "tipo": "Novo"},
    
    # Honda
    {"marca": "Honda", "modelo": "Civic EXL 2.0", "ano_modelo": 2025, "preco_base": 180000.00, "tipo": "Novo"},
    {"marca": "Honda", "modelo": "Civic Advanced Hybrid", "ano_modelo": 2025, "preco_base": 265000.00, "tipo": "Novo"},
    {"marca": "Honda", "modelo": "HR-V EXL Sensing", "ano_modelo": 2025, "preco_base": 175000.00, "tipo": "Novo"},
    {"marca": "Honda", "modelo": "HR-V EX 1.8", "ano_modelo": 2019, "preco_base": 92000.00, "tipo": "Seminovo"},
    {"marca": "Honda", "modelo": "City Sedan EXL", "ano_modelo": 2025, "preco_base": 140000.00, "tipo": "Novo"},
    
    # Toyota
    {"marca": "Toyota", "modelo": "Corolla XEi 2.0", "ano_modelo": 2025, "preco_base": 169000.00, "tipo": "Novo"},
    {"marca": "Toyota", "modelo": "Corolla Altis Hybrid", "ano_modelo": 2025, "preco_base": 199990.00, "tipo": "Novo"},
    {"marca": "Toyota", "modelo": "Corolla XEi 2.0", "ano_modelo": 2017, "preco_base": 90000.00, "tipo": "Seminovo"},
    {"marca": "Toyota", "modelo": "Hilux SRV 2.8 Diesel", "ano_modelo": 2025, "preco_base": 290000.00, "tipo": "Novo"},
    {"marca": "Toyota", "modelo": "SW4 SRX 2.8 Diesel 4x4", "ano_modelo": 2025, "preco_base": 430000.00, "tipo": "Novo"},
    
    # Fiat
    {"marca": "Fiat", "modelo": "Pulse Audace 1.0T", "ano_modelo": 2025, "preco_base": 124000.00, "tipo": "Novo"},
    {"marca": "Fiat", "modelo": "Toro Volcano 1.3T", "ano_modelo": 2025, "preco_base": 165000.00, "tipo": "Novo"},
    {"marca": "Fiat", "modelo": "Fastback Abarth 1.3T", "ano_modelo": 2025, "preco_base": 153000.00, "tipo": "Novo"},
    {"marca": "Fiat", "modelo": "Argo Drive 1.0", "ano_modelo": 2025, "preco_base": 87000.00, "tipo": "Novo"},
    {"marca": "Fiat", "modelo": "Strada Freedom 1.3", "ano_modelo": 2025, "preco_base": 115000.00, "tipo": "Novo"},
    {"marca": "Fiat", "modelo": "Uno Attractive 1.0", "ano_modelo": 2019, "preco_base": 45000.00, "tipo": "Seminovo"},
    
    # Volkswagen
    {"marca": "Volkswagen", "modelo": "T-Cross Highline 1.4T", "ano_modelo": 2025, "preco_base": 171000.00, "tipo": "Novo"},
    {"marca": "Volkswagen", "modelo": "Nivus Highline 1.0T", "ano_modelo": 2025, "preco_base": 154000.00, "tipo": "Novo"},
    {"marca": "Volkswagen", "modelo": "Gol 1.6 MSI", "ano_modelo": 2020, "preco_base": 55000.00, "tipo": "Seminovo"},
    {"marca": "Volkswagen", "modelo": "Polo Comfortline 1.0T", "ano_modelo": 2025, "preco_base": 112000.00, "tipo": "Novo"},

    # Chevrolet
    {"marca": "Chevrolet", "modelo": "Onix Premier 1.0T", "ano_modelo": 2025, "preco_base": 123000.00, "tipo": "Novo"},
    {"marca": "Chevrolet", "modelo": "Tracker Premier 1.2T", "ano_modelo": 2025, "preco_base": 160000.00, "tipo": "Novo"},
    {"marca": "Chevrolet", "modelo": "Onix Plus LTZ 1.0T", "ano_modelo": 2022, "preco_base": 88000.00, "tipo": "Seminovo"},

    # Hyundai
    {"marca": "Hyundai", "modelo": "HB20 Comfort Plus 1.0T", "ano_modelo": 2025, "preco_base": 110000.00, "tipo": "Novo"},
    {"marca": "Hyundai", "modelo": "Creta N-Line 1.0T", "ano_modelo": 2025, "preco_base": 178000.00, "tipo": "Novo"},
    {"marca": "Hyundai", "modelo": "Creta Attitude 1.6", "ano_modelo": 2020, "preco_base": 83000.00, "tipo": "Seminovo"},
    
    # Jeep
    {"marca": "Jeep", "modelo": "Compass Limited T270", "ano_modelo": 2025, "preco_base": 205000.00, "tipo": "Novo"},
    {"marca": "Jeep", "modelo": "Commander Overland T270", "ano_modelo": 2025, "preco_base": 255000.00, "tipo": "Novo"},
    {"marca": "Jeep", "modelo": "Renegade Longitude 1.3T", "ano_modelo": 2021, "preco_base": 105000.00, "tipo": "Seminovo"},

    # Ford (Seminovos)
    {"marca": "Ford", "modelo": "Ka SE 1.0", "ano_modelo": 2020, "preco_base": 52000.00, "tipo": "Seminovo"},
    {"marca": "Ford", "modelo": "Ranger XLS 2.2 Diesel 4x4", "ano_modelo": 2019, "preco_base": 145000.00, "tipo": "Seminovo"},
    {"marca": "Ford", "modelo": "Bronco Sport Wildtrak", "ano_modelo": 2025, "preco_base": 250000.00, "tipo": "Novo"},
    {"marca": "Ford", "modelo": "Maverick Lariat FX4 Hybrid", "ano_modelo": 2025, "preco_base": 215000.00, "tipo": "Novo"},
    {"marca": "Ford", "modelo": "Fiesta 1.6 SE", "ano_modelo": 2017, "preco_base": 45000.00, "tipo": "Usado"},
    {"marca": "Ford", "modelo": "Ecosport Freestyle 1.5", "ano_modelo": 2020, "preco_base": 79000.00, "tipo": "Seminovo"},

    # Bmw
    {"marca": "BMW", "modelo": "320i Sport GP 2.0 Turbo", "ano_modelo": 2025, "preco_base": 299000.00, "tipo": "Novo"},
    {"marca": "BMW", "modelo": "X1 sDrive20i GP", "ano_modelo": 2025, "preco_base": 289900.00, "tipo": "Novo"},
    {"marca": "BMW", "modelo": "320i Sport GP 2.0 Turbo", "ano_modelo": 2021, "preco_base": 198000.00, "tipo": "Seminovo"},

    # Mercedez
    {"marca": "Mercedes-Benz", "modelo": "GLA 200 Advance", "ano_modelo": 2025, "preco_base": 279000.00, "tipo": "Novo"},
    {"marca": "Mercedes-Benz", "modelo": "C200 Avantgarde", "ano_modelo": 2025, "preco_base": 335000.00, "tipo": "Novo"},
    {"marca": "Mercedes-Benz", "modelo": "C200 Avantgarde", "ano_modelo": 2020, "preco_base": 190000.00, "tipo": "Seminovo"},

    # Audi
    {"marca": "Audi", "modelo": "A3 Sedan Performance 2.0", "ano_modelo": 2025, "preco_base": 245000.00, "tipo": "Novo"},
    {"marca": "Audi", "modelo": "Q3 Prestige 1.4T", "ano_modelo": 2025, "preco_base": 265000.00, "tipo": "Novo"},
    {"marca": "Audi", "modelo": "A3 Sedan Prestige 1.4T", "ano_modelo": 2021, "preco_base": 160000.00, "tipo": "Seminovo"},

    # Renault
    {"marca": "Renault", "modelo": "Kwid Zen 1.0", "ano_modelo": 2025, "preco_base": 72000.00, "tipo": "Novo"},
    {"marca": "Renault", "modelo": "Duster Iconic 1.3 Turbo", "ano_modelo": 2025, "preco_base": 152000.00, "tipo": "Novo"},
    {"marca": "Renault", "modelo": "Sandero Stepway 1.6", "ano_modelo": 2021, "preco_base": 65000.00, "tipo": "Seminovo"},

    # Peugeot / Citroën
    {"marca": "Peugeot", "modelo": "208 Active 1.6 AT", "ano_modelo": 2025, "preco_base": 98000.00, "tipo": "Novo"},
    {"marca": "Peugeot", "modelo": "2008 Style 1.6", "ano_modelo": 2025, "preco_base": 118000.00, "tipo": "Novo"},
    {"marca": "Citroën", "modelo": "C3 Feel 1.0", "ano_modelo": 2025, "preco_base": 78000.00, "tipo": "Novo"},

    # Volvo
    {"marca": "Volvo", "modelo": "XC40 Recharge Pure Electric", "ano_modelo": 2025, "preco_base": 279000.00, "tipo": "Novo"},
    {"marca": "Volvo", "modelo": "C40 Recharge Pure Electric", "ano_modelo": 2024, "preco_base": 265000.00, "tipo": "Seminovo"},
    {"marca": "Volvo", "modelo": "XC60 T8 Hybrid", "ano_modelo": 2022, "preco_base": 315000.00, "tipo": "Seminovo"},

    # BYD
    {"marca": "BYD", "modelo": "Dolphin Plus", "ano_modelo": 2025, "preco_base": 155000.00, "tipo": "Novo"},
    {"marca": "BYD", "modelo": "Dolphin Standard", "ano_modelo": 2024, "preco_base": 124000.00, "tipo": "Seminovo"},
    {"marca": "BYD", "modelo": "Han EV", "ano_modelo": 2025, "preco_base": 355000.00, "tipo": "Novo"},
    {"marca": "BYD", "modelo": "Song Plus DM-i", "ano_modelo": 2025, "preco_base": 225000.00, "tipo": "Novo"},

    # Tesla
    {"marca": "Tesla", "modelo": "Model 3 Long Range", "ano_modelo": 2024, "preco_base": 310000.00, "tipo": "Seminovo"},
    {"marca": "Tesla", "modelo": "Model Y Performance", "ano_modelo": 2025, "preco_base": 360000.00, "tipo": "Novo"},

    # Nissan
    {"marca": "Nissan", "modelo": "Kicks Exclusive 1.6 CVT", "ano_modelo": 2025, "preco_base": 144000.00, "tipo": "Novo"},
    {"marca": "Nissan", "modelo": "Versa Exclusive", "ano_modelo": 2025, "preco_base": 115000.00, "tipo": "Novo"},
    {"marca": "Nissan", "modelo": "Kicks SL 1.6", "ano_modelo": 2021, "preco_base": 88000.00, "tipo": "Seminovo"},

    # Land Rover
    {"marca": "Land Rover", "modelo": "Discovery Sport R-Dynamic", "ano_modelo": 2025, "preco_base": 389000.00, "tipo": "Novo"},
    {"marca": "Land Rover", "modelo": "Range Rover Evoque P250", "ano_modelo": 2024, "preco_base": 350000.00, "tipo": "Seminovo"},

    # Suzuki
    {"marca": "Suzuki", "modelo": "Jimny Sierra 4Sport", "ano_modelo": 2025, "preco_base": 168000.00, "tipo": "Novo"},
    {"marca": "Suzuki", "modelo": "Vitara 4Sport", "ano_modelo": 2019, "preco_base": 98000.00, "tipo": "Seminovo"},

]

# 3. Lista de nomes de concessionárias (Sufixos comuns)
store_suffixes = ["Motors", "Veículos", "Concessionária", "Car", "Multimarcas", "Auto Center"]

# 4. Métodos de Pagamento
payment_methods = ["Financiamento", "À Vista", "Consórcio", "Troca com Troco"]

# ID de Venda global para garantir que seja único
venda_counter = 20250000

def gerar_dados_venda(num_vendas):
    """Gera uma lista de dados fictícios de vendas."""
    global venda_counter
    dados_vendas = []
    
    for _ in range(num_vendas):
        # --- Dados do Carro ---
        carro = random.choice(car_catalog)
        
        preco_base = carro["preco_base"]
        variacao = random.uniform(0.95, 1.08) if carro["tipo"] == "Seminovo" else random.uniform(0.98, 1.05)
        preco_venda = round(preco_base * variacao, 2)
        
        # --- Dados da Loja (Concessionária) ---
        loja_estado = fake.state_abbr()
        loja_cidade = fake.city()
        
        if random.random() > 0.3:
             nome_loja = f"{carro['marca']} {loja_cidade}"
        else:
             nome_loja = f"{fake.last_name()} {random.choice(store_suffixes)}"

        # --- Dados do Cliente ---
        cliente_nome = fake.name()
        cliente_estado = fake.state_abbr()
        cliente_cidade = fake.city()
        
        # --- Dados da Transação ---
        venda_id = f"VENDA-{venda_counter}"
        venda_counter += 1 # Incrementa o ID global
        
        # Gera data da venda nos últimos 10 minutos para simular tempo real
        data_venda = fake.date_time_between(start_date="-10m", end_date="now", tzinfo=tz_br)

        metodo_pagamento = random.choice(payment_methods)

        # --- Monta o registro da venda ---
        # Note que não incluímos id, created_at ou updated_at. O DB cuida disso.
        registro = {
            "id_venda": venda_id,
            "data_venda": data_venda,
            "cliente_nome": cliente_nome,
            "cliente_email": f"{cliente_nome.split(' ')[0].lower()}@email.com",
            "cliente_estado": cliente_estado,
            "cliente_cidade": cliente_cidade,
            "loja_nome": nome_loja,
            "loja_estado": loja_estado,
            "loja_cidade": loja_cidade,
            "carro_marca": carro["marca"],
            "carro_modelo": carro["modelo"],
            "carro_ano": carro["ano_modelo"],
            "carro_tipo": carro["tipo"],
            "preco_base_tabela": preco_base,
            "preco_venda_final": preco_venda,
            "metodo_pagamento": metodo_pagamento
        }
        dados_vendas.append(registro)
        
    return dados_vendas

# --- INGESTÃO CONTÍNUA NO POSTGRESQL ---

print("--- Iniciando Ingestão Contínua no PostgreSQL ---")

# 1. String de conexão
USER = "postgres"
PASSWORD = "postgres"
HOST = "localhost" 
PORT = "5432"
DB_NAME = "cars"
DATABASE_URL = f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME}"

# 2. Tenta se conectar ao banco
try:
    engine = create_engine(DATABASE_URL)
    print(f"Conectado ao banco de dados '{DB_NAME}' com sucesso.")
    print("Iniciando loop de ingestão... (Pressione CTRL+C para parar)")

    # 3. Inicia o loop infinito
    while True:
        try:
            # 4. Gera um lote aleatório de 1 a 5 vendas
            num_novas_vendas = random.randint(1, 5)
            dados_ficticios = gerar_dados_venda(num_novas_vendas)
            df = pd.DataFrame(dados_ficticios)

            # 5. Faz a ingestão usando 'append'
            df.to_sql(
                'vendas_carros', 
                con=engine, 
                if_exists='append', # << IMPORTANTE: Mudou de 'replace' para 'append'
                index=False,
                method='multi'
            )
            
            print(f"Sucesso: Lote de {num_novas_vendas} novos registros inseridos.")

            # 6. Aguarda um tempo aleatório (5 a 120 segundos)
            intervalo_sleep = random.uniform(5, 120)
            print(f"Aguardando {intervalo_sleep:.2f} segundos...")
            time.sleep(intervalo_sleep)

        except KeyboardInterrupt:
            print("\nLoop interrompido pelo usuário. Encerrando.")
            break
        except Exception as e:
            print(f"Erro durante a inserção no loop: {e}")
            print("Tentando novamente em 30 segundos...")
            time.sleep(30) # Espera mais se houver um erro de inserção

except Exception as e:
    print(f"\nOcorreu um erro fatal ao conectar ao banco de dados:")
    print(e)
    print("\nVerifique se o container 'postgres-db' está em execução e se a tabela 'vendas_carros' foi criada com o SQL.")