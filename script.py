import pandas as pd
from sqlalchemy import create_engine, text
import os
import logging
from sqlalchemy.exc import IntegrityError

# Configurar logging
logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

db_username = os.getenv('DB_USERNAME', 'paulorosa')  
db_password = os.getenv('DB_PASSWORD')  
db_host = os.getenv('DB_HOST', 'localhost')
db_port = os.getenv('DB_PORT', '5432')
db_name = os.getenv('DB_NAME', 'tsmx')
engine = create_engine(f'postgresql+psycopg2://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}')

# Ler o arquivo Excel
file_path = 'dados_importacao.xlsx'
df = pd.read_excel(file_path)

# Mapeando colunas relevantes para clientes
df_clientes = df.rename(columns={
    'Nome/Razão Social': 'nome_razao_social',
    'Nome Fantasia': 'nome_fantasia',
    'CPF/CNPJ': 'cpf_cnpj',
    'Data Nasc.': 'data_nascimento',
    'Data Cadastro cliente': 'data_cadastro'
})

# Selecionando apenas as colunas necessárias para a tabela tbl_clientes
df_clientes = df_clientes[['nome_razao_social', 'nome_fantasia', 'cpf_cnpj', 'data_nascimento', 'data_cadastro']]
df_clientes.drop_duplicates(subset='cpf_cnpj', inplace=True)

total_atualizacoes = 0
total_insercoes = 0
registros_nao_importados = []

for index, row in df_clientes.iterrows():
    cpf_cnpj = str(row['cpf_cnpj']).replace('.', '').replace('/', '').replace('-', '')
    nome_razao_social = row['nome_razao_social']
    nome_fantasia = row['nome_fantasia'] if pd.notna(row['nome_fantasia']) else 'Não informado'
    
    data_nascimento = pd.to_datetime(row['data_nascimento'], errors='coerce') if pd.notna(row['data_nascimento']) else None
    data_cadastro = pd.to_datetime(row['data_cadastro'], errors='coerce') if pd.notna(row['data_cadastro']) else None
    
    max_length = 14
    if len(cpf_cnpj) > max_length:
        logging.warning(f"CPF/CNPJ {cpf_cnpj} excede o comprimento máximo de {max_length} caracteres.")
        registros_nao_importados.append({'cpf_cnpj': cpf_cnpj, 'motivo': f"Excede o comprimento máximo de {max_length} caracteres."})
        continue

    try:
        with engine.begin() as conn:
            result = conn.execute(text("SELECT id FROM tbl_clientes WHERE cpf_cnpj = :cpf_cnpj"), {'cpf_cnpj': cpf_cnpj}).fetchone()
            
            if result:
                # Atualiza cliente existente
                conn.execute(text("""
                    UPDATE tbl_clientes
                    SET nome_razao_social = :nome_razao_social,
                        nome_fantasia = :nome_fantasia,
                        data_nascimento = :data_nascimento,
                        data_cadastro = :data_cadastro
                    WHERE cpf_cnpj = :cpf_cnpj
                """), {
                    'nome_razao_social': nome_razao_social,
                    'nome_fantasia': nome_fantasia,
                    'data_nascimento': data_nascimento,
                    'data_cadastro': data_cadastro,
                    'cpf_cnpj': cpf_cnpj
                })
                total_atualizacoes += 1
            else:
                logging.info(f"Inserindo novo cliente com CPF/CNPJ {cpf_cnpj}.")
                conn.execute(text("""
                    INSERT INTO tbl_clientes (nome_razao_social, nome_fantasia, cpf_cnpj, data_nascimento, data_cadastro)
                    VALUES (:nome_razao_social, :nome_fantasia, :cpf_cnpj, :data_nascimento, :data_cadastro)
                """), {
                    'nome_razao_social': nome_razao_social,
                    'nome_fantasia': nome_fantasia,
                    'cpf_cnpj': cpf_cnpj,
                    'data_nascimento': data_nascimento,
                    'data_cadastro': data_cadastro
                })
                total_insercoes += 1

    except IntegrityError as ie:
        logging.error(f"Erro de integridade para CPF/CNPJ {cpf_cnpj}: {ie}")
        registros_nao_importados.append({'cpf_cnpj': cpf_cnpj, 'motivo': str(ie)})
    except Exception as e:
        logging.error(f"Erro ao processar CPF/CNPJ {cpf_cnpj}: {e}")
        registros_nao_importados.append({'cpf_cnpj': cpf_cnpj, 'motivo': str(e)})

# Log de resumo
logging.info(f"Total de registros importados: {len(df_clientes)}")
logging.info(f"Total de entradas atualizadas: {total_atualizacoes}")
logging.info(f"Total de novas inserções: {total_insercoes}")

# Log de registros não importados
if registros_nao_importados:
    logging.info("Registros não importados e seus motivos:")
    for registro in registros_nao_importados:
        logging.info(f"CPF/CNPJ: {registro['cpf_cnpj']}, Motivo: {registro['motivo']}")
