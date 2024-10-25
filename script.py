import pandas as pd
from sqlalchemy import create_engine, text
import os
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S')

db_username = os.getenv('DB_USERNAME', 'paulorosa')  # Default user set to 'paulorosa'``
db_password = os.getenv('DB_PASSWORD')  # Ensure DB_PASSWORD is set in your environment variables
db_host = os.getenv('DB_HOST', 'localhost')
db_port = os.getenv('DB_PORT', '5432')
db_name = os.getenv('DB_NAME', 'tsmx')
engine = create_engine(f'postgresql+psycopg2://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}')

# Lê o arquivo Excel
file_path = 'dados_importacao.xlsx'
df = pd.read_excel(file_path)

# Mapeando as colunas relevantes para clientes
df_clientes = df.rename(columns={
    'Nome/Razão Social': 'nome_razao_social',
    'Nome Fantasia': 'nome_fantasia',
    'CPF/CNPJ': 'cpf_cnpj',
    'Data Nasc.': 'data_nascimento',
    'Data Cadastro cliente': 'data_cadastro'
})

# Selecionando apenas as colunas necessárias para a tabela tbl_clientes
df_clientes = df_clientes[['nome_razao_social', 'nome_fantasia', 'cpf_cnpj', 'data_nascimento', 'data_cadastro']]

# Remove duplicates from DataFrame based on 'cpf_cnpj'
df_clientes.drop_duplicates(subset='cpf_cnpj', inplace=True)
total_atualizacoes = 0
total_insercoes = 0  # Variável para contar o número de inserções

# Lista para armazenar registros não importados e seus motivos
registros_nao_importados = []

# Verificando e inserindo ou atualizando clientes
for index, row in df_clientes.iterrows():  # Unpack the tuple into index and row
    cpf_cnpj = str(row['cpf_cnpj']).replace('.', '').replace('/', '').replace('-', '')  # Remove caracteres especiais
    nome_razao_social = row['nome_razao_social']
    nome_fantasia = row['nome_fantasia'] if pd.notna(row['nome_fantasia']) else 'Não informado'  # Valor padrão

    # Tratar valores de data
    data_nascimento = row['data_nascimento'] if pd.notna(row['data_nascimento']) else None
    data_cadastro = row['data_cadastro'] if pd.notna(row['data_cadastro']) else None

    # Verifica se o CPF/CNPJ excede o comprimento máximo
    max_length = 14  # Ajuste conforme necessário
    if len(cpf_cnpj) > max_length:
        logging.warning(f"CPF/CNPJ {cpf_cnpj} excede o comprimento máximo de {max_length} caracteres.")
        registros_nao_importados.append({
            'cpf_cnpj': cpf_cnpj,
            'motivo': f"Excede o comprimento máximo de {max_length} caracteres."
        })
        continue  # Ou trate o erro conforme necessário

    try:
        with engine.begin() as conn:  # Use begin() to ensure transactions are committed
            # Verifica se o CPF/CNPJ já existe
            result = conn.execute(text("SELECT id FROM tbl_clientes WHERE cpf_cnpj = :cpf_cnpj"), {'cpf_cnpj': cpf_cnpj}).fetchone()
            
            if result:
                # Atualiza o cliente existente se o CPF/CNPJ já estiver cadastrado
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
                # Insere o novo cliente se não existir duplicata
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
                total_insercoes += 1  # Incrementa o contador de inserções

    except Exception as e:
        logging.error(f"Erro ao processar CPF/CNPJ {cpf_cnpj}: {e}")
        registros_nao_importados.append({
            'cpf_cnpj': cpf_cnpj,
            'motivo': str(e)
        })

# Exibe o resumo com o total de registros importados
total_registros_importados = len(df_clientes)
logging.info(f"Total de registros importados: {total_registros_importados}")
logging.info(f"Total de entradas atualizadas: {total_atualizacoes}")
logging.info(f"Total de novas inserções: {total_insercoes}")

# Exibe os registros não importados e seus motivos
if registros_nao_importados:
    logging.info("Registros não importados e seus motivos:")
    for registro in registros_nao_importados:
        logging.info(f"CPF/CNPJ: {registro['cpf_cnpj']}, Motivo: {registro['motivo']}")
