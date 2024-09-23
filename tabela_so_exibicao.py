import pandas as pd
import pyodbc
import os

# Função para limpar e tratar a coluna 'publico'
def tratar_publico(valor):
    try:
        # Remover espaços, tratar valores nulos e converter para número inteiro
        if pd.isnull(valor) or valor == '' or str(valor).isspace():
            return None
        return int(valor)
    except ValueError:
        # Caso não consiga converter, retornar None
        return None

# Função para limpar e tratar a coluna 'cnpj_distribuidora'
def clean_cnpj(cnpj):
    """Remove caracteres especiais de CNPJ e retorna o valor numérico como string"""
    if pd.isna(cnpj):  # Verifica se o valor é nulo
        return None
    # Limpar o CNPJ e garantir que tenha 14 dígitos
    cnpj_clean = cnpj.replace('.', '').replace('/', '').replace('-', '')
    return cnpj_clean if len(cnpj_clean) == 14 else None

# Função para normalizar os nomes das colunas
def normalize_column_name(df):
    df.columns = df.columns.str.strip().str.lower()
    return df

# Caminho para a pasta com os arquivos CSV
folder_path = 'C:/Users/viver/Documents/SAD/bilheteria-diaria-obras-por-distribuidoras-csv'

# Conexão com o banco de dados SQL Server
conn_str = (
    r"Driver={SQL Server};"
    r"Server=JESSICA-WORK\SQLEXPRESS;"
    r"Database=Data_Mart;"
    r"UID=sa;"
    r"PWD=abc123;"
    r"Trusted_Connection=no;"
)
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# Iterar sobre os arquivos CSV
for csv_file in os.listdir(folder_path):
    full_path = os.path.join(folder_path, csv_file)
    print(f"Lendo arquivo: {full_path}")

    if not full_path.endswith('.csv'):
        continue

    try:
        # Leitura do arquivo CSV com 'on_bad_lines' para ignorar erros
        df = pd.read_csv(full_path, sep=';', on_bad_lines='skip')

        # Normalizar os nomes das colunas
        df = normalize_column_name(df)

        # Exibir os nomes das colunas para verificar a estrutura
        print("Colunas disponíveis no arquivo:", df.columns)
        
        # Verificar se as colunas necessárias existem após normalização
        colunas_necessarias = ['publico', 'titulo_original', 'nome_sala', 'data_exibicao', 'cnpj_distribuidora']
        if all(coluna in df.columns for coluna in colunas_necessarias):
            # Tratar as colunas 'publico' e 'cnpj_distribuidora'
            df['publico_tratado'] = df['publico'].apply(tratar_publico)
            df['cnpj_tratado'] = df['cnpj_distribuidora'].apply(clean_cnpj)

            # Filtrar linhas onde 'publico_tratado' e 'cnpj_tratado' são válidos
            df = df[df['publico_tratado'].notnull() & df['cnpj_tratado'].notnull()]

            # Inserir dados na tabela 'Exibicao_de_Filmes' em lotes de 1000 registros
            batch_size = 1000
            for i in range(0, len(df), batch_size):
                batch = df.iloc[i:i + batch_size]
                for index, row in batch.iterrows():
                    # Inserir dados na tabela 'Exibicao_de_Filmes'
                    cursor.execute("""
                        INSERT INTO Exibicao_de_Filmes (data_exibicao, titulo_original, publico, nome_da_sala, cnpj_distribuidora)
                        VALUES (?, ?, ?, ?, ?)
                    """, row['data_exibicao'], row['titulo_original'], row['publico_tratado'], row['nome_sala'], float(row['cnpj_tratado']))
            
                # Confirmar a inserção do lote
                conn.commit()
        else:
            print(f"Colunas necessárias não encontradas no arquivo {csv_file}. Pulando este arquivo.")

    except pd.errors.ParserError as e:
        print(f"Erro ao processar o arquivo {csv_file}: {e}")
    except pyodbc.Error as db_err:
        print(f"Erro de banco de dados: {db_err}")

# Fechar a conexão
cursor.close()
conn.close()

print("Dados inseridos com sucesso!")