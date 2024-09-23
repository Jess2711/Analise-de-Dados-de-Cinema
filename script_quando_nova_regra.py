import pandas as pd
import pyodbc
import os

# Função para extrair trimestre e período
def extrair_trimestre_e_periodo(data_exibicao):
    try:
        data = pd.to_datetime(data_exibicao, format='%d/%m/%Y', errors='coerce')
        trimestre = data.quarter
        if ((data.month == 12 and data.day >= 20) or (data.month == 1 and data.day <= 20) or
            (data.month == 6 and data.day >= 15) or (data.month == 7 and data.day <= 15)):
            periodo = "férias"
        else:
            periodo = "normal"
        return pd.Series([trimestre, periodo])
    except Exception as e:
        print(f"Erro ao processar a data {data_exibicao}: {e}")
        return pd.Series([None, None])

# Função para verificar se a combinação de data e título já existe
def quando_existe(cursor, data_exibicao, titulo_original):
    cursor.execute("""
        SELECT chave_de_Tempo 
        FROM Quando 
        WHERE data_exibicao = ? AND titulo_original = ?
    """, data_exibicao, titulo_original)
    row = cursor.fetchone()
    if row:
        return row[0]
    return None

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
    
    try:
        # Leitura do arquivo CSV em chunks de 1000 linhas
        chunk_size = 1000
        for chunk in pd.read_csv(full_path, sep=';', on_bad_lines='skip', chunksize=chunk_size):
            
            # Normalizar os nomes das colunas removendo espaços e colocando em minúsculas
            chunk.columns = chunk.columns.str.strip().str.lower()
            
            # Verificar se a coluna 'data_exibicao' e 'titulo_original' existem
            if 'data_exibicao' in chunk.columns and 'titulo_original' in chunk.columns:
                chunk[['trimestre', 'periodo']] = chunk['data_exibicao'].apply(extrair_trimestre_e_periodo)
            else:
                print(f"Colunas 'data_exibicao' ou 'titulo_original' não encontradas no arquivo {csv_file}. Pulando este arquivo.")
                continue

            # Inserir dados na tabela 'Quando'
            for index, row in chunk.iterrows():
                # Verificar se a combinação de data e título já existe
                data_id = quando_existe(cursor, row['data_exibicao'], row['titulo_original'])
                
                # Se a combinação não existir, inserir uma nova linha
                if not data_id:
                    cursor.execute("""
                        INSERT INTO Quando (data_exibicao, titulo_original, trimestre, periodo)
                        VALUES (?, ?, ?, ?)
                    """, row['data_exibicao'], row['titulo_original'], row['trimestre'], row['periodo'])
                    
                    # Confirmar a inserção
                    conn.commit()

    except pd.errors.ParserError as e:
        print(f"Erro ao processar o arquivo {csv_file}: {e}")

# Fechar a conexão
cursor.close()
conn.close()

print("Dados inseridos com sucesso!")