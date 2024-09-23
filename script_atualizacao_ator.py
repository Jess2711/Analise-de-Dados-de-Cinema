import pandas as pd
import pyodbc
import csv

# Função para normalizar os nomes das colunas
def normalizar_colunas(df):
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
    return df

# Função para verificar se a chave de ator (nconst) existe na tabela Ator e se o campo tconst está vazio
def verificar_chave_ator(cursor, nconst):
    cursor.execute("""
        SELECT Chave_de_Ator, tconst FROM Ator WHERE Chave_de_Ator = ?
    """, nconst)
    row = cursor.fetchone()
    if row:
        return row[0], row[1]  # Retorna Chave_de_Ator e tconst se existir
    return None, None

# Função para detectar o delimitador do arquivo CSV
def detectar_delimitador(csv_path):
    with open(csv_path, 'r') as file:
        sniffer = csv.Sniffer()
        sample = file.read(1024)  # Ler uma amostra do arquivo para detectar o delimitador
        has_header = sniffer.has_header(sample)  # Verificar se o arquivo tem cabeçalho
        delimiter = sniffer.sniff(sample).delimiter  # Detectar o delimitador
        return delimiter, has_header

# Caminho para o arquivo CSV
csv_path = 'C:/Users/viver/Documents/SAD/title.principals.csv'

# Detectar o delimitador do CSV
delimiter, has_header = detectar_delimitador(csv_path)
print(f"Delimitador detectado: '{delimiter}', com cabeçalho: {has_header}")

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

# Verificar se a coluna 'tconst' existe na tabela Ator
cursor.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'Ator' AND COLUMN_NAME = 'tconst'")
column_exists = cursor.fetchone()
if not column_exists:
    print("A coluna 'tconst' não existe na tabela 'Ator'. Verifique a estrutura da tabela.")
    cursor.close()
    conn.close()
    exit()

# Tamanho do lote para commits
batch_size = 1000

# Leitura do CSV em chunks com o delimitador detectado
chunk_size = 1000
chunks = pd.read_csv(csv_path, sep=delimiter, chunksize=chunk_size, on_bad_lines='skip')

for chunk in chunks:
    # Normalizar as colunas do chunk
    chunk = normalizar_colunas(chunk)

    # Verificar se as colunas necessárias existem
    if 'nconst' in chunk.columns and 'tconst' in chunk.columns:
        for index, row in chunk.iterrows():
            nconst = row['nconst']
            tconst = row['tconst']

            # Verificar se o nconst já está na tabela Ator e se o tconst está vazio
            chave_ator, tconst_banco = verificar_chave_ator(cursor, nconst)

            if chave_ator and not tconst_banco:  # Se o tconst está vazio
                print(f"Inserindo tconst: {tconst} para Chave_de_Ator: {chave_ator}")
                # Inserir o valor de tconst na tabela Ator onde Chave_de_Ator = nconst e tconst é NULL
                cursor.execute("""
                    UPDATE Ator SET tconst = ? WHERE Chave_de_Ator = ? AND tconst IS NULL
                """, tconst, chave_ator)

        # Commit em lotes
        conn.commit()

# Fechar a conexão
cursor.close()
conn.close()

print("Dados inseridos na tabela Ator com sucesso!")