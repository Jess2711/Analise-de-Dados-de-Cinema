import pandas as pd
import pyodbc
import csv
import time

# Função para normalizar os nomes das colunas
def normalizar_colunas(df):
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
    return df

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

# Recuperar as chaves de ator onde tconst está NULL
cursor.execute("SELECT Chave_de_Ator FROM Ator WHERE tconst IS NULL")
chaves_ator_faltando = set(row[0] for row in cursor.fetchall())
print(f"Faltam {len(chaves_ator_faltando)} registros para inserir tconst.")

# Se não houver registros faltando, encerrar o script
if not chaves_ator_faltando:
    cursor.close()
    conn.close()
    print("Nenhum registro faltando para atualizar.")
    exit()

# Leitura do CSV em chunks com o delimitador detectado
chunk_size = 100  # Reduzido para acelerar os testes
chunks = pd.read_csv(csv_path, sep=delimiter, chunksize=chunk_size, on_bad_lines='skip')

# Variável para contar quantos registros foram atualizados
registros_atualizados = 0
total_chunks = 0

# Tempo inicial
start_time = time.time()

for chunk in chunks:
    # Normalizar as colunas do chunk
    chunk = normalizar_colunas(chunk)
    total_chunks += 1

    # Verificar se as colunas necessárias existem
    if 'nconst' in chunk.columns and 'tconst' in chunk.columns:
        # Filtrar apenas os registros cujo nconst está nas chaves de ator faltando
        chunk_filtro = chunk[chunk['nconst'].isin(chaves_ator_faltando)]

        for index, row in chunk_filtro.iterrows():
            nconst = row['nconst']
            tconst = row['tconst']

            # Inserir o valor de tconst na tabela Ator onde Chave_de_Ator = nconst e tconst é NULL
            cursor.execute("""
                UPDATE Ator SET tconst = ? WHERE Chave_de_Ator = ? AND tconst IS NULL
            """, tconst, nconst)
            registros_atualizados += 1

        # Commit em lotes
        conn.commit()

    # Log após cada chunk
    print(f"Chunk {total_chunks} processado. Registros atualizados até agora: {registros_atualizados}")

# Tempo total
total_time = time.time() - start_time

print(f"Processamento completo. {registros_atualizados} registros foram atualizados.")
print(f"Tempo total de execução: {total_time:.2f} segundos.")

# Fechar a conexão
cursor.close()
conn.close()