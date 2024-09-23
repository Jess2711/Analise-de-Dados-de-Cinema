import pandas as pd
import pyodbc
import os

# Função para identificar o delimitador
def identificar_delimitador(arquivo):
    with open(arquivo, 'r', encoding='utf-8') as f:
        linha = f.readline()
        if '\t' in linha:
            return '\t'
        elif ',' in linha:
            return ','
        else:
            return ';'

# Função para normalizar os nomes das colunas e os dados
def normalizar_colunas(df):
    df.columns = df.columns.str.strip().str.lower()
    return df

# Função para truncar o titulo_original
def truncar_titulo(titulo, limite=255):
    if pd.isnull(titulo):
        return None
    return titulo[:limite]  # Trunca o título para o limite de caracteres

# Caminho para o arquivo CSV
csv_file = 'C:/Users/viver/Documents/SAD/title.basics.tsv/title.basics.csv'

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

# Identificar o delimitador do arquivo
delimitador = identificar_delimitador(csv_file)

# Lendo o arquivo CSV com o delimitador identificado e aplicando os tratamentos de erro
try:
    df = pd.read_csv(csv_file, sep=delimitador, on_bad_lines='skip', low_memory=False)

    # Normalizar as colunas
    df = normalizar_colunas(df)

    # Exibir as colunas disponíveis no arquivo
    print("Colunas disponíveis no arquivo:", df.columns)
    
    # Verificar se as colunas necessárias estão presentes
    if 'tconst' in df.columns and 'originaltitle' in df.columns and 'genres' in df.columns:
        # Filtrar as linhas onde 'tconst' não está vazio
        df = df[df['tconst'].notnull()]
        
        # Inserir dados na tabela 'Genero' em lotes de 1000 linhas
        batch_size = 1000
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i + batch_size]
            for index, row in batch.iterrows():
                # Verificar e pegar o primeiro gênero
                genero = row['genres'].split(',')[0] if pd.notnull(row['genres']) else None
                
                # Truncar o título original se necessário
                titulo_original = truncar_titulo(row['originaltitle'], limite=255)
                
                # Inserir no banco apenas se o gênero estiver presente
                if genero:
                    cursor.execute("""
                        INSERT INTO Genero (tconst, titulo_original, genero)
                        VALUES (?, ?, ?)
                    """, row['tconst'], titulo_original, genero)
            
            # Confirmar a inserção do lote
            conn.commit()

    else:
        print(f"Colunas necessárias não encontradas no arquivo {csv_file}.")
except pd.errors.ParserError as e:
    print(f"Erro ao processar o arquivo {csv_file}: {e}")

# Fechar a conexão
cursor.close()
conn.close()

print("Dados inseridos com sucesso!")