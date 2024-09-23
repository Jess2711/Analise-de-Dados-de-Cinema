import pandas as pd
import pyodbc

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

# Ler o arquivo title.principals.csv e descobrir o delimitador automaticamente
file_path = 'C:/Users/viver/Documents/SAD/title.principals.csv'
with open(file_path, 'r', encoding='utf-8') as f:
    first_line = f.readline()
    if ',' in first_line:
        sep = ','
    elif ';' in first_line:
        sep = ';'
    else:
        sep = '\t'

# Ler o CSV em chunks e associar o tconst à Chave_de_Diretor
chunksize = 10000
chunks = pd.read_csv(file_path, sep=sep, chunksize=chunksize)

for chunk in chunks:
    # Iterar sobre as linhas do chunk e inserir o tconst na tabela Diretor
    for index, row in chunk.iterrows():
        nconst = row['nconst']
        tconst = row['tconst']
        
        # Verificar se o nconst existe na tabela Diretor
        cursor.execute("SELECT Chave_de_Diretor FROM Diretor WHERE Chave_de_Diretor = ?", nconst)
        diretor = cursor.fetchone()
        
        if diretor:
            # Se existir, inserir o tconst correspondente
            cursor.execute("""
                UPDATE Diretor
                SET tconst = ?
                WHERE Chave_de_Diretor = ?
            """, tconst, nconst)
            print(f"Inserindo tconst: {tconst} para Chave_de_Diretor: {nconst}")

    # Confirmar as mudanças no banco de dados
    conn.commit()

# Fechar a conexão
cursor.close()
conn.close()

print("Dados inseridos na tabela Diretor com sucesso!")