import pandas as pd
import pyodbc

# Conexão com o banco de dados
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

# Leitura do arquivo CSV
csv_file = 'C:/Users/viver/Documents/SAD/name.basics.tsv/name.basics.csv'
df = pd.read_csv(csv_file, sep=',', usecols=['nconst', 'primaryName', 'primaryProfession'])

#verificar as colunas
print("Colunas no Dataframe:", df.columns)
print(df.head())

# Converter todos os valores para string para evitar problemas de tipos
df['nconst'] = df['nconst'].astype(str)
df['primaryName'] = df['primaryName'].astype(str)
df['primaryProfession'] = df['primaryProfession'].astype(str)

# Remover quaisquer linhas com valores nulos
df.dropna(subset=['nconst', 'primaryName', 'primaryProfession'], inplace=True)

# Filtrar dados por actor, actress e director
df_ator = df[df['primaryProfession'].str.contains('actor|actress', na=False)]
df_diretor = df[df['primaryProfession'].str.match(r'^director$', na=False)]

# Inserir apenas 100 linhas para teste
#df_ator_test = df_ator.head(100)
#df_diretor_test = df_diretor.head(100)

# Inserir na tabela Ator
#for index, row in df_ator_test.iterrows():
for index, row in df_ator.iterrows():
    cursor.execute("""
        INSERT INTO Ator (Chave_de_Ator, Nome_do_Ator)
        VALUES (?, ?, ?)
    """, row['nconst'], row['primaryName'])

    # Commit a cada 1000 registros
    if index % 1000 == 0:
        conn.commit()

# Inserir na tabela Diretor
#for index, row in df_diretor_test.iterrows():
for index, row in df_diretor.iterrows():
    cursor.execute("""
        INSERT INTO Diretor (Chave_de_Diretor, Nome_do_Diretor)
        VALUES (?, ?, ?)
    """, row['nconst'], row['primaryName'])

    # Commit a cada 1000 registros
    if index % 1000 == 0:
        conn.commit()

# Confirmar as inserções
conn.commit()


# Fechar a conexão
cursor.close()
conn.close()