import pandas as pd
import pyodbc
import os

# Função para gerar chave_da_sala incremental
def obter_proximo_id(cursor):
    cursor.execute("SELECT ISNULL(MAX(chave_da_sala), 0) + 1 FROM Sala_de_Cinema")
    return cursor.fetchone()[0]

# Função para normalizar os nomes das colunas e os dados
def normalizar_colunas(df):
    df.columns = df.columns.str.strip().str.lower()
    if 'cnpj_distribuidora' in df.columns:
        # Limpar e garantir que CNPJ seja tratado como string
        df['cnpj_distribuidora'] = df['cnpj_distribuidora'].astype(str).str.replace(r'\D', '', regex=True)
        # Filtrar CNPJs com menos de 14 caracteres
        df = df[df['cnpj_distribuidora'].str.len() == 14]
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
        df = normalizar_colunas(df)

        # Exibir os nomes das colunas para verificar a estrutura
        print("Colunas disponíveis no arquivo:", df.columns)
        
        # Verificar se as colunas necessárias existem após normalização
        if 'nome_sala' in df.columns and 'cnpj_distribuidora' in df.columns and 'municipio_sala_complexo' in df.columns and 'titulo_original' in df.columns:
            # Filtrar linhas onde 'registro_sala' não está vazio
            df = df[df['registro_sala'].notnull()]
            
            # Inserir dados na tabela 'Sala_de_Cinema' em lotes de 1000 registros
            batch_size = 1000
            for i in range(0, len(df), batch_size):
                batch = df.iloc[i:i + batch_size]
                for index, row in batch.iterrows():
                    # Gerar novo ID para chave_da_sala
                    chave_da_sala = obter_proximo_id(cursor)

                    # Inserir dados na tabela 'Sala_de_Cinema'
                    cursor.execute("""
                        INSERT INTO Sala_de_Cinema (nome_da_sala, chave_da_sala, cnpj_distribuidora, nome_cidade, titulo_original)
                        VALUES (?, ?, ?, ?, ?)
                    """, row['nome_sala'], chave_da_sala, row['cnpj_distribuidora'], row['municipio_sala_complexo'], row['titulo_original'])
            
                # Confirmar a inserção do lote
                conn.commit()
        else:
            print(f"Colunas necessárias não encontradas no arquivo {csv_file}. Pulando este arquivo.")

    except pd.errors.ParserError as e:
        print(f"Erro ao processar o arquivo {csv_file}: {e}")

# Fechar a conexão
cursor.close()
conn.close()

print("Dados inseridos com sucesso!")