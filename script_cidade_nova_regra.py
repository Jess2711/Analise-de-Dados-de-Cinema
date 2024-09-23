import pandas as pd
import pyodbc
import os
import math

# Dicionário para mapear UF para regiões do Brasil
uf_para_regiao = {
    'AC': 'Norte', 'AP': 'Norte', 'AM': 'Norte', 'PA': 'Norte', 'RO': 'Norte', 'RR': 'Norte', 'TO': 'Norte',
    'AL': 'Nordeste', 'BA': 'Nordeste', 'CE': 'Nordeste', 'MA': 'Nordeste', 'PB': 'Nordeste', 'PE': 'Nordeste',
    'PI': 'Nordeste', 'RN': 'Nordeste', 'SE': 'Nordeste',
    'DF': 'Centro-Oeste', 'GO': 'Centro-Oeste', 'MT': 'Centro-Oeste', 'MS': 'Centro-Oeste',
    'ES': 'Sudeste', 'MG': 'Sudeste', 'RJ': 'Sudeste', 'SP': 'Sudeste',
    'PR': 'Sul', 'RS': 'Sul', 'SC': 'Sul'
}

# Função para normalizar os nomes das colunas
def normalizar_colunas(df):
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
    return df

# Função para verificar se a cidade já existe no banco
def cidade_existe(cursor, nome_cidade, nome_estado):
    cursor.execute("""
        SELECT id_cidade FROM Cidade WHERE nome_cidade = ? AND nome_estado = ?
    """, nome_cidade, nome_estado)
    row = cursor.fetchone()
    if row:
        return row[0]  # Retorna o id_cidade se já existir
    return None

# Função para validar dados antes de inserir no banco
def validar_dado(dado):
    if pd.isna(dado) or isinstance(dado, float) and math.isnan(dado):
        return ''
    return str(dado)

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

# Obter o último id_cidade existente no banco para continuar a numeração
cursor.execute("SELECT MAX(id_cidade) FROM Cidade")
id_cidade = cursor.fetchone()[0]
if id_cidade is None:
    id_cidade = 1  # Caso não haja nenhuma cidade, começar com 1
else:
    id_cidade += 1

# Iterar sobre os arquivos CSV
batch_size = 1000

for csv_file in os.listdir(folder_path):
    full_path = os.path.join(folder_path, csv_file)
    print(f"Lendo arquivo: {full_path}")
    
    try:
        # Leitura do arquivo CSV com 'on_bad_lines' para ignorar erros e normalização das colunas
        df = pd.read_csv(full_path, sep=';', on_bad_lines='skip')
        df = normalizar_colunas(df)

        # Verificar se as colunas necessárias existem
        if 'municipio_sala_complexo' in df.columns and 'uf_sala_complexo' in df.columns and 'titulo_original' in df.columns:
            # Renomear colunas para corresponder ao banco de dados
            df['nome_cidade'] = df['municipio_sala_complexo']
            df['nome_estado'] = df['uf_sala_complexo']
            df['nome_regiao'] = df['nome_estado'].map(uf_para_regiao)

            # Inserir dados na tabela 'Cidade' em lotes de 1000 registros
            for i in range(0, len(df), batch_size):
                batch = df.iloc[i:i + batch_size]
                for index, row in batch.iterrows():
                    # Verificar se a cidade já existe no banco de dados
                    nome_cidade = validar_dado(row['nome_cidade'])
                    nome_estado = validar_dado(row['nome_estado'])
                    nome_regiao = validar_dado(row['nome_regiao'])
                    titulo_original = validar_dado(row['titulo_original'])
                    
                    # Ignorar registros vazios
                    if not nome_cidade or not nome_estado:
                        continue
                    
                    cidade_id = cidade_existe(cursor, nome_cidade, nome_estado)
                    
                    if cidade_id is None:
                        # Se a cidade não existir, inserir uma nova cidade e atribuir um novo id_cidade
                        cidade_id = id_cidade
                        cursor.execute("""
                            INSERT INTO Cidade (id_cidade, nome_cidade, nome_estado, nome_regiao, titulo_original)
                            VALUES (?, ?, ?, ?, ?)
                        """, cidade_id, nome_cidade, nome_estado, nome_regiao, titulo_original)
                        
                        id_cidade += 1  # Incrementar o ID para a próxima cidade nova

                # Confirmar a inserção do lote
                conn.commit()
        else:
            print(f"Colunas 'municipio_sala_complexo', 'uf_sala_complexo' ou 'titulo_original' não encontradas no arquivo {csv_file}")

    except pd.errors.ParserError as e:
        print(f"Erro ao processar o arquivo {csv_file}: {e}")

# Fechar a conexão
cursor.close()
conn.close()

print("Dados inseridos com sucesso!")