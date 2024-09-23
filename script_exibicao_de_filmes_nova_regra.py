import pyodbc
import pandas as pd
import os

# Função para detectar delimitador
def detectar_delimitador(arquivo):
    with open(arquivo, 'r', encoding='utf-8') as f:
        linha = f.readline()
        if ',' in linha:
            return ','
        elif '\t' in linha:
            return '\t'
        elif ';' in linha:
            return ';'
        else:
            return ' '  # Caso seja espaço

# Conexão com o banco de dados SQL Server
conn = pyodbc.connect(
    r"Driver={SQL Server};"
    r"Server=JESSICA-WORK\SQLEXPRESS;"
    r"Database=Data_Mart;"
    r"UID=sa;"
    r"PWD=abc123;"
    r"Trusted_Connection=no;"
)

cursor = conn.cursor()

# Função para inserir os dados no banco em lotes
def inserir_dados_lote(cursor, conn, data):
    for index, row in data.iterrows():
        try:
            print(f"Inserindo dados da linha {index}:")
            print(row)  # Log para verificar os dados da linha
            cursor.execute('''
                INSERT INTO Exibicao_de_Filmes (publico, chave_de_tempo, chave_da_sala, id_cidade, Chave_de_ator, Chave_de_diretor, tconst, titulo_original, cnpj_distribuidora)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', 
            row['publico'], 
            row['chave_de_tempo'], 
            row['chave_da_sala'], 
            row['id_cidade'], 
            row['Chave_de_ator'], 
            row['Chave_de_diretor'], 
            row['tconst'],  
            row['titulo_original'], 
            row['cnpj_distribuidora'])
        except pyodbc.Error as e:
            print(f"Erro ao inserir dados da linha {index}: {e}")
            print(f"Dados com erro: {row}")
    conn.commit()

# Função para tratar a conversão da coluna "publico"
def tratar_publico(valor):
    try:
        # Tenta converter o valor em int, ignorando valores não numéricos
        return int(float(valor))
    except (ValueError, TypeError) as e:
        print(f"Erro ao converter 'publico': {valor}, erro: {e}")
        return None  # Retorna None se não for possível converter

# Caminho da pasta com os arquivos CSV
pasta_csv = 'C:/Users/viver/Documents/SAD/bilheteria-diaria-obras-por-distribuidoras-csv'

# Limitador para inserir apenas 1000 registros
limite_registros = 1000
total_inseridos = 0

# Loop para percorrer os arquivos CSV
for arquivo in os.listdir(pasta_csv):
    if arquivo.endswith('.csv'):
        caminho_arquivo = os.path.join(pasta_csv, arquivo)
        
        delimitador = detectar_delimitador(caminho_arquivo)
        
        try:
            # Lendo o CSV com tratamento para bad lines e normalizando as colunas para minúsculas
            df = pd.read_csv(caminho_arquivo, sep=delimitador, on_bad_lines='skip', dtype=str)
            
            # Convertendo todas as colunas para minúsculas
            df.columns = df.columns.str.lower()
            
            print(f"Processando o arquivo: {arquivo}")
            
            # Tratamento dos dados conforme a regra de negócio
            df['publico'] = df['publico'].apply(tratar_publico)  # Aplica a função de tratamento de 'publico'
            df = df.dropna(subset=['publico', 'titulo_original'])  # Remove linhas onde publico ou titulo_original são NaN
            
            # Lógica para buscar e inserir as chaves estrangeiras
            for index, row in df.iterrows():
                if total_inseridos >= limite_registros:
                    break

                titulo_original = row['titulo_original'].strip()  # Remover espaços em branco no 'titulo_original'
                cnpj_distribuidora = row['cnpj_distribuidora'].strip() if pd.notna(row['cnpj_distribuidora']) else None

                # Verificação nas tabelas relacionadas
                try:
                    cursor.execute("SELECT chave_de_tempo FROM Quando WHERE titulo_original = ?", titulo_original)
                    chave_de_tempo = cursor.fetchone()

                    cursor.execute("SELECT chave_da_sala FROM Sala_de_Cinema WHERE titulo_original = ? AND cnpj_distribuidora = ?", titulo_original, cnpj_distribuidora)
                    chave_da_sala = cursor.fetchone()

                    cursor.execute("SELECT id_cidade FROM Cidade WHERE titulo_original = ?", titulo_original)
                    id_cidade = cursor.fetchone()

                    # Match entre tconst da tabela Gênero e o titulo_original
                    cursor.execute("SELECT tconst FROM Genero WHERE titulo_original = ?", titulo_original)
                    tconst_genero = cursor.fetchone()
                except pyodbc.Error as e:
                    print(f"Erro ao buscar chaves estrangeiras para titulo_original '{titulo_original}': {e}")
                    continue

                if tconst_genero:
                    # Verificando o tconst nas tabelas Ator e Diretor usando o tconst da tabela Genero
                    cursor.execute("SELECT Chave_de_ator FROM Ator WHERE tconst = ?", tconst_genero[0])
                    chave_de_ator = cursor.fetchone()

                    cursor.execute("SELECT Chave_de_diretor FROM Diretor WHERE tconst = ?", tconst_genero[0])
                    chave_de_diretor = cursor.fetchone()
                else:
                    chave_de_ator = None
                    chave_de_diretor = None

                # Adicionando os dados que foram buscados ao DataFrame
                df.at[index, 'chave_de_tempo'] = chave_de_tempo[0] if chave_de_tempo else None
                df.at[index, 'chave_da_sala'] = chave_da_sala[0] if chave_da_sala else None
                df.at[index, 'id_cidade'] = id_cidade[0] if id_cidade else None
                df.at[index, 'Chave_de_ator'] = chave_de_ator[0] if chave_de_ator else None
                df.at[index, 'Chave_de_diretor'] = chave_de_diretor[0] if chave_de_diretor else None
                df.at[index, 'tconst'] = tconst_genero[0] if tconst_genero else None

                total_inseridos += 1

            # Inserção dos dados no banco de dados em lotes
            inserir_dados_lote(cursor, conn, df)
            print(f"{total_inseridos} registros inseridos até agora.")
        
        except Exception as e:
            print(f"Erro ao processar o arquivo {arquivo}: {e}")

        if total_inseridos >= limite_registros:
            break

# Fechar a conexão
cursor.close()
conn.close()

print("Processamento concluído.")