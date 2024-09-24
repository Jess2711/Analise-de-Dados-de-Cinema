CREATE TABLE Relacionamento_Filmes (
    id_relacionamento INT IDENTITY(1,1) PRIMARY KEY,
    id_cidade INT NOT NULL,
    id_genero VARCHAR(50) NOT NULL,
    chave_de_tempo INT NOT NULL,
    chave_da_sala INT NOT NULL,
    chave_de_ator VARCHAR(20) NOT NULL,
    chave_de_diretor VARCHAR(20) NOT NULL,
    titulo_original NVARCHAR(255) NOT NULL,
    cnpj_distribuidora FLOAT NULL,
    publico INT NULL,
    FOREIGN KEY (id_cidade) REFERENCES Cidade(id_cidade),
    FOREIGN KEY (id_genero) REFERENCES Genero(tconst),
    FOREIGN KEY (chave_de_tempo) REFERENCES Quando(chave_de_tempo),
    FOREIGN KEY (chave_da_sala) REFERENCES Sala_de_Cinema(chave_da_sala),
    FOREIGN KEY (chave_de_ator) REFERENCES Ator(Chave_de_Ator),
    FOREIGN KEY (chave_de_diretor) REFERENCES Diretor(Chave_de_Diretor)

);

select * from Relacionamento_Filmes;


INSERT INTO Relacionamento_Filmes (
    id_cidade,
    id_genero,
    chave_de_tempo,
    chave_da_sala,
    chave_de_ator,
    chave_de_diretor,
    titulo_original,
    cnpj_distribuidora,
    publico
)
SELECT 
    c.id_cidade,
    g.tconst,
    t.chave_de_tempo,
    s.chave_da_sala,
    a.Chave_de_Ator,
    d.Chave_de_Diretor,
    e.titulo_original,
    e.cnpj_distribuidora,  -- A partir de Exibicao_de_Filmes
    e.publico  -- A partir de Exibicao_de_Filmes
FROM 
    Exibicao_de_Filmes e
    JOIN Cidade c ON e.titulo_original = c.titulo_original
    JOIN Genero g ON e.titulo_original = g.titulo_original
    JOIN Quando t ON e.titulo_original = t.titulo_original AND e.data_exibicao = t.data_exibicao
    JOIN Sala_de_Cinema s 
        ON e.nome_da_sala = s.nome_da_sala
        AND e.cnpj_distribuidora = s.cnpj_distribuidora  -- Verificação de CNPJ correspondente
    JOIN Ator a ON g.tconst = a.tconst
    JOIN Diretor d ON g.tconst = d.tconst
WHERE 
    e.cnpj_distribuidora = s.cnpj_distribuidora  -- Verificação final de CNPJ
    AND e.publico IS NOT NULL  -- Certificar que o público não está vazio
    AND e.cnpj_distribuidora IS NOT NULL;  -- Certificar que o CNPJ não está vazio


INSERT INTO Relacionamento_Filmes (
    id_cidade,
    id_genero,
    chave_de_tempo,
    chave_da_sala,
    chave_de_ator,
    chave_de_diretor,
    titulo_original,
    cnpj_distribuidora,
    publico
)
SELECT 
    c.id_cidade,
    g.tconst,
    t.chave_de_tempo,
    s.chave_da_sala,
    a.Chave_de_Ator,
    d.Chave_de_Diretor,
    e.titulo_original,
    COALESCE(e.cnpj_distribuidora, s.cnpj_distribuidora),  -- Tenta usar CNPJ de ambas as tabelas
    e.publico
FROM 
    Exibicao_de_Filmes e
    LEFT JOIN Cidade c ON e.titulo_original = c.titulo_original
    LEFT JOIN Genero g ON e.titulo_original = g.titulo_original
    LEFT JOIN Quando t ON e.titulo_original = t.titulo_original AND e.data_exibicao = t.data_exibicao
    LEFT JOIN Sala_de_Cinema s 
        ON e.nome_da_sala = s.nome_da_sala
        AND (e.cnpj_distribuidora = s.cnpj_distribuidora OR e.cnpj_distribuidora IS NULL)
    LEFT JOIN Ator a ON g.tconst = a.tconst
    LEFT JOIN Diretor d ON g.tconst = d.tconst
WHERE 
    e.publico IS NOT NULL;



	-- Verifique o nome lógico do log (pode ser diferente do exemplo abaixo)
DBCC SQLPERF(LOGSPACE); 

-- Mude o banco para o modo SIMPLE para liberar os logs
ALTER DATABASE Data_Mart SET RECOVERY SIMPLE;

-- Trunca o log de transações
DBCC SHRINKFILE (Data_Mart_log, 1);  -- Nome do arquivo lógico de log

-- Volte o banco para o modo FULL se for necessário para futuras transações
ALTER DATABASE Data_Mart SET RECOVERY FULL;

DBCC FREEPROCCACHE;
DBCC DROPCLEANBUFFERS;

EXEC sp_MSforeachtable @command1="EXEC sp_spaceused '?'";


EXEC sp_readerrorlog;

USE Data_Mart; -- Substitua pelo nome do seu banco de dados
GO

-- Consulta para ver os arquivos de dados e log do banco
SELECT name AS NomeLogico, physical_name AS CaminhoFisico, type_desc AS Tipo
FROM sys.master_files
WHERE database_id = DB_ID(N'Data_Mart');