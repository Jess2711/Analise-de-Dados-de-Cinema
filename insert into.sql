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

-- Select funcionando
SELECT top 100
    c.id_cidade,
    g.tconst,
    t.chave_de_tempo,
    s.chave_da_sala,
    a.Chave_de_Ator,
    d.Chave_de_Diretor,
    e.titulo_original,
    COALESCE(e.cnpj_distribuidora, s.cnpj_distribuidora),  
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


	DBCC FREEPROCCACHE;
	DBCC DROPCLEANBUFFERS;

	EXEC sp_MSforeachtable @command1="EXEC sp_spaceused '?'";

	EXEC sp_readerrorlog;

	USE Data_Mart; 
GO

-- Consulta para ver os arquivos de dados e log do banco
SELECT name AS NomeLogico, physical_name AS CaminhoFisico, type_desc AS Tipo
FROM sys.master_files
WHERE database_id = DB_ID(N'Data_Mart');

-- Verifica o nome l�gico do log (pode ser diferente do exemplo abaixo)
DBCC SQLPERF(LOGSPACE); 

-- Muda o banco para o modo SIMPLE para liberar os logs
ALTER DATABASE Data_Mart SET RECOVERY SIMPLE;

-- Trunca o log de transa��es
DBCC SHRINKFILE (Data_Mart_log, 1);  -- Nome do arquivo l�gico de log

-- Volta o banco para o modo FULL se for necess�rio para futuras transa��es
ALTER DATABASE Data_Mart SET RECOVERY FULL;


use tempdb;

go

exec sp_spaceused;

USE [tempdb];
GO

-- Definir o tamanho inicial e o crescimento autom�tico
ALTER DATABASE [tempdb]
MODIFY FILE (NAME = 'tempdev', SIZE = 500MB, FILEGROWTH = 100MB); -- Para o arquivo de dados
ALTER DATABASE [tempdb]
MODIFY FILE (NAME = 'templog', SIZE = 500MB, FILEGROWTH = 100MB); -- Para o log de transa��es

select * from Ator;

select * from Relacionamento_Filmes;



-- Vari�veis para controle do tamanho do lote e offset inicial
DECLARE @BatchSize INT = 1000;  -- Defina o tamanho do lote (ajuste conforme necess�rio)
DECLARE @Offset INT = 0;        -- Inicialize o offset para controle da inser��o em lotes

-- Loop para inser��o em lotes
WHILE 1 = 1
BEGIN
    -- Inser��o na tabela de relacionamento com verifica��es e filtragens adequadas
    INSERT INTO Relacionamento_Filmes 
    (
        id_relacionamento, 
        id_cidade, 
        chave_de_Tempo, 
        chave_da_sala, 
        chave_de_Ator, 
        chave_de_Diretor, 
        publico, 
        cnpj_distribuidora, 
        titulo_original
    )
    SELECT TOP (@BatchSize)
        e.id_exibicao, 
        c.id_cidade, 
        t.chave_de_Tempo, 
        s.chave_da_sala, 
        a.Chave_de_Ator, 
        d.Chave_de_Diretor, 
        e.publico, 
        s.cnpj_distribuidora, 
        e.titulo_original
    FROM Exibicao_de_Filmes e
    -- Combina��es para garantir que a cidade, exibi��o e sala estejam corretas
    JOIN Cidade c ON e.titulo_original = c.titulo_original
    JOIN Quando t ON e.data_exibicao = t.data_exibicao AND e.titulo_original = t.titulo_original
    JOIN Sala_de_Cinema s 
        ON e.titulo_original = s.titulo_original 
        AND e.nome_da_sala = s.nome_da_sala 
        AND e.cnpj_distribuidora = s.cnpj_distribuidora
    -- Combina��o do tconst para garantir o ator e o diretor corretos com base no g�nero
    JOIN Genero g ON e.titulo_original = g.titulo_original
    LEFT JOIN Ator a ON g.tconst = a.tconst
    LEFT JOIN Diretor d ON g.tconst = d.tconst
    -- Condi��o para garantir que somente inser��es v�lidas sejam feitas
    WHERE a.Chave_de_Ator IS NOT NULL 
      AND d.Chave_de_Diretor IS NOT NULL
      AND e.titulo_original = g.titulo_original  -- Garante correspond�ncia pelo t�tulo
    -- Inser��o em lotes baseada no offset e no batch size
    ORDER BY e.id_exibicao
    OFFSET @Offset ROWS;

    -- Atualiza o offset para o pr�ximo lote
    SET @Offset = @Offset + @BatchSize;

    -- Verifica se o n�mero de registros inseridos foi menor que o tamanho do lote
    -- Se sim, interrompe o loop pois todos os registros foram processados
    IF @@ROWCOUNT < @BatchSize
        BREAK;
END;

-- Informar conclus�o
PRINT 'Inser��o conclu�da com sucesso!';



SELECT TOP 10
    c.id_cidade,
    g.tconst AS id_genero,
    t.chave_de_tempo,
    s.chave_da_sala,
    a.Chave_de_Ator,
    d.Chave_de_Diretor,
    e.titulo_original,
    COALESCE(e.cnpj_distribuidora, s.cnpj_distribuidora) AS cnpj_distribuidora,  
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
    e.publico IS NOT NULL
    AND a.Chave_de_Ator IS NOT NULL   
    AND d.Chave_de_Diretor IS NOT NULL  
    AND g.tconst IS NOT NULL  
    AND c.id_cidade IS NOT NULL  
    AND t.chave_de_tempo IS NOT NULL  
    AND s.chave_da_sala IS NOT NULL;  