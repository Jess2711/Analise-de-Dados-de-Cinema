USE [Data_Mart]
GO
/****** Object:  View [dbo].[vw_Relacionamento_Filmes]    Script Date: 24/09/2024 15:30:34 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE VIEW [dbo].[vw_Relacionamento_Filmes] AS
WITH CTE_Final AS (
    SELECT 
        c.id_cidade,
        s.chave_da_sala,
        e.titulo_original,
        e.cnpj_distribuidora,
        e.publico,
        g.tconst AS id_genero,
        t.chave_de_Tempo,
        a.Chave_de_Ator,
        d.Chave_de_Diretor
    FROM Exibicao_de_Filmes e
    JOIN Cidade c ON LTRIM(RTRIM(LOWER(e.titulo_original))) = LTRIM(RTRIM(LOWER(c.titulo_original)))
    JOIN Sala_de_Cinema s ON e.titulo_original = s.titulo_original
        AND e.nome_da_sala = s.nome_da_sala
        AND e.cnpj_distribuidora = s.cnpj_distribuidora
    JOIN Genero g ON LTRIM(RTRIM(LOWER(e.titulo_original))) = LTRIM(RTRIM(LOWER(g.titulo_original)))
    JOIN Quando t ON e.titulo_original = t.titulo_original
        AND e.data_exibicao = t.data_exibicao
    LEFT JOIN Ator a ON g.tconst = a.tconst
    LEFT JOIN Diretor d ON g.tconst = d.tconst
    WHERE a.tconst IS NOT NULL 
    AND d.tconst IS NOT NULL
)
SELECT * FROM CTE_Final;
GO
