
SELECT COUNT(*) AS TotalLinhas
FROM Quando;

SELECT COUNT(*) AS TotalLinhas
FROM Ator;

SELECT COUNT(*) AS TotalLinhas
FROM Diretor;

SELECT COUNT(*) AS TotalLinhas
FROM Exibicao_de_Filmes;

SELECT COUNT(*) AS TotalLinhas
FROM Genero;

SELECT COUNT(*) AS TotalLinhas
FROM Sala_de_Cinema;


select * from Quando;
select * from Ator;
select * from Diretor;
select * from Exibicao_de_Filmes;
select * from Genero;
select * from Sala_de_Cinema;
select * from Sala_de_Cinema;

SELECT * 
FROM vw_Relacionamento_Filmes
ORDER BY id_cidade 
OFFSET 0 ROWS FETCH NEXT 100 ROWS ONLY;
