

select * from Quando

select * from Cidade;

select * from Sala_de_Cinema;

select * from Ator;

select * from Diretor;

select * from Exibicao_de_Filmes;

DBCC CHECKIDENT ('Exibicao_de_Filmes', RESEED, 0);

delete from Exibicao_de_Filmes;


select * from Genero;

ALTER TABLE Exibicao_de_Filmes
ALTER COLUMN cnpj_distribuidora FLOAT;



SELECT COUNT(*) 
FROM Ator 
WHERE tconst IS NOT NULL;

SELECT COUNT(*) 
FROM Diretor
WHERE tconst IS NULL;

CREATE TABLE Exibicao_de_Filmes (
    chave_exibicao INT PRIMARY KEY IDENTITY(1,1),
    publico INT NOT NULL,
    chave_de_tempo INT,
    chave_da_sala INT,
    id_cidade INT,
    chave_de_ator VARCHAR(20),
    chave_de_diretor VARCHAR(20),
    tconst VARCHAR(50), -- FK para a tabela Gênero
    titulo_original VARCHAR(255),
    cnpj_distribuidora FLOAT,
    
    -- Definindo as chaves estrangeiras
    FOREIGN KEY (chave_de_tempo) REFERENCES Quando(chave_de_tempo),
    FOREIGN KEY (chave_da_sala) REFERENCES Sala_de_Cinema(chave_da_sala),
    FOREIGN KEY (id_cidade) REFERENCES Cidade(id_cidade),
    FOREIGN KEY (chave_de_ator) REFERENCES Ator(Chave_de_Ator),
    FOREIGN KEY (chave_de_Diretor) REFERENCES Diretor(Chave_de_Diretor),
    FOREIGN KEY (tconst) REFERENCES Genero(tconst) -- FK para a tabela Genero

);

drop table Exibicao_de_Filmes;

	SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'Exibicao_de_Filmes' AND COLUMN_NAME = 'publico';


	SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'Sala_de_Cinema' AND COLUMN_NAME = 'cnpj_distribuidora';




ALTER TABLE Exibicao_de_Filmes ALTER COLUMN titulo_original NVARCHAR(255);
ALTER TABLE Genero ALTER COLUMN titulo_original NVARCHAR(255);
ALTER TABLE Sala_de_Cinema ALTER COLUMN titulo_original NVARCHAR(255);
ALTER TABLE Cidade ALTER COLUMN titulo_original NVARCHAR(255);


select * from Cidade;


SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'Genero' AND COLUMN_NAME = 'titulo_original';


CREATE TABLE Exibicao_de_Filmes (
    id_exibicao INT IDENTITY(1,1) PRIMARY KEY,  -- Chave primária incremental
    data_exibicao DATE,                         -- Data de exibição
    titulo_original NVARCHAR(255),              -- Título original do filme
    nome_da_sala NVARCHAR(255),                 -- Nome da sala
    publico INT,                                -- Número de público presente
    cnpj_distribuidora NVARCHAR(14) NULL        -- CNPJ da distribuidora (opcional, se existir no CSV)
);