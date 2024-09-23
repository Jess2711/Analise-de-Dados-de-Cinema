USE Data_Mart

CREATE TABLE Ator (
	nconst VARCHAR(20) PRIMARY KEY,
	primaryName VARCHAR(255)
);

CREATE TABLE Diretor (
	nconst VARCHAR(20) PRIMARY KEY,
	primaryName VARCHAR(255)

);

select * from Ator;

select * from Diretor;

ALTER TABLE Ator ADD primaryProfession VARCHAR(255);
ALTER TABLE Diretor ADD primaryProfession VARCHAR(255);

delete from Diretor;

delete from Ator;

