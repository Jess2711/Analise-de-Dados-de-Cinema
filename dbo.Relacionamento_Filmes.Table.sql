USE [Data_Mart]
GO
/****** Object:  Table [dbo].[Relacionamento_Filmes]    Script Date: 24/09/2024 15:30:34 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Relacionamento_Filmes](
	[id_relacionamento] [int] IDENTITY(1,1) NOT NULL,
	[id_cidade] [int] NOT NULL,
	[id_genero] [varchar](50) NOT NULL,
	[chave_de_tempo] [int] NOT NULL,
	[chave_da_sala] [int] NOT NULL,
	[chave_de_ator] [varchar](20) NOT NULL,
	[chave_de_diretor] [varchar](20) NOT NULL,
	[titulo_original] [nvarchar](255) NOT NULL,
	[cnpj_distribuidora] [float] NULL,
	[publico] [int] NULL,
PRIMARY KEY CLUSTERED 
(
	[id_relacionamento] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
ALTER TABLE [dbo].[Relacionamento_Filmes]  WITH NOCHECK ADD FOREIGN KEY([chave_de_tempo])
REFERENCES [dbo].[Quando] ([chave_de_Tempo])
GO
ALTER TABLE [dbo].[Relacionamento_Filmes]  WITH NOCHECK ADD FOREIGN KEY([chave_da_sala])
REFERENCES [dbo].[Sala_de_Cinema] ([chave_da_sala])
GO
ALTER TABLE [dbo].[Relacionamento_Filmes]  WITH NOCHECK ADD FOREIGN KEY([chave_de_ator])
REFERENCES [dbo].[Ator] ([Chave_de_Ator])
GO
ALTER TABLE [dbo].[Relacionamento_Filmes]  WITH NOCHECK ADD FOREIGN KEY([chave_de_diretor])
REFERENCES [dbo].[Diretor] ([Chave_de_Diretor])
GO
ALTER TABLE [dbo].[Relacionamento_Filmes]  WITH NOCHECK ADD FOREIGN KEY([id_cidade])
REFERENCES [dbo].[Cidade] ([id_cidade])
GO
ALTER TABLE [dbo].[Relacionamento_Filmes]  WITH NOCHECK ADD FOREIGN KEY([id_genero])
REFERENCES [dbo].[Genero] ([tconst])
GO
