USE [Data_Mart]
GO
/****** Object:  Table [dbo].[Exibicao_de_Filmes]    Script Date: 24/09/2024 15:30:34 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Exibicao_de_Filmes](
	[id_exibicao] [int] IDENTITY(1,1) NOT NULL,
	[data_exibicao] [date] NULL,
	[titulo_original] [nvarchar](255) NULL,
	[nome_da_sala] [nvarchar](255) NULL,
	[publico] [int] NULL,
	[cnpj_distribuidora] [float] NULL,
PRIMARY KEY CLUSTERED 
(
	[id_exibicao] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
