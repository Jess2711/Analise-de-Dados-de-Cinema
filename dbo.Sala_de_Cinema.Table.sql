USE [Data_Mart]
GO
/****** Object:  Table [dbo].[Sala_de_Cinema]    Script Date: 24/09/2024 15:30:34 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Sala_de_Cinema](
	[chave_da_sala] [int] NOT NULL,
	[nome_da_sala] [varchar](255) NULL,
	[cnpj_distribuidora] [float] NULL,
	[titulo_original] [nvarchar](255) NULL,
	[nome_cidade] [varchar](255) NULL,
PRIMARY KEY CLUSTERED 
(
	[chave_da_sala] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
