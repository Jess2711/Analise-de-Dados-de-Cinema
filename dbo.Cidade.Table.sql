USE [Data_Mart]
GO
/****** Object:  Table [dbo].[Cidade]    Script Date: 24/09/2024 15:30:34 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Cidade](
	[id_cidade] [int] NOT NULL,
	[nome_cidade] [varchar](255) NOT NULL,
	[nome_estado] [char](2) NOT NULL,
	[nome_regiao] [varchar](50) NOT NULL,
	[titulo_original] [nvarchar](255) NULL,
PRIMARY KEY CLUSTERED 
(
	[id_cidade] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
