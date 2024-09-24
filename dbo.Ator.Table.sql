USE [Data_Mart]
GO
/****** Object:  Table [dbo].[Ator]    Script Date: 24/09/2024 15:30:34 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Ator](
	[Chave_de_Ator] [varchar](20) NOT NULL,
	[Nome_do_Ator] [varchar](255) NULL,
	[tconst] [varchar](255) NULL,
PRIMARY KEY CLUSTERED 
(
	[Chave_de_Ator] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
