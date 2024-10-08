USE [Data_Mart]
GO
/****** Object:  Table [dbo].[Quando]    Script Date: 24/09/2024 15:30:34 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Quando](
	[chave_de_Tempo] [int] IDENTITY(1,1) NOT NULL,
	[data_exibicao] [date] NOT NULL,
	[titulo_original] [nvarchar](255) NOT NULL,
	[trimestre] [int] NOT NULL,
	[periodo] [nvarchar](50) NOT NULL,
PRIMARY KEY CLUSTERED 
(
	[chave_de_Tempo] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
