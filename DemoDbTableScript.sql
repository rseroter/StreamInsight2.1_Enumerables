USE [DemoDb]
GO
/****** Object:  Table [dbo].[ServerEvent]    Script Date: 08/02/2012 21:21:57 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [dbo].[ServerEvent](
	[Id] [int] NOT NULL,
	[ServerName] [varchar](50) NULL,
	[Level] [varchar](50) NULL,
	[Timestamp] [datetime] NULL,
 CONSTRAINT [PK_ServerEvent] PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
SET ANSI_PADDING OFF
GO
