CREATE SCHEMA [OrderContext]
GO

CREATE TABLE [dbo].[Order](
	[OrderId] [uniqueidentifier] NOT NULL,
	[OrderNumber] int NOT NULL,
	[PlacedAtDate] [datetime] NOT NULL,
 CONSTRAINT [PK_OrderContext.Order] PRIMARY KEY CLUSTERED 
(
	[OrderId] ASC
))



