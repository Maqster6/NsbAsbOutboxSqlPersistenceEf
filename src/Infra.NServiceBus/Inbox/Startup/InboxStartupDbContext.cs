using System;
using System.Configuration;
using System.Data.Entity;
using System.Data.SqlClient;
using System.Data.Common;
using System.Data;

namespace Infra.NServiceBus.Inbox.Startup
{
    public class InboxStartupDbContext : DbContext
    {
        private const string createSchemaSql = @"IF NOT EXISTS (
                                                    SELECT  schema_name
                                                    FROM    information_schema.schemata
                                                    WHERE   schema_name = '{0}') 

                                                    BEGIN
                                                    EXEC sp_executesql N'CREATE SCHEMA {0}'
                                                    END";

        private const string createInboxRecordSql = @"IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[{0}].[{1}InboxRecord]') AND type in (N'U'))
                                                    BEGIN
                                                     CREATE TABLE [{0}].[{1}InboxRecord](
	                                                    [Id] [int] IDENTITY(1,1) NOT NULL,
	                                                    [ContentId] [nvarchar](100) NULL,
	                                                    [ContentVersion] [bigint] NOT NULL,
	                                                    [CheckMessageOrderType] [nvarchar](300) NULL,
	                                                    [MessageId] [uniqueidentifier] NOT NULL,
	                                                    [ModifiedAtUtc] [datetime] NOT NULL,
                                                        CONSTRAINT [PK_{0}.{1}InboxRecord] PRIMARY KEY CLUSTERED 
                                                    (
	                                                    [Id] ASC
                                                    )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
                                                    ) ON [PRIMARY]
                                                    END";


        private const string createInboxRecordIndexSql = @"IF NOT EXISTS (SELECT * FROM sys.indexes 
			                                                              WHERE name='UIX_{1}InboxRecord_ContentId_CheckMessageOrderType' AND object_id = OBJECT_ID('[{0}].[{1}InboxRecord]'))
                                                         BEGIN
															CREATE UNIQUE NONCLUSTERED INDEX UIX_{1}InboxRecord_ContentId_CheckMessageOrderType
															ON [{0}].[{1}InboxRecord] (ContentId, CheckMessageOrderType)
                                                         END";

        private const string createInboxRecordIndexModifiedAtUtcSql = @"IF NOT EXISTS (SELECT * FROM sys.indexes 
			                                                              WHERE name='UIX_{1}InboxRecord_All' AND object_id = OBJECT_ID('[{0}].[{1}InboxRecord]'))
                                                         BEGIN
															CREATE NONCLUSTERED INDEX [UIX_{1}InboxRecord_All] ON [{0}].[{1}InboxRecord] ([ModifiedAtUtc]) INCLUDE ([CheckMessageOrderType], [ContentId], [ContentVersion], [MessageId]) WITH (ONLINE = ON)
                                                         END";


        private const string createInboxDiscardedRecordSql = @"IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[{0}].[{1}InboxRecordDiscarded]') AND type in (N'U'))
                                                    BEGIN
                                                    CREATE TABLE [{0}].[{1}InboxRecordDiscarded](
	                                                    [Id] [int] IDENTITY(1,1) NOT NULL,
	                                                    [ContentId] [nvarchar](100) NULL,
	                                                    [ContentVersion] [bigint] NOT NULL,
	                                                    [LatestContentVersion] [bigint] NOT NULL,
	                                                    [CheckMessageOrderType] [nvarchar](300) NULL,
	                                                    [MessageId] [uniqueidentifier] NOT NULL,
	                                                    [ModifiedAtUtc] [datetime] NOT NULL,
                                                        CONSTRAINT [PK_{0}.{1}InboxRecordDiscarded] PRIMARY KEY CLUSTERED 
                                                    (
	                                                    [Id] ASC
                                                    )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
                                                    ) ON [PRIMARY]
                                                    END";
        private readonly string dbSchemaName;
        private readonly object dbTablePrefix;

        public InboxStartupDbContext() : base(Environment.GetEnvironmentVariable("AzureDb.ConnectionString"))
        {
            dbSchemaName = ConfigurationManager.AppSettings["DatabaseSchemaName"];
            dbTablePrefix = ConfigurationManager.AppSettings["EndpointTablePrefix"];
            TurnOffAutomaticDatabaseCreationAndSchemaUpdates();
        }

      
        public void RemoveEntriesOlderThan(DateTime dateTime)
        {
            var sql = $@"SET NOCOUNT ON;
            DECLARE @r INT;

            SET @r = 1;

            WHILE @r > 0
            BEGIN
            BEGIN TRANSACTION;

            DELETE TOP(1000)
            FROM [{dbSchemaName}].[{dbTablePrefix}InboxRecord] WHERE ModifiedAtUtc < @dateTime;

            SET @r = @@ROWCOUNT;
            Print @r

            COMMIT TRANSACTION;
            END";

            Database.ExecuteSqlCommand(sql, new SqlParameter("@dateTime", dateTime));
        }

        public void InitializeDatabase()
        {
            object[] parameters = { dbSchemaName, dbTablePrefix }; ;
            Database.ExecuteSqlCommand(string.Format(createSchemaSql, parameters));
            Database.ExecuteSqlCommand(string.Format(createInboxRecordSql, parameters));
            Database.ExecuteSqlCommand(string.Format(createInboxRecordIndexSql, parameters));
            Database.ExecuteSqlCommand(string.Format(createInboxRecordIndexModifiedAtUtcSql, parameters));
            Database.ExecuteSqlCommand(string.Format(createInboxDiscardedRecordSql, parameters));
        }

        private static void TurnOffAutomaticDatabaseCreationAndSchemaUpdates()
        {
            Database.SetInitializer<InboxStartupDbContext>(null);
        }
    }
}
