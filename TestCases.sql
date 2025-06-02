USE [master]
GO

ALTER DATABASE [AdventureWorks2019] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
RESTORE DATABASE [AdventureWorks2019]
FROM DISK = N'C:\MSSQL\Backup\AdventureWorks2019.bak'
WITH FILE = 1
   , NOUNLOAD
   , REPLACE
   , STATS = 1;
ALTER DATABASE [AdventureWorks2019] SET MULTI_USER;
GO

USE [master];
ALTER DATABASE [AdventureWorksDW2019] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
RESTORE DATABASE [AdventureWorksDW2019]
FROM DISK = N'C:\MSSQL\Backup\AdventureWorksDW2019.bak'
WITH FILE = 1
   , NOUNLOAD
   , REPLACE
   , STATS = 1;
ALTER DATABASE [AdventureWorksDW2019] SET MULTI_USER;
GO


USE [AdventureWorks2019];
GO

DECLARE @SchemaNames         NVARCHAR(MAX) = N'Sales
                                              ,Production'           
      , @TableNames          NVARCHAR(MAX) = N'SalesOrderDetail
                                              ,Product
                                              ,SalesOrderHeader
                                              ,Document' 

EXEC [dbo].[sp_ForceTruncate] @SchemaNames = @SchemaNames
                            , @TableNames = @TableNames
GO


                                                                                                                     
/* Truncating all tables over 10 records EXCEPT FOR all tables in HumanResources schema: */
USE [AdventureWorks2019]
GO

EXEC [dbo].[sp_ForceTruncate] 
                                     @TruncateAllTablesPerDB = 1
                                   , @SchemaNamesExpt = 'HumanResources'
                                   , @TableNamesExpt = '*'
                                   , @RowCountThreshold = 10
                                   , @WhatIf = 1
GO

/* Truncating all tables in all schemas matching name patterns: N'*Product*, *Address, *Tax*, Employee*' 
   except for Table names matching pattern: '*History': */
USE [AdventureWorks2019]
GO

DECLARE 
        @SchemaNames     NVARCHAR(MAX) = N'*'
      , @TableNames      NVARCHAR(MAX) = N'*Product*, *Address, *Tax*, Employee*'      
      , @SchemaNamesExpt NVARCHAR(MAX) = N'*'
      , @TableNamesExpt  NVARCHAR(MAX) = N'*History'

EXEC [dbo].[sp_ForceTruncate] @SchemaNames = @SchemaNames
                            , @TableNames = @TableNames
                            , @SchemaNamesExpt = @SchemaNamesExpt
                            , @TableNamesExpt = @TableNamesExpt
                            , @WhatIf = 1
GO

                                  
/* Truncating all tables over 1000 records EXCEPT FOR all tables with 'Dim' in the table name: */
USE [AdventureWorksDW2019]
GO

EXEC [dbo].[sp_ForceTruncate] 
                                     @TruncateAllTablesPerDB = 1
                                   , @SchemaNamesExpt = '*'
                                   , @TableNamesExpt = 'Dim*'
                                   , @RowCountThreshold = 1000
                                   , @WhatIf = 1