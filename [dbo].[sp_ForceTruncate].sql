USE [master]
GO

IF (CAST(SUBSTRING(CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(20)), 1, 2) AS INT) < 14)
BEGIN
    RAISERROR('You can only install/run this sp on SQL Versions older than 14 (2017) if you modify the code in all sections where @DbEngineVersion is used', 18, 1)
END
GO

IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_NAME = 'sp_ForceTruncate')
    EXEC ('CREATE PROC dbo.sp_ForceTruncate AS SELECT ''stub version, to be replaced''')
GO

EXEC [sys].[sp_MS_marksystemobject] '[dbo].[sp_ForceTruncate]';
GO

ALTER PROCEDURE [dbo].[sp_ForceTruncate]

/* ==================================================================================================================== */
/* Author:      CleanSql.com © Copyright CleanSql.com                                                                   */
/* Create date: 2024-11-21                                                                                              */
/* Description: Truncates all tables specified by input parameters: @SchemaNames/@TableNames having first dropped       */
/*              or disabled all limiting dependencies; after truncate the sp recreates all dropped dependencies         */
/*              (based on their config/definitions saved previously into temp tables for that purpose).                 */
/*              If any truncate fails the sp rolls back the entire transaction, logs errors into temp tables            */
/*              If any recreate fails the sp rolls back the entire operation, unless @ContinueOnError = 1 is used,      */
/*              in which case the sp will ignore all recreate errors (logging them in temp tables), commit all truncate */
/*              operations as well as successfull recreate ops.                                                         */
/* ==================================================================================================================== */
/* Change History:                                                                                                      */
/* -------------------------------------------------------------------------------------------------------------------- */
/* Date:       Version:  Change:                                                                                        */
/* -------------------------------------------------------------------------------------------------------------------- */
/* 2024-11-21  1.00      Created                                                                                        */
/* 2024-12-06  1.01      added @SchemaName/@TableName validation                                                        */
/*                       allowed new-lines in input params: @SchemaNames/@TableNames if present                         */
/*                       using sys.tables for @TruncateAllTablesPerDB instead of INFORMATION_SCHEMA                     */
/* 2024-12-12  1.02      improved @TableNames validation and error handling,                                            */
/*                       added support for [#IndexesOnSchemaBoundViews], encrypted SchBv error-handling                 */
/*                       and for [#TriggerssOnSchemaBoundViews] + encrypted Trgr error-handling                         */
/* 2024-12-12  1.03      fixed bug: missing schema in ON clause populating [#IndexesOnSchemaBoundViews]                 */
/* 2024-12-14  1.04      resetting @ErrorMessage = NULL after error logging in recreate section                         */
/* 2024-12-17  1.05      added exception lists (@SchemaNamesExpt/@TableNamesExpt) for @SchemaNames/@TableNames          */
/* 2025-05-22  1.06      fixed bug with incorrect processing when @TruncateAllTablesPerDB = NULL                        */
/* -------------------------------------------------------------------------------------------------------------------- */
/* ==================================================================================================================== */
/* Example use:

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

                                                                                                                     
Truncating all tables over 10 records EXCEPT FOR all tables in HumanResources schema:
EXEC [dbo].[sp_ForceTruncate] 
                                     @TruncateAllTablesPerDB = 1
                                   , @SchemaNamesExpt = 'HumanResources'
                                   , @TableNamesExpt = '*'
                                   , @RowCountThreshold = 10
                                   , @WhatIf = 1
                                  
Truncating all tables over 1000 records EXCEPT FOR all tables with 'Dim' in the table name:
USE [AdventureWorksDW2019]
GO

EXEC [dbo].[sp_ForceTruncate] 
                                     @TruncateAllTablesPerDB = 1
                                   , @SchemaNamesExpt = '*'
                                   , @TableNamesExpt = 'Dim'
                                   , @RowCountThreshold = 1000
                                   , @WhatIf = 1
*/
/*THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO    */
/*THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE      */
/*AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, */
/*TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE      */
/*SOFTWARE.                                                                                                           */
/*LICENSE: https://github.com/CleanSql-com/sp_ForceTruncate?tab=MIT-1-ov-file#readme                                  */
/* ===================================================================================================================*/

    /* Input parameters: */
    @SchemaNames                     NVARCHAR(MAX) = N''    /* for example: N'Sales' */
  , @TableNames                      NVARCHAR(MAX) = N''    /* for example: N'SalesOrderHeader,SalesOrderHeaderSalesReason,Customer,CreditCard,PersonCreditCard,CurrencyRate' */
  , @Delimiter                       CHAR(1)       = ','    /* character that was used to delimit the list of names above in @SchemaNames/@TableNames */
  , @WhatIf                          BIT           = 0      /* 1 = only printout commands to be executed, without running them */
  , @ContinueOnError                 BIT           = 0      /* Set to = 1 ONLY if you do not care about any errors encountered within recreate block(s) 
                                                               !!! BE CAREFULL - truncation of selected tables may be at the expense of losing metadata definition; 
                                                               if set to = 1 leave @SchemaNames and @TableNames empty (do not specify any values) */
  , @TruncateAllTablesPerDB          BIT           = 0      /* Set @TruncateAllTablesPerDB to = 1 ONLY if you want to ignore the @SchemaNames/@TableNames 
                                                               specified above and truncate ALL TABLES IN THE ENTIRE DB !!! USE CAREFULLLY */
  , @RowCountThreshold               BIGINT        = 0      /* Truncate only tables with rowcount >= @RowCountThreshold  
                                                               this parameter works independently of @TruncateAllTablesPerDB
                                                            */
  , @SchemaNamesExpt                 NVARCHAR(MAX) = NULL
  , @TableNamesExpt                  NVARCHAR(MAX) = NULL
  , @ExceptionListWildcard           CHAR(1)       = '*'
  
  , @BatchSize                       INT           = 10
  , @ReenableCDC                     BIT           = 1
  , @RecreatePublishedArticles       BIT           = 1

AS
BEGIN
SET NOCOUNT ON;
SET XACT_ABORT ON;

DECLARE

/* ==================================================================================================================== */
/* ----------------------------------------- VARIABLE AND TEMP TABLE DECLARATIONS: ------------------------------------ */
/* ==================================================================================================================== */

  /* Internal parameters: */
    @SpCurrentVersion                CHAR(5) = '1.06'
  , @ObjectId                        BIGINT
  , @SchemaId                        INT
  , @StartSearchSch                  INT
  , @DelimiterPosSch                 INT
  , @SchemaName                      SYSNAME
  , @TableName                       SYSNAME
  , @TemporalType                    TINYINT
  , @HistoryTblName                  SYSNAME
  , @SchBvName                       SYSNAME
  , @StartSearchTbl                  INT
  , @DelimiterPosTbl                 INT
  , @DbEngineVersion                 INT
  , @Id                              INT
  , @IdMax                           INT
  , @PercentProcessed                INT           = 0
  , @IsDbCDCEnabled                  BIT

  /* Trigger parsing variables: */
  , @TriggerId           INT
  , @TriggerName         SYSNAME
  , @TriggerDefinition   NVARCHAR(MAX)
  , @PointerString       INT
  , @PointerNewLine      INT
  , @LineOfCode          NVARCHAR(MAX)
  , @LineOfCodeId        INT
  , @LineOfCodeIdMax     INT

  /* error handling variables: */
  , @ErrorMessage                    NVARCHAR(MAX)
  , @ErrorSeverity11                 INT           = 11     /* 11 changes the message color to red */
  , @ErrorSeverity18                 INT           = 18     /* 16 and below does not break execution */
  , @ErrorState                      INT

  
  /* dynamic sql variables: */
  , @SqlSchemaId                     NVARCHAR(MAX)
  , @SqlObjectId                     NVARCHAR(MAX)
  , @SqlDropConstraint               NVARCHAR(MAX)
  , @SqlDropView                     NVARCHAR(MAX)
  , @SqlTriggerDefinition            NVARCHAR(MAX)
  , @SqlTruncateTable                NVARCHAR(MAX)
  , @SqlUpdateStatistics             NVARCHAR(MAX)
  , @SqlRecreateConstraint           NVARCHAR(MAX)
  , @SqlRecreateView                 NVARCHAR(MAX)
  , @SqlXtndProperties               NVARCHAR(MAX)
  , @SqlRecreateIdxOnSchBv           NVARCHAR(MAX)
  , @SqlRecreateTrgOnSchBv           NVARCHAR(MAX)
  , @SqlReenableCDCInstance          NVARCHAR(MAX)
  , @SqlTableCounts                  NVARCHAR(MAX)
  , @SqlSetIsTruncated               NVARCHAR(MAX)
  , @SqlRecreatePublishedArticle     NVARCHAR(MAX)
  , @SqlLogError                     NVARCHAR(MAX)
  
  , @IsTruncated                     BIT
  , @IsEncrypted                     BIT
  , @ParamDefinition                 NVARCHAR(4000)

  , @CountTablesSelected             INT           = 0
  , @CountFKFound                    INT           = 0
  , @CountFKDropped                  INT           = 0
  , @CountFKRecreated                INT           = 0
  , @CountSchBvFound                 INT           = 0
  , @CountSchBvDropped               INT           = 0
  , @CountSchBvRecreated             INT           = 0
  , @CountTblsCDCEnabled             INT           = 0
  , @CountPublishedTablesFound       INT           = 0
  , @CountPublishedArticlesFound     INT           = 0
  , @CountPublishedArticlesDropped   INT           = 0
  , @CountPublishedArticlesRecreated INT           = 0
  
  , @CountCDCInstFound               INT           = 0
  , @CountCDCInstDisabled            INT           = 0
  , @CountCDCInstReenabled           INT           = 0
  
  , @CountTablesTruncated            INT           = 0
  , @CountSchBvsReferencedObjectIds  INT           = 0
  , @CountIsReferencedByFk           INT           = 0
  , @CountTblsReferencedBySchBvs     INT           = 0
  , @CountFKObjectIdTrgt             INT           = 0
  , @CountTblsReferencedByArticles   INT           = 0
  , @CountExceptionList              INT           = 0
  , @CountTemporalTbls               INT           = 0
  
  , @level0type                      VARCHAR(128)
  , @level0name                      SYSNAME
  , @level1type                      VARCHAR(128)
  , @level1name                      SYSNAME
  , @crlf                            CHAR(32)      = CONCAT(CHAR(13), CHAR(10))
  , @UnionAll                        VARCHAR(32)   = CONCAT(CHAR(10), 'UNION ALL', CHAR(10))

  /* CDC Instance definition variables: */
  , @CDC_source_schema               SYSNAME         
  , @CDC_source_name                 SYSNAME
  , @CDC_capture_instance            SYSNAME         
  
  /* Published Articles variables: */
  , @publication_id                  INT
  , @max_publication_id              INT      
  , @publication                     SYSNAME      
  , @article                         SYSNAME;      

PRINT(CONCAT('/* Current SP Version: ', @SpCurrentVersion, IIF(@WhatIf = 1, 'with @WhatIf = 1 - no actual changes will be made', ''), ' */'))

/* ==================================================================================================================== */
/* ----------------------------------------- VALIDATE INPUT PARAMETERS: ----------------------------------------------- */
/* ==================================================================================================================== */

SELECT @DbEngineVersion = CAST(SUBSTRING(CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(20)), 1, 2) AS INT);
IF (@DbEngineVersion < 14)
BEGIN
    SET @ErrorMessage = 'You can only install/run this sp on SQL Versions older than 14 (2017) if you modify the code in all sections where @DbEngineVersion is used'
    GOTO ERROR
END

IF (@TruncateAllTablesPerDB = 0 OR @TruncateAllTablesPerDB IS NULL) AND (LEN(@SchemaNames) = 0 OR LEN(@TableNames) = 0)
BEGIN
    SET @ErrorMessage = N'@SchemaNames AND @TableNames parameters can not be empty, unless you want to truncate ALL tables per DB by using @TruncateAllTablesPerDB = 1';
    GOTO ERROR;
END;

IF @TruncateAllTablesPerDB = 1 AND (LEN(@SchemaNames) > 0 OR LEN(@TableNames) > 0)
BEGIN
    SET @ErrorMessage = N'If you want to truncate ALL tables per DB by using @TruncateAllTablesPerDB = 1 then both @SchemaNames AND @TableNames must be empty.';
    GOTO ERROR;
END;

IF (LEN(@SchemaNamesExpt) > 0 AND LEN(@TableNamesExpt) = 0) OR (LEN(@SchemaNamesExpt) = 0 AND LEN(@TableNamesExpt) > 0)
BEGIN
    SET @ErrorMessage = N'If you want to add any exceptions then both @SchemaNamesExpt and @TableNamesExpt must contain a value';
    GOTO ERROR;
END;

IF ((CHARINDEX(@ExceptionListWildcard, @SchemaNamesExpt, 0) > 0 AND @ExceptionListWildcard <> @SchemaNamesExpt)
OR ( CHARINDEX(@ExceptionListWildcard, @TableNamesExpt,  0) > 0 AND @ExceptionListWildcard <> @TableNamesExpt))
BEGIN
    SET @ErrorMessage = CONCAT(N'You can not mix the @ExceptionListWildcard: ', @ExceptionListWildcard
                             , ' within @SchemaNamesExpt/@TableNamesExpt.
If your @SchemaNamesExpt/@TableNamesExpt contain: ', @ExceptionListWildcard, ' then specify your own character as @ExceptionListWildcard parameter.
Any value of @ExceptionListWildcard specified as @SchemaNamesExpt/@TableNamesExpt has to be used exclusively on its own');
    GOTO ERROR;
END;

/* remove new-line and append delimiter at the end of @SchemaNames/@TableNames if it is missing: */
SET @SchemaNames = REPLACE(@SchemaNames, @crlf, '')
SET @TableNames = REPLACE(@TableNames, @crlf, '')
IF  LEN(@SchemaNames) > 0 AND (RIGHT(@SchemaNames, 1)) <> @Delimiter SET @SchemaNames = CONCAT(@SchemaNames, @Delimiter);
IF  LEN(@TableNames) > 0 AND (RIGHT(@TableNames, 1)) <> @Delimiter SET @TableNames = CONCAT(@TableNames, @Delimiter);

SET @SchemaNamesExpt = REPLACE(@SchemaNamesExpt, @crlf, '')
SET @TableNamesExpt = REPLACE(@TableNamesExpt, @crlf, '')
IF  LEN(@SchemaNamesExpt) > 0 AND (RIGHT(@SchemaNamesExpt, 1)) <> @Delimiter SET @SchemaNamesExpt = CONCAT(@SchemaNamesExpt, @Delimiter);
IF  LEN(@TableNamesExpt) > 0 AND (RIGHT(@TableNamesExpt, 1)) <> @Delimiter SET @TableNamesExpt = CONCAT(@TableNamesExpt, @Delimiter);


/* ==================================================================================================================== */
/* ----------------------------------------- DEFINE TEMP TABLES: ------------------------------------------------------ */
/* ==================================================================================================================== */

CREATE TABLE [#SelectedTables]
(
    [Id]                    INT           NOT NULL PRIMARY KEY CLUSTERED IDENTITY(1, 1)
  , [SchemaID]              INT           NOT NULL
  , [ObjectID]              INT           NOT NULL UNIQUE
  , [SchemaName]            SYSNAME       NOT NULL
  , [TableName]             SYSNAME       NOT NULL
  , [IsReferencedByFk]      BIT           NULL
  , [IsReferencedBySchBv]   BIT           NULL
  , [IsCDCEnabled]          BIT           NULL
  , [IsPublished]           BIT           NULL
  , [TemporalType]          TINYINT       NULL
  , [HistoryTblObjectID]    INT           NULL

  , [NumFkReferencing]      INT           NULL
  , [NumFkDropped]          INT           NULL
  , [NumFkRecreated]        INT           NULL
  , [NumSchBvReferencing]   INT           NULL
  , [NumSchBvDropped]       INT           NULL
  , [NumSchBvRecreated]     INT           NULL
  
  , [NumCDCInstReferencing] INT           NULL
  , [NumCDCInstDisabled]    INT           NULL
  , [NumCDCInstReenabled]   INT           NULL  
  
  , [NumPublArtReferencing] INT           NULL
  , [NumPublArtDropped]     INT           NULL
  , [NumPublArtRecreated]   INT           NULL
  , [RowCountBefore]        BIGINT        NULL
  , [RowCountAfter]         BIGINT        NULL
  , [IsToBeTruncated]       BIT           NULL
  , [IsOnExceptionList]     BIT           NULL
  , [IsTruncated]           BIT           NULL
  , [ErrorMessage]          NVARCHAR(MAX) NULL
);

CREATE TABLE [#ExceptionList] ([SchemaNameExpt] SYSNAME NOT NULL, [TableNameExpt] SYSNAME NOT NULL);

CREATE TABLE [#ForeignKeyConstraintDefinitions]
(
    [Id]                        INT           NOT NULL PRIMARY KEY CLUSTERED IDENTITY(1, 1)
  , [ForeignKeyId]              INT           NOT NULL UNIQUE
  , [ForeignKeyName]            SYSNAME       NOT NULL
  , [ObjectIdTrgt]              INT           NOT NULL
  , [SchemaNameSrc]             SYSNAME       NOT NULL
  , [TableNameSrc]              SYSNAME       NOT NULL
  , [SchemaNameTrgt]            SYSNAME       NOT NULL
  , [TableNameTrgt]             SYSNAME       NOT NULL
  , [DropConstraintCommand]     NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL
  , [RecreateConstraintCommand] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL
  , [ErrorMessage]              NVARCHAR(MAX) NULL
);

CREATE TABLE [#TableRowCounts]
(
    [Id]        INT          NOT NULL PRIMARY KEY CLUSTERED IDENTITY(1, 1)
  , [ObjectID]  INT          NOT NULL UNIQUE 
  , [TableName] VARCHAR(256) NOT NULL
  , [RowCount]  BIGINT       NOT NULL
);

CREATE TABLE [#SchemaBoundViews]
(
    [Id]                      INT           NOT NULL PRIMARY KEY CLUSTERED IDENTITY(1, 1)
  , [SbvObjectId]             INT           NOT NULL UNIQUE
  , [ReferencingObjectSchema] NVARCHAR(128) NOT NULL
  , [ReferencingObjectName]   NVARCHAR(128) NOT NULL
  , [DropViewCommand]         NVARCHAR(MAX) NOT NULL
  , [RecreateViewCommand]     NVARCHAR(MAX) NULL
  , [IsEncrypted]             BIT           NOT NULL
  , [@level0type]             VARCHAR(128)  NULL
  , [@level0name]             SYSNAME       NULL
  , [@level1type]             VARCHAR(128)  NULL
  , [@level1name]             SYSNAME       NULL
  , [XtdProperties]           NVARCHAR(MAX) NULL
  , [Dropped]                 BIT           NULL
  , [Recreated]               BIT           NULL
  , [ErrorMessage]            NVARCHAR(MAX) NULL
);

CREATE TABLE [#SbvToSelTablesLink] ([SbvObjectId] INT NOT NULL, [ReferncedObjectId] INT NOT NULL);

CREATE TABLE [#IndexesOnSchemaBoundViews]
(
    [Id]                     INT           NOT NULL PRIMARY KEY CLUSTERED IDENTITY(1, 1)
  , [ReferencedViewObjectId] INT           NOT NULL
  , [IndexId]                INT           NOT NULL
  , [IsUnique]               VARCHAR(7)    NOT NULL
  , [IndexType]              VARCHAR(60)   NOT NULL
  , [IndexName]              SYSNAME       NOT NULL
  , [OnView]                 SYSNAME       NOT NULL
  , [ColumnNames]            NVARCHAR(MAX) NOT NULL
  , UNIQUE ([ReferencedViewObjectId], [IndexId])
);

CREATE TABLE [#TriggersOnSchemaBoundViews]
(
    [Id]                     INT           NOT NULL PRIMARY KEY CLUSTERED IDENTITY(1, 1)
  , [ReferencedViewObjectId] INT           NOT NULL
  , [TriggerId]              INT           NOT NULL
  , [TriggerName]            SYSNAME       NOT NULL
  , [IsEncrypted]            BIT           NOT NULL
  , [ErrorMessage]           NVARCHAR(MAX) NULL
  , UNIQUE ([TriggerId])
);

CREATE TABLE [#TriggerDefinitions]
(
    [TriggerId]  INT           NOT NULL
  , [LineId]     INT           IDENTITY(1, 1) NOT NULL
  , [LineOfCode] NVARCHAR(MAX) NOT NULL
  , PRIMARY KEY CLUSTERED ([TriggerId], [LineId])
);

CREATE TABLE [#CDCInstances]
(
    [Id]                     INT            NOT NULL PRIMARY KEY CLUSTERED IDENTITY(1, 1)
  , [CdcObjectId]            INT            NOT NULL UNIQUE
  , [ReferncedObjectId]      INT            NOT NULL
  , [source_schema]          SYSNAME        NOT NULL
  , [source_name]            SYSNAME        NOT NULL
  , [capture_instance]       SYSNAME        NOT NULL
  , [supports_net_changes]   BIT            NOT NULL
  , [role_name]              SYSNAME        NULL
  , [index_name]             SYSNAME        NULL
  , [captured_column_list]   NVARCHAR(4000) NULL
  , [filegroup_name]         SYSNAME        NULL
  , [allow_partition_switch] BIT            NOT NULL
  , [ErrorMessage]           NVARCHAR(MAX)  NULL
);

CREATE TABLE [#PublicationsArticles]
(
    [Id]                            INT           NOT NULL PRIMARY KEY CLUSTERED IDENTITY(1, 1)
  , [publication_id]                INT           NOT NULL  
  , [article_id]                    INT           NOT NULL UNIQUE
  , [publication]                   SYSNAME       NOT NULL
  , [article]                       SYSNAME       NOT NULL
  , [source_table]                  NVARCHAR(386) NOT NULL
  , [destination_table]             SYSNAME       NOT NULL
  , [vertical_partition]            NCHAR(5)      NOT NULL
  , [type]                          SYSNAME       NULL
  , [filter]                        NVARCHAR(386) NULL
  , [sync_object]                   NVARCHAR(386) NULL
  , [ins_cmd]                       NVARCHAR(255) NULL
  , [del_cmd]                       NVARCHAR(255) NULL
  , [upd_cmd]                       NVARCHAR(255) NULL
  , [creation_script]               NVARCHAR(255) NULL
  , [description]                   NVARCHAR(255) NULL
  , [pre_creation_cmd]              NVARCHAR(10)  NOT NULL
  , [filter_clause]                 NVARCHAR(MAX) NULL
  , [schema_option]                 VARBINARY(8)  NULL
  , [destination_owner]             SYSNAME       NULL
  , [status]                        TINYINT       NOT NULL
  , [source_owner]                  SYSNAME       NULL
  , [sync_object_owner]             SYSNAME       NULL
  , [filter_owner]                  SYSNAME       NULL
  , [source_object]                 SYSNAME       NULL
  , [auto_identity_range]           NVARCHAR(5)   NULL
  , [pub_identity_range]            BIGINT        NULL
  , [identity_range]                BIGINT        NULL
  , [threshold]                     INT           NULL
  , [force_invalidate_snapshot]     BIT           NOT NULL
  , [use_default_datatypes]         BIT           NOT NULL
  , [identityrangemanagementoption] NVARCHAR(10)  NULL
  , [publisher]                     SYSNAME       NULL
  , [fire_triggers_on_snapshot]     VARCHAR(5)    NOT NULL
  , [ReferncedObjectId]             BIGINT        NOT NULL
  , [ErrorMessage]                  NVARCHAR(MAX) NULL
);

CREATE TABLE [#sp_helparticle]
(
    [article id]                    INT           NOT NULL PRIMARY KEY CLUSTERED
  , [article name]                  NVARCHAR(128) NOT NULL
  , [base object]                   NVARCHAR(300) NOT NULL
  , [destination object]            NVARCHAR(128) NOT NULL
  , [synchronization object]        NVARCHAR(300) NOT NULL
  , [type]                          SMALLINT      NOT NULL
  , [status]                        INT           NOT NULL
  , [filter]                        NVARCHAR(386) NULL
  , [description]                   NVARCHAR(255) NULL
  , [insert_command]                NVARCHAR(255) NULL
  , [update_command]                NVARCHAR(255) NULL
  , [delete_command]                NVARCHAR(255) NULL
  , [creation script path]          NVARCHAR(255) NULL
  , [vertical partition]            BIT           NULL
  , [pre_creation_cmd]              TINYINT       NULL
  , [filter_clause]                 NVARCHAR(MAX) NULL
  , [schema_option]                 BINARY(8)     NULL
  , [dest_owner]                    SYSNAME       NULL
  , [source_owner]                  SYSNAME       NULL
  , [unqua_source_object]           SYSNAME       NULL
  , [sync_object_owner]             SYSNAME       NULL
  , [unqualified_sync_object]       SYSNAME       NULL
  , [filter_owner]                  SYSNAME       NULL
  , [unqua_filter]                  SYSNAME       NULL
  , [auto_identity_range]           BIT           NULL
  , [publisher_identity_range]      BIGINT        NULL
  , [identity_range]                BIGINT        NULL
  , [threshold]                     INT           NULL
  , [identityrangemanagementoption] INT           NULL
  , [fire_triggers_on_snapshot]     BIT           NULL
);

/* ==================================================================================================================== */
/* ----------------------------------------- COLLECTING METADATA: ----------------------------------------------------- */
/* ==================================================================================================================== */

IF (@WhatIf = 1 )
BEGIN
    PRINT(CONCAT('USE [', DB_NAME(), ']'));
    PRINT(CONCAT('GO', @crlf));
END;

PRINT ('/*--------------------------------------- COLLECTING [#SelectedTables]: ------------------------------------------*/');
SET @StartSearchSch = 0;
SET @DelimiterPosSch = 0;
IF (@TruncateAllTablesPerDB = 1)
BEGIN
    PRINT (CONCAT(
                     N'/* Specified @TruncateAllTablesPerDB = 1 so collecting list of all non-system tables in the database: '
                   , QUOTENAME(DB_NAME())
                   , ' */'
                 )
          );

    INSERT INTO [#SelectedTables] ([SchemaID], [ObjectID], [SchemaName], [TableName], [IsTruncated])
    SELECT [ss].[schema_id] AS [SchemaID]
         , [st].[object_id] AS [ObjectID]
         , [ss].[name] AS [SchemaName]
         , [st].[name] AS [TableName]
         , 0
    FROM sys.tables AS [st]
    JOIN sys.schemas AS [ss]
        ON [st].[schema_id] = [ss].[schema_id]
    WHERE [st].[is_ms_shipped] <> 1;
END
ELSE 
BEGIN
    WHILE CHARINDEX(@Delimiter, @SchemaNames, @StartSearchSch + 1) > 0
    BEGIN
        SET @DelimiterPosSch = CHARINDEX(@Delimiter, @SchemaNames, @StartSearchSch + 1) - @StartSearchSch;
        SET @SchemaName = TRIM(SUBSTRING(@SchemaNames, @StartSearchSch, @DelimiterPosSch));
        SET @SchemaId = NULL;

        SET @SqlSchemaId = CONCAT('SELECT @_SchemaId = schema_id FROM [', DB_NAME(), '].sys.schemas WHERE name = @_SchemaName;');
        SET @ParamDefinition = N'@_SchemaName SYSNAME, @_SchemaId INT OUTPUT';

        EXEC sys.sp_executesql @stmt = @SqlSchemaId, @params = @ParamDefinition, @_SchemaName = @SchemaName, @_SchemaId = @SchemaId OUTPUT;

        IF (@SchemaId IS NULL)
        BEGIN
            SET @ErrorMessage = CONCAT('Could not find @SchemaName: ', QUOTENAME(@SchemaName), ' in Database: ', QUOTENAME(DB_NAME()));
            GOTO ERROR;    
        END
        ELSE 
        BEGIN
            SET @StartSearchTbl = 0;
            SET @DelimiterPosTbl = 0;

            WHILE CHARINDEX(@Delimiter, @TableNames, @StartSearchTbl + 1) > 0
            BEGIN
                SET @DelimiterPosTbl = CHARINDEX(@Delimiter, @TableNames, @StartSearchTbl + 1) - @StartSearchTbl;
                SET @TableName = TRIM(SUBSTRING(@TableNames, @StartSearchTbl, @DelimiterPosTbl));
                SET @ObjectId = NULL

                SET @SqlObjectId = CONCAT('SELECT @_ObjectId = object_id FROM [', DB_NAME(), '].sys.tables WHERE [is_ms_shipped] = 0 AND name = @_TableName;');
                SET @ParamDefinition = N'@_TableName SYSNAME, @_ObjectId INT OUTPUT';

                EXEC sys.sp_executesql @stmt = @SqlObjectId, @params = @ParamDefinition, @_TableName = @TableName, @_ObjectId = @ObjectId OUTPUT;

                IF (@ObjectId IS NULL)
                BEGIN
                    SET @ErrorMessage = CONCAT('Could not find @TableName: ', QUOTENAME(@TableName), ' in any schema within Database: ', QUOTENAME(DB_NAME()));
                    GOTO ERROR;
                END
                ELSE 
                BEGIN
                    /* PRINT(CONCAT('Found a Table with name: ', @TableName, ' now trying to find an ObjectId for: ', '[', @SchemaName, '].[', @TableName, ']')) */
                    /* Below is not redundant: your @TableName may exist in other schema(s) not included in @SchemaNames so the @ObjectId obtained above may be wrong for that @TableName */                    
                    SET @ObjectId = NULL
                    SET @ObjectId = OBJECT_ID('[' + @SchemaName + '].[' + @TableName + ']');
                END
                
                IF (@ObjectId IS NOT NULL)
                BEGIN
                    INSERT INTO [#SelectedTables] ([SchemaID], [ObjectID], [SchemaName], [TableName], [IsTruncated])
                    VALUES (@SchemaId, @ObjectId, @SchemaName, @TableName, 0);
                END;

                SET @StartSearchTbl = CHARINDEX(@Delimiter, @TableNames, @StartSearchTbl + @DelimiterPosTbl) + 1;
            END;
        END;
        SET @StartSearchSch = CHARINDEX(@Delimiter, @SchemaNames, @StartSearchSch + @DelimiterPosSch) + 1;
    END;
END;

PRINT ('/*--------------------------------------- END OF COLLECTING [#SelectedTables] ------------------------------------*/');

IF NOT EXISTS (SELECT 1 FROM [#SelectedTables])
BEGIN
    BEGIN
        SET @ErrorMessage = CONCAT('Could not find any objects specified in the list of schemas: [', @SchemaNames, N'] and tables: [', @TableNames, N'] in the database: [', DB_NAME(DB_ID()), N'].');
        GOTO ERROR;
    END;
END;

IF (LEN(@SchemaNamesExpt) > 0) AND (LEN(@TableNamesExpt) > 0)
BEGIN
SET @StartSearchSch = 0;
SET @DelimiterPosSch = 0;

WHILE CHARINDEX(@Delimiter, @SchemaNamesExpt, @StartSearchSch + 1) > 0
    BEGIN
        SET @DelimiterPosSch = CHARINDEX(@Delimiter, @SchemaNamesExpt, @StartSearchSch + 1) - @StartSearchSch;
        SET @SchemaName = TRIM(SUBSTRING(@SchemaNamesExpt, @StartSearchSch, @DelimiterPosSch));
    
        BEGIN
            SET @StartSearchTbl = 0;
            SET @DelimiterPosTbl = 0;
    
            WHILE CHARINDEX(@Delimiter, @TableNamesExpt, @StartSearchTbl + 1) > 0
            BEGIN
                
                SET @DelimiterPosTbl = CHARINDEX(@Delimiter, @TableNamesExpt, @StartSearchTbl + 1) - @StartSearchTbl;
                SET @TableName = TRIM(SUBSTRING(@TableNamesExpt, @StartSearchTbl, @DelimiterPosTbl));
                
                --PRINT(CONCAT('@TableName: ', @TableName))
    
                INSERT INTO [#ExceptionList] ([SchemaNameExpt], [TableNameExpt])
                VALUES (@SchemaName, @TableName);
    
                SET @StartSearchTbl = CHARINDEX(@Delimiter, @TableNamesExpt, @StartSearchTbl + @DelimiterPosTbl) + 1;
            END;
        END;
        SET @StartSearchSch = CHARINDEX(@Delimiter, @SchemaNamesExpt, @StartSearchSch + @DelimiterPosSch) + 1;
    END;
END


PRINT ('/*--------------------------------------- UPDATING [RowCountBefore] AND [IsToBeTruncated] OF [#SelectedTables] ---*/');
TRUNCATE TABLE [#TableRowCounts];
SELECT @Id = MIN([Id]), @IdMax = MAX([Id]) FROM [#SelectedTables];
WHILE (@Id <= @IdMax)
BEGIN
    SELECT @SqlTableCounts
        = CASE WHEN @DbEngineVersion < 14 /* For SQL Versions older than 14 (2017) use FOR XML PATH instead of STRING_AGG(): */          
          THEN
                    STUFF(
                            (
                                SELECT @UnionAll + ' SELECT ' + CAST([ObjectID] AS NVARCHAR(MAX)) + ' AS [ObjectID], ' + '''' + CAST(QUOTENAME([SchemaName]) AS NVARCHAR(MAX)) + '.'
                                       + CAST(QUOTENAME([TableName]) AS NVARCHAR(MAX)) + ''' AS [TableName], COUNT_BIG(1) AS [RowCount] FROM ' + CAST(QUOTENAME([SchemaName]) AS NVARCHAR(MAX)) + '.'
                                       + CAST(QUOTENAME([TableName]) AS NVARCHAR(MAX))
                                FROM [#SelectedTables]
                                WHERE [Id] BETWEEN @Id AND (@Id + @BatchSize)
                                FOR XML PATH(''), TYPE
                            ).[value]('.', 'NVARCHAR(MAX)')
                          , 1
                          , LEN(@UnionAll)
                          , ''
                         )
          ELSE /* For SQL Versions 14+ (2017+) use STRING_AGG() - comment below section out if you want to install on older versions */
                   STRING_AGG(
                                 CONCAT(
                                           'SELECT '
                                         , CAST([ObjectID] AS NVARCHAR(MAX))
                                         , ' AS [ObjectID], '
                                         , ''''
                                         , CAST(QUOTENAME([SchemaName]) AS NVARCHAR(MAX))
                                         , '.'
                                         , CAST(QUOTENAME([TableName]) AS NVARCHAR(MAX))
                                         , ''' AS [TableName], COUNT_BIG(1) AS [RowCount] FROM '
                                         , CAST(QUOTENAME([SchemaName]) AS NVARCHAR(MAX))
                                         , '.'
                                         , CAST(QUOTENAME([TableName]) AS NVARCHAR(MAX))
                                       )
                               , @UnionAll
                             )
          END
    FROM  [#SelectedTables]
    WHERE [Id] BETWEEN @Id AND (@Id + @BatchSize);

    SET @SqlTableCounts = CONCAT(N'INSERT INTO [#TableRowCounts] ([ObjectID], [TableName], [RowCount])', @SqlTableCounts);

    --SET @SqlTableCounts = CONCAT('SimulatedSyntaxError_', @SqlTableCounts)
    EXEC sys.sp_executesql @stmt = @SqlTableCounts;
    IF (@@ERROR <> 0)
    BEGIN
        SET @ErrorMessage = CONCAT('Error while executing: ', @SqlTableCounts);
        GOTO ERROR;
    END;
    SELECT @Id = MIN([Id]) FROM [#SelectedTables] WHERE [Id] > (@Id + @BatchSize);
    IF  (@Id < @IdMax) AND @WhatIf <> 1
    AND (@Id * 100) / @IdMax <> @PercentProcessed AND @WhatIf <> 1
    BEGIN
        SET @PercentProcessed = (@Id * 100) / @IdMax;
        PRINT (CONCAT(@PercentProcessed, ' percent processed.'));
    END;
END;

UPDATE [st]
SET [st].[RowCountBefore] = [trc].[RowCount]
  , [st].[IsToBeTruncated] = IIF([trc].[RowCount] > COALESCE(@RowCountThreshold, 0), 1, 0)
FROM [#SelectedTables] AS [st]
JOIN [#TableRowCounts] AS [trc]
    ON [trc].[ObjectID] = [st].[ObjectID];

UPDATE [st]
SET [st].[IsOnExceptionList] = 1
  , [st].[IsToBeTruncated] = 0
FROM [#SelectedTables] AS [st]
WHERE EXISTS 
(
    SELECT 1
    FROM 
    [#ExceptionList] AS [el]
    WHERE IIF([el].[SchemaNameExpt] = @ExceptionListWildcard, 1 , CHARINDEX([el].[SchemaNameExpt], [st].[SchemaName], 0)) > 0
    AND   IIF([el].[TableNameExpt]  = @ExceptionListWildcard, 1 , CHARINDEX([el].[TableNameExpt], [st].[TableName], 0)  ) > 0
)
SELECT @CountExceptionList = @@ROWCOUNT
IF (@CountExceptionList > 0) PRINT (CONCAT('/* Flagged ', @CountExceptionList, ' Records in [#SelectedTables] as Exceptions and Updated [IsToBeTruncated] = 0 */'));

SELECT @CountTablesSelected = COUNT([Id]) FROM [#SelectedTables] WHERE [IsToBeTruncated] = 1;

PRINT (CONCAT('/* [#SelectedTables] has a total of: ', @CountTablesSelected, ' Records WHERE [IsToBeTruncated] = 1 */'));

PRINT ('/*--------------------------------------- POPULATING [#ForeignKeyConstraintDefinitions]: -------------------------*/');
WITH [cte]
AS (SELECT [ForeignKeyId] = [fk].[object_id]
         , [ForeignKeyName] = [fk].[name]
         , [SchemaNameSrc] = [SchSrc].[SchemaName]
         , [TableNameSrc] = OBJECT_NAME([fkc].[parent_object_id])
         , [ColumnIdSrc] = [fkc].[parent_column_id]
         , [ColumnNameSrc] = [ColSrc].[name]
         , [SchemaNameTrgt] = [SchTgt].[SchemaName]
         , [TableNameTrgt] = OBJECT_NAME([fkc].[referenced_object_id])
         , [ColumnIdTrgt] = [fkc].[referenced_column_id]
         , [ColumnNameTrgt] = [ColTgt].[name]
         , [SchemaIdTrgt] = [SchTgt].[SchemaId]
         , [DeleteReferentialAction] = [fk].[delete_referential_action]
         , [UpdateReferentialAction] = [fk].[update_referential_action]
         , [ObjectIdTrgt] = OBJECT_ID('[' + [SchTgt].[SchemaName] + '].[' + OBJECT_NAME([fkc].[referenced_object_id]) + ']')
    FROM sys.foreign_keys AS [fk]
    CROSS APPLY (
                    SELECT [fkc].[parent_column_id]
                         , [fkc].[parent_object_id]
                         , [fkc].[referenced_object_id]
                         , [fkc].[referenced_column_id]
                    FROM [sys].[foreign_key_columns] AS [fkc]
                    WHERE 1 = 1
                    AND   [fk].[parent_object_id] = [fkc].[parent_object_id]
                    AND   [fk].[referenced_object_id] = [fkc].[referenced_object_id]
                    AND   [fk].[object_id] = [fkc].[constraint_object_id]
                ) AS [fkc]
    CROSS APPLY (
                    SELECT [ss].[name] AS [SchemaName]
                    FROM sys.objects AS [so]
                    INNER JOIN sys.schemas AS [ss]
                        ON [ss].[schema_id] = [so].[schema_id]
                    WHERE [so].[object_id] = [fkc].[parent_object_id]
                ) AS [SchSrc]
    CROSS APPLY (
                    SELECT [sc].[name]
                    FROM sys.columns AS [sc]
                    WHERE [sc].[object_id] = [fk].[parent_object_id]
                    AND   [sc].[column_id] = [fkc].[parent_column_id]
                ) AS [ColSrc]
    CROSS APPLY (
                    SELECT [ss].[schema_id] AS [SchemaId]
                         , [ss].[name] AS [SchemaName]
                    FROM sys.objects AS [so]
                    INNER JOIN sys.schemas AS [ss]
                        ON [ss].[schema_id] = [so].[schema_id]
                    WHERE [so].[object_id] = [fkc].[referenced_object_id]
                ) AS [SchTgt]
    CROSS APPLY (
                    SELECT [sc].[name]
                    FROM sys.columns AS [sc]
                    WHERE [sc].[object_id] = [fk].[referenced_object_id]
                    AND   [sc].[column_id] = [fkc].[referenced_column_id]
                ) AS [ColTgt]
    INNER JOIN [#SelectedTables] AS [st]
        ON  [st].[SchemaID] = [SchTgt].[SchemaId]
        /* if you want to search by source schema+table names (rather than target) uncomment line below and comment the next one: */
        /* AND             [st].[ObjectID] = OBJECT_ID(QUOTENAME([SchSrc].[SchemaName]) + '.' + QUOTENAME(OBJECT_NAME([fkc].[parent_object_id]))) */
        AND [st].[ObjectID] = OBJECT_ID(QUOTENAME([SchTgt].[SchemaName]) + '.' + QUOTENAME(OBJECT_NAME([fkc].[referenced_object_id])))
        AND [st].[IsToBeTruncated] = 1
        )

INSERT INTO [#ForeignKeyConstraintDefinitions]
    (
        [ForeignKeyId]
      , [ForeignKeyName]
      , [ObjectIdTrgt]
      , [SchemaNameSrc]
      , [TableNameSrc]
      , [SchemaNameTrgt]
      , [TableNameTrgt]
      , [DropConstraintCommand]
      , [RecreateConstraintCommand]
    )
SELECT [cte].[ForeignKeyId]
     , [cte].[ForeignKeyName]
     , [cte].[ObjectIdTrgt]
     , [cte].[SchemaNameSrc]
     , [cte].[TableNameSrc]
     , [cte].[SchemaNameTrgt]
     , [cte].[TableNameTrgt]
     , [DropConstraintCommand] = 'ALTER TABLE ' + QUOTENAME([cte].[SchemaNameSrc]) + '.' + QUOTENAME([cte].[TableNameSrc]) + ' DROP CONSTRAINT ' + QUOTENAME([cte].[ForeignKeyName]) + ';'
     , [RecreateConstraintCommand] = CONCAT(
                                             'ALTER TABLE ' + QUOTENAME([cte].[SchemaNameSrc]) + '.' + QUOTENAME([cte].[TableNameSrc]) + ' WITH NOCHECK ADD CONSTRAINT '
                                             + QUOTENAME([cte].[ForeignKeyName]) + ' '
                                           , CASE
                                             WHEN @DbEngineVersion < 14 /* For SQL Versions older than 14 (2017) use FOR XML PATH for all multi-column constraints: */
                                             THEN
                                                       'FOREIGN KEY (' + STUFF((
                                                                                   SELECT ', ' + QUOTENAME([t].[ColumnNameSrc])
                                                                                   FROM [cte] AS [t]
                                                                                   WHERE [t].[ForeignKeyId] = [cte].[ForeignKeyId]
                                                                                   ORDER BY [t].[ColumnIdTrgt] --This is identical to the ORDER BY in WITHIN GROUP clause in STRING_AGG
                                                                                   FOR XML PATH(''), TYPE
                                                                               ).[value]('(./text())[1]', 'VARCHAR(MAX)')
                                                                             , 1
                                                                             , 2
                                                                             , ''
                                                                              ) + ' ) ' + 'REFERENCES ' + QUOTENAME([cte].[SchemaNameTrgt]) + '.' + QUOTENAME([cte].[TableNameTrgt]) + ' ('
                                                       + STUFF((
                                                                   SELECT ', ' + QUOTENAME([t].[ColumnNameTrgt])
                                                                   FROM [cte] AS [t]
                                                                   WHERE [t].[ForeignKeyId] = [cte].[ForeignKeyId]
                                                                   ORDER BY [t].[ColumnIdTrgt] --This is identical to the ORDER BY in WITHIN GROUP clause in STRING_AGG
                                                                   FOR XML PATH(''), TYPE
                                                               ).[value]('(./text())[1]', 'VARCHAR(MAX)')
                                                             , 1
                                                             , 2
                                                             , '') + ' )'
                                             ELSE /* For SQL Versions 2017+ use STRING_AGG for all multi-column constraints: */
                                                     'FOREIGN KEY (' + STRING_AGG(QUOTENAME([cte].[ColumnNameSrc]), ', ')WITHIN GROUP(ORDER BY [cte].[ColumnIdTrgt]) + ') ' + 'REFERENCES '
                                                     + QUOTENAME([cte].[SchemaNameTrgt]) + '.' + QUOTENAME([cte].[TableNameTrgt]) + ' (' + STRING_AGG(QUOTENAME([cte].[ColumnNameTrgt]), ', ') + ')'
                                             END
                                           , CASE
                                                 WHEN [cte].[DeleteReferentialAction] = 1 THEN ' ON DELETE CASCADE '
                                                 WHEN [cte].[DeleteReferentialAction] = 2 THEN ' ON DELETE SET NULL '
                                                 WHEN [cte].[DeleteReferentialAction] = 3 THEN ' ON DELETE SET DEFAULT '
                                                 ELSE ''
                                             END
                                           , CASE
                                                 WHEN [cte].[UpdateReferentialAction] = 1 THEN ' ON UPDATE CASCADE '
                                                 WHEN [cte].[UpdateReferentialAction] = 2 THEN ' ON UPDATE SET NULL '
                                                 WHEN [cte].[UpdateReferentialAction] = 3 THEN ' ON UPDATE SET DEFAULT '
                                                 ELSE ';'
                                             END
                                             --, TRIM(@cr) + 'ALTER TABLE ' + QUOTENAME([cte].[SchemaNameSrc]) + '.' + QUOTENAME([cte].[TableNameSrc]) + ' CHECK CONSTRAINT '
                                             --  + QUOTENAME([cte].[ForeignKeyName]) 
                                             --, @crlf, ';'
                                           )
FROM [cte]
GROUP BY [cte].[ForeignKeyId]
       , [cte].[SchemaNameSrc]
       , [cte].[TableNameSrc]
       , [cte].[ForeignKeyName]
       , [cte].[ObjectIdTrgt]
       , [cte].[SchemaNameTrgt]
       , [cte].[TableNameTrgt]
       , [cte].[DeleteReferentialAction]
       , [cte].[UpdateReferentialAction]
ORDER BY [cte].[TableNameSrc];

SELECT @CountFKFound = COUNT([Id]) FROM [#ForeignKeyConstraintDefinitions];
PRINT (CONCAT(N'/* Found: ', @CountFKFound, ' Foreign Keys Referencing ', @CountTablesSelected, ' tables selected for truncation in : [', DB_NAME(DB_ID()), N'] database */'));

IF (@CountFKFound > 0)
    UPDATE [st]
    SET [st].[NumFkReferencing] = [Fk].[ReferencingObjCnt]
    FROM [#SelectedTables] AS [st]
    CROSS APPLY (
                   SELECT COUNT(DISTINCT [fkd].[ForeignKeyId]) AS [ReferencingObjCnt]
                   FROM [#ForeignKeyConstraintDefinitions] AS [fkd]
                   WHERE [fkd].[ObjectIdTrgt] = [st].[ObjectID]
                   AND [st].[IsToBeTruncated] = 1
                ) [Fk];

IF (@CountFKFound < 1)
BEGIN
    UPDATE [st] SET [st].[IsReferencedByFk] = 0 FROM [#SelectedTables] AS [st] WHERE [IsToBeTruncated] = 1;
END;
ELSE
BEGIN
    UPDATE [st]
    SET [st].[IsReferencedByFk] = CASE WHEN [fkc].[ObjectIdTrgt] IS NOT NULL THEN 1 ELSE 0 END
    FROM [#SelectedTables] AS [st]
    LEFT JOIN [#ForeignKeyConstraintDefinitions] AS [fkc]
        ON [st].[ObjectID] = [fkc].[ObjectIdTrgt]
        AND [st].[IsToBeTruncated] = 1;

    SELECT @CountFKObjectIdTrgt = COUNT(DISTINCT [ObjectIdTrgt]) FROM [#ForeignKeyConstraintDefinitions];
    SELECT @CountIsReferencedByFk = COUNT([Id]) FROM [#SelectedTables] WHERE [IsReferencedByFk] = 1 AND [IsToBeTruncated] = 1;

    IF (@CountFKObjectIdTrgt <> @CountIsReferencedByFk)
    BEGIN
        SET @ErrorMessage
            = CONCAT(
                        'Distinct Count of [#ForeignKeyConstraintDefinitions].[ObjectIdTrgt]: '
                      , @CountFKObjectIdTrgt
                      , ' does not match the number of [IsReferencedByFk] flags in [#SelectedTables]: '
                      , @CountIsReferencedByFk
                    );
        GOTO ERROR;
    END;
    ELSE
    BEGIN
        PRINT (CONCAT(
                         '/* Distinct Count of [#ForeignKeyConstraintDefinitions].[ObjectIdTrgt]: '
                       , @CountFKObjectIdTrgt
                       , ' matches the number of [IsReferencedByFk] flags in [#SelectedTables]: '
                       , @CountIsReferencedByFk, ' */'
                     )
              );
    END;
END;

PRINT ('/*--------------------------------------- POPULATING [#SchemaBoundViews]: ----------------------------------------*/');

TRUNCATE TABLE [#SchemaBoundViews];
INSERT INTO [#SchemaBoundViews]
    (
        [SbvObjectId]
      , [ReferencingObjectSchema]
      , [ReferencingObjectName]
      , [IsEncrypted]
      , [DropViewCommand]
      , [RecreateViewCommand]
    )
SELECT DISTINCT
       [sed].[referencing_id] AS [SbvObjectId]
     , SCHEMA_NAME([ss].[schema_id]) AS [ReferencingObjectSchema]
     , OBJECT_NAME([vid].[object_id]) AS [ReferencingObjectName]
     , OBJECTPROPERTY([vid].[object_id], 'IsEncrypted') AS [IsEncrypted]
     , CONCAT('DROP VIEW ', QUOTENAME(SCHEMA_NAME([ss].[schema_id])), '.', QUOTENAME(OBJECT_NAME([vid].[object_id]))) AS [DropViewCommand]
     , [sqm].[definition] AS [RecreateViewCommand]
FROM sys.sql_expression_dependencies AS [sed]
JOIN sys.objects AS [vid]
    ON [sed].[referencing_id] = [vid].[object_id]
JOIN sys.schemas AS [ss]
    ON [ss].[schema_id] = [vid].[schema_id]
JOIN sys.sql_modules AS [sqm]
    ON [sqm].[object_id] = [vid].[object_id]
WHERE [vid].[type_desc] = 'VIEW'
AND   [sqm].[is_schema_bound] = 1
AND EXISTS (
                SELECT 1 
                FROM [#SelectedTables] AS [st]
                WHERE [sed].[referenced_id] = [st].[ObjectID]
                AND [st].[IsToBeTruncated] = 1
            );

UPDATE [sbv]
SET [sbv].[@level0type] = [Xtp].[@level0type]
  , [sbv].[@level0name] = [Xtp].[@level0name]
  , [sbv].[@level1type] = [Xtp].[@level1type]
  , [sbv].[@level1name] = [Xtp].[@level1name]
FROM [#SchemaBoundViews] AS [sbv]
OUTER APPLY (
                SELECT DISTINCT
                       'SCHEMA' AS [@level0type]
                     , [sch].[name] AS [@level0name]
                     , [obj].[type_desc] AS [@level1type]
                     , [obj].[name] AS [@level1name]
                FROM sys.objects [obj]
                INNER JOIN sys.schemas AS [sch]
                    ON [obj].[schema_id] = [sch].[schema_id]
                INNER JOIN sys.columns AS [col]
                    ON [obj].[object_id] = [col].[object_id]
                WHERE [obj].[object_id] = [sbv].[SbvObjectId]
            ) AS [Xtp]
WHERE [sbv].[IsEncrypted] = 0;

SELECT @CountSchBvFound = COUNT([Id]) FROM [#SchemaBoundViews];
PRINT (CONCAT(N'/* Found: ', @CountSchBvFound, ' Schema-Bound Views Referencing ', @CountTablesSelected, ' tables selected for truncation in : [', DB_NAME(DB_ID()), N'] database */'));

IF (@CountSchBvFound < 1)
BEGIN
    UPDATE [st] SET [st].[IsReferencedBySchBv] = 0 FROM [#SelectedTables] AS [st] WHERE [st].[IsToBeTruncated] = 1;
END;
ELSE
BEGIN
    INSERT INTO [#SbvToSelTablesLink] ([SbvObjectId], [ReferncedObjectId])
    SELECT DISTINCT
           [sbv].[SbvObjectId]
         , [sed].[referenced_id] AS [ReferncedObjectId]
    FROM [#SchemaBoundViews] AS [sbv]
    JOIN sys.sql_expression_dependencies AS [sed]
        ON [sed].[referencing_id] = [sbv].[SbvObjectId]
    JOIN [#SelectedTables] AS [st]
        ON [st].[ObjectID] = [sed].[referenced_id]
        AND [st].[IsToBeTruncated] = 1;

    UPDATE [st]
    SET [st].[IsReferencedBySchBv] = CASE WHEN [sbvcr].[ReferncedObjectId] IS NOT NULL THEN 1 ELSE 0 END
    FROM [#SelectedTables] AS [st]
    LEFT JOIN [#SbvToSelTablesLink] AS [sbvcr]
        ON [sbvcr].[ReferncedObjectId] = [st].[ObjectID]
        AND [st].[IsToBeTruncated] = 1;

    UPDATE [st]
    SET [st].[NumSchBvReferencing] = [Sbv].[ReferencingObjCnt]
    FROM [#SelectedTables] AS [st]
    CROSS APPLY (
                   SELECT COUNT(DISTINCT [sbvcr].[SbvObjectId]) AS [ReferencingObjCnt]
                   FROM [#SbvToSelTablesLink] AS [sbvcr]
                   JOIN [#SchemaBoundViews] AS [sbv]
                       ON [sbvcr].[SbvObjectId] = [sbv].[SbvObjectId]
                       AND [sbvcr].[ReferncedObjectId] = [st].[ObjectID]
                       AND [st].[IsToBeTruncated] = 1
               ) [Sbv];

    SELECT @CountSchBvsReferencedObjectIds = COUNT(DISTINCT [ReferncedObjectId]) FROM [#SbvToSelTablesLink];
    SELECT @CountTblsReferencedBySchBvs = COUNT([Id]) FROM [#SelectedTables] WHERE [IsReferencedBySchBv] = 1 AND [IsToBeTruncated] = 1;

    IF (@CountSchBvsReferencedObjectIds <> @CountTblsReferencedBySchBvs)
    BEGIN
        SET @ErrorMessage
            = CONCAT(
                        'Number of DISTINCT [ReferencedObjectId] in Schema-Bound Views: '
                      , @CountSchBvsReferencedObjectIds
                      , ' does not match the Number of Updated [#SelectedTables].[IsReferencedBySchBv] flag: '
                      , @CountTblsReferencedBySchBvs
                    );
        GOTO ERROR;
    END;
    ELSE
    BEGIN
        PRINT (CONCAT(
                         '/* Number of DISTINCT [ReferencedObjectId] in Schema-Bound Views: '
                       , @CountSchBvsReferencedObjectIds
                       , ' matches the Number of Updated [#SelectedTables].[IsReferencedBySchBv] flag: '
                       , @CountTblsReferencedBySchBvs, ' */'
                     )
              );
    END;

    PRINT ('/*--------------------------------------- UPDATING [XtdProperties] of [#SchemaBoundViews]: -----------------------*/');

    SELECT @Id = MIN([Id]), @IdMax = MAX([Id]) FROM [#SchemaBoundViews] WHERE [IsEncrypted] = 0;
    WHILE (@Id <= @IdMax)
    BEGIN
        SELECT @level0type = [@level0type]
             , @level0name = [@level0name]
             , @level1type = [@level1type]
             , @level1name = [@level1name]
        FROM [#SchemaBoundViews]
        WHERE [Id] = @Id;

        IF @DbEngineVersion < 14
        BEGIN
            SELECT @SqlXtndProperties = 
                STUFF((
                    SELECT @crlf + CONCAT('EXEC [sys].[sp_addextendedproperty] @name = '''
                                          , [name]
                                          , ''', @value = '''
                                          , CONVERT(NVARCHAR(MAX), [value])
                                          , ''', @level0type = '''
                                          , @level0type
                                          , ''', @level0name = '''
                                          , @level0name
                                          , ''', @level1type = '''
                                          , @level1type
                                          , ''', @level1name = '''
                                          , @level1name
                                          , ''';')
                    FROM sys.fn_listextendedproperty(NULL, @level0type, @level0name, @level1type, @level1name, NULL, NULL)
                    FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, LEN(@crlf), '');
        END;
        ELSE
        BEGIN
            SELECT @SqlXtndProperties = STRING_AGG(
                                            CONCAT('EXEC [sys].[sp_addextendedproperty] @name = '''
                                                   , [name]
                                                   , ''', @value = '''
                                                   , CONVERT(NVARCHAR(MAX), [value])
                                                   , ''', @level0type = '''
                                                   , @level0type
                                                   , ''', @level0name = '''
                                                   , @level0name
                                                   , ''', @level1type = '''
                                                   , @level1type
                                                   , ''', @level1name = '''
                                                   , @level1name
                                                   , ''';')
                                         , @crlf)
            FROM sys.fn_listextendedproperty(NULL, @level0type, @level0name, @level1type, @level1name, NULL, NULL);
        END;

        IF (@SqlXtndProperties IS NOT NULL)
        BEGIN
            UPDATE [#SchemaBoundViews] SET [XtdProperties] = @SqlXtndProperties WHERE [Id] = @Id;
        END;

        SET @SqlXtndProperties = NULL;
        SELECT @Id = COALESCE(MIN([Id]), @Id + 1) FROM [#SchemaBoundViews] WHERE [Id] > @Id AND [IsEncrypted] = 0;
        IF  (@Id < @IdMax) AND @WhatIf <> 1
        AND (@Id * 100) / @IdMax <> @PercentProcessed AND @WhatIf <> 1
        BEGIN
            SET @PercentProcessed = (@Id * 100) / @IdMax;
            PRINT (CONCAT(@PercentProcessed, ' percent processed.'));
        END;
    END;

    PRINT ('/*--------------------------------------- POPULATING [#IndexesOnSchemaBoundViews]: -------------------------------*/');
    
    INSERT INTO [#IndexesOnSchemaBoundViews] ([ReferencedViewObjectId], [IndexId], [IsUnique], [IndexType], [IndexName], [OnView], [ColumnNames])
    SELECT 
           [v].[object_id]                                                                                    AS [ReferencedViewObjectId]
         , [i].[index_id]                                                                                     AS [IndexId]
         /* , 'CREATE ' */
         , IIF([i].[is_unique] = 1, 'UNIQUE ', '')                                                            AS [IsUnique]
         , [i].[type_desc]                                                                                    AS [IndexType]
         /* , ' INDEX ' */
         , QUOTENAME([i].[name])                                                                              AS [IndexName]
         , CONCAT('ON ', QUOTENAME([ss].[name]), '.', QUOTENAME([v].[name]))                                  AS [OnView]
         , CONCAT('(', STRING_AGG(QUOTENAME([c].[name]), ', ')WITHIN GROUP(ORDER BY [ic].[key_ordinal]), ')') AS [ColumnNames]
    FROM sys.indexes [i]
    JOIN sys.views [v]
        ON [i].[object_id] = [v].[object_id]
    JOIN sys.schemas AS [ss]
        ON [ss].[schema_id] = [v].[schema_id]
    JOIN [#SchemaBoundViews] AS [sbv]
        ON [sbv].[SbvObjectId] = [v].[object_id]
    JOIN sys.index_columns [ic]
        ON  [i].[object_id] = [ic].[object_id]
        AND [i].[index_id] = [ic].[index_id]
    JOIN sys.columns [c]
        ON  [ic].[object_id] = [c].[object_id]
        AND [ic].[column_id] = [c].[column_id]
    WHERE [i].[is_hypothetical] = 0
    AND   [sbv].[IsEncrypted] = 0
    GROUP BY [v].[object_id]
           , [v].[name]
           , [ss].[name]
           , [i].[index_id]
           , [i].[name]
           , [i].[type_desc]
           , [i].[is_unique]
    ORDER BY [v].[object_id]
           , [i].[index_id];

    PRINT ('/*--------------------------------------- POPULATING [#TriggersOnSchemaBoundViews]: ------------------------------*/');

    INSERT INTO [#TriggersOnSchemaBoundViews] ([ReferencedViewObjectId], [TriggerId], [TriggerName], [IsEncrypted])
    SELECT [tr].[parent_id] AS [ReferencedViewObjectId]
         , [tr].[object_id] AS [TriggerId]
         , QUOTENAME(OBJECT_NAME([tr].[object_id])) AS [TriggerName]
         , OBJECTPROPERTY([tr].[object_id], 'IsEncrypted') AS [IsEncrypted]
    FROM sys.triggers [tr]
    JOIN [#SchemaBoundViews] AS [sbv]
        ON [sbv].[SbvObjectId] = [tr].[parent_id];

    IF EXISTS (SELECT 1 FROM [#TriggersOnSchemaBoundViews] WHERE [IsEncrypted] = 0)
    BEGIN
        PRINT ('/*--------------------------------------- POPULATING [#TriggerDefinitions]: --------------------------------------*/');

        SELECT @Id = MIN([Id]), @IdMax = MAX([Id]) FROM [#TriggersOnSchemaBoundViews] WHERE [IsEncrypted] = 0;
        WHILE (@Id <= @IdMax)
        BEGIN
            BEGIN
                
                SELECT @TriggerId = [TriggerId] FROM [#TriggersOnSchemaBoundViews] WHERE [Id] = @Id
                SET @SqlTriggerDefinition = CONCAT('SELECT  @_TriggerDefinition = [definition] FROM [', DB_NAME(), '].sys.sql_modules WHERE [object_id] = @_TriggerId;');
                SET @ParamDefinition = N'@_TriggerId INT, @_TriggerDefinition NVARCHAR(MAX) OUTPUT';

                EXEC sys.sp_executesql @stmt = @SqlTriggerDefinition, @params = @ParamDefinition, @_TriggerId = @TriggerId, @_TriggerDefinition = @TriggerDefinition OUTPUT;
                                
                SET @PointerString = 0;
                SET @PointerNewLine = -2; /* (-2) because at first iteration we want to catch the first 2 characters of the first line */
    
                DBCC CHECKIDENT('#TriggerDefinitions', RESEED, 0) WITH NO_INFOMSGS;
    
                /* Print out (save into temp table) each line at a time: */
                WHILE @PointerString <= LEN(@TriggerDefinition)
                BEGIN
                    IF (   (SUBSTRING(@TriggerDefinition, @PointerString + 1, 2) = @crlf)
                     OR    (@PointerString = LEN(@TriggerDefinition))
                       )
                    BEGIN
                        SELECT @LineOfCode = REPLACE(REPLACE(SUBSTRING(@TriggerDefinition, @PointerNewLine + LEN(@crlf), (@PointerString - @PointerNewLine)), CHAR(13), ''), CHAR(10), '');
                        INSERT INTO [#TriggerDefinitions] ([TriggerId], [LineOfCode]) VALUES (@TriggerId, @LineOfCode);
                        SET @PointerNewLine = @PointerString;
                    END;
                    SET @PointerString = @PointerString + 1;
                END;
            END;
            SELECT @Id = MIN([Id]) FROM [#TriggersOnSchemaBoundViews] WHERE [Id] > @Id AND [IsEncrypted] = 0;
        END;
    END;
END;

PRINT ('/*--------------------------------------- UPDATING [IsCDCEnabled] flag of [#SelectedTables]: ---------------------*/');
SELECT @IsDbCDCEnabled = [is_cdc_enabled] FROM [master].sys.databases WHERE [name] = DB_NAME();
IF (@IsDbCDCEnabled = 1)
BEGIN
IF EXISTS 
(
            SELECT 1
            FROM [#SelectedTables] AS [st]
            JOIN sys.tables AS [stb]
                ON  [st].[ObjectID] = [stb].[object_id]
                AND [st].[IsToBeTruncated] = 1
                AND [stb].[is_tracked_by_cdc] = 1
            LEFT JOIN [cdc].[change_tables] AS [cdc]
                ON [st].[ObjectID] = [cdc].[source_object_id]
)
    UPDATE [st]
    SET [st].[IsCDCEnabled] = CASE WHEN [stb].[object_id] IS NOT NULL THEN 1 ELSE 0 END
    FROM [#SelectedTables] AS [st]
    JOIN sys.tables AS [stb]
        ON [st].[ObjectID] = [stb].[object_id]
        AND [st].[IsToBeTruncated] = 1
        AND [stb].[is_tracked_by_cdc] = 1
    LEFT JOIN [cdc].[change_tables] AS [cdc]
        ON [st].[ObjectID] = [cdc].[source_object_id];
END;
ELSE
BEGIN
    UPDATE [#SelectedTables] SET [IsCDCEnabled] = 0 WHERE [IsToBeTruncated] = 1;
END;

SELECT @CountTblsCDCEnabled = COUNT([Id]) FROM [#SelectedTables] WHERE [IsCDCEnabled] = 1 AND [IsToBeTruncated] = 1;
PRINT (CONCAT(N'/* Flagged: ', @CountTblsCDCEnabled, ' Tables as CDC-Enabled within the set of: ', @CountTablesSelected, ' tables selected for truncation in : [', DB_NAME(), N'] db */'));

PRINT ('/*--------------------------------------- POPULATING [#CDCInstances]: -------------------------------------------*/');
IF (@IsDbCDCEnabled = 1)
INSERT INTO [#CDCInstances]
    (
        [CdcObjectId]
      , [ReferncedObjectId]
      , [source_schema]
      , [source_name]
      , [capture_instance]
      , [supports_net_changes]
      , [role_name]
      , [index_name]
      , [captured_column_list]
      , [filegroup_name]
      , [allow_partition_switch]
    )
SELECT [ct].[object_id] AS [CdcObjectId]
     , [so].[object_id] AS [ReferncedObjectId]
     , [ss].[name] AS [source_schema]
     , [so].[name] AS [source_name]
     , [ct].[capture_instance]
     , [ct].[supports_net_changes]
     , [ct].[role_name]
     , [ct].[index_name]
     , CASE
           WHEN @DbEngineVersion < 14 THEN STUFF((
                                                     SELECT ', ' + [cc].[column_name]
                                                     FROM [cdc].[captured_columns] AS [cc]
                                                     WHERE [cc].[object_id] = [ct].[object_id]
                                                     FOR XML PATH(''), TYPE
                                                 ).[value]('.', 'NVARCHAR(MAX)')
                                               , 1
                                               , 2
                                               , ''
                                                )
           ELSE STRING_AGG([cc].[column_name], ', ')
       END AS [captured_column_list]
     , [ct].[filegroup_name]
     , [ct].[partition_switch] AS [allow_partition_switch]
FROM [cdc].[change_tables] AS [ct]
JOIN [cdc].[captured_columns] AS [cc]
    ON [ct].[object_id] = [cc].[object_id]
JOIN sys.objects AS [so]
    ON [ct].[source_object_id] = [so].[object_id]
JOIN sys.schemas AS [ss]
    ON [so].[schema_id] = [ss].[schema_id]
JOIN [#SelectedTables] AS [st]
    ON [st].[ObjectID] = [so].[object_id]
WHERE [st].[IsCDCEnabled] = 1
AND   [st].[IsToBeTruncated] = 1
GROUP BY [ct].[object_id]
       , [so].[object_id]
       , [ss].[name]
       , [so].[name]
       , [ct].[capture_instance]
       , [ct].[supports_net_changes]
       , [ct].[role_name]
       , [ct].[index_name]
       , [ct].[filegroup_name]
       , [ct].[partition_switch];

SELECT @CountCDCInstFound = COUNT([Id]) FROM [#CDCInstances];
PRINT (CONCAT(N'/* Found: ', @CountCDCInstFound, ' CDC Instances Referencing ', @CountTblsCDCEnabled, ' CDC-Enabled Tables within ', @CountTablesSelected, ' tables selected for truncation in : [', DB_NAME(DB_ID()), N'] database */'));

UPDATE [st]
SET [st].[NumCDCInstReferencing] = [cdc].[ReferencingObjCnt]
FROM [#SelectedTables] AS [st]
CROSS APPLY (
                SELECT COUNT([cdc].[CdcObjectId]) AS [ReferencingObjCnt]
                FROM [#CDCInstances] AS [cdc]
                WHERE [cdc].[ReferncedObjectId] = [st].[ObjectID]
                AND   [st].[IsCDCEnabled] = 1
                AND   [st].[IsToBeTruncated] = 1
            ) [cdc];
PRINT ('/*--------------------------------------- UPDATING [IsPublished] flag of [#SelectedTables]: ----------------------*/');

UPDATE [st]
SET [st].[IsPublished] = COALESCE([tb].[is_published], [tb].[is_merge_published], [tb].[is_schema_published], 0)
FROM [#SelectedTables] AS [st]
JOIN sys.tables AS [tb]
    ON [st].[ObjectID] = [tb].[object_id]
    AND [st].[IsToBeTruncated] = 1
JOIN sys.schemas AS [ss]
    ON [ss].[schema_id] = [tb].[schema_id]
WHERE [tb].[is_published] = 1
OR    [tb].[is_merge_published] = 1
OR    [tb].[is_schema_published] = 1;


IF (@DbEngineVersion >= 13)
BEGIN
    PRINT ('/*--------------------------------------- UPDATING [TemporalType] value of [#SelectedTables]: ----------------*/');
    
    UPDATE [st]
    SET [st].[TemporalType] = [tb].[temporal_type]
      , [st].[HistoryTblObjectID] = [ht].[object_id]
    FROM [#SelectedTables] AS [st]
    JOIN sys.tables AS [tb]
        ON [st].[ObjectID] = [tb].[object_id]
        AND [st].[IsToBeTruncated] = 1
    LEFT JOIN sys.tables [ht] 
        ON [tb].[history_table_id] = [ht].[object_id]
    WHERE [tb].[temporal_type] > 0
    
    SELECT @CountTemporalTbls = @@ROWCOUNT;

    PRINT (CONCAT(N'/* Updated: ', @CountTemporalTbls, ' Tables as Temporal in: [', DB_NAME(DB_ID()), N'] database */'));
END

PRINT ('/*--------------------------------------- POPULATING [#PublicationsArticles]: -------------------------------------*/');
SELECT @CountPublishedTablesFound = COUNT([Id]) FROM [#SelectedTables] WHERE [IsPublished] = 1 AND [IsToBeTruncated] = 1;
PRINT (CONCAT(N'/* Flagged: ', @CountPublishedTablesFound, ' Tables as Published within the set of: ', @CountTablesSelected, ' tables selected for truncation in : [', DB_NAME(), N'] db */'));

IF (@CountPublishedTablesFound > 0)
BEGIN    
    TRUNCATE TABLE [#PublicationsArticles];
    IF EXISTS (
                SELECT 1
                FROM [#SelectedTables] AS [slt]
                JOIN sys.tables AS [sytb]
                    ON  [slt].[ObjectID] = [sytb].[object_id]
                    AND [slt].[IsToBeTruncated] = 1
                    AND [sytb].[is_published] = 1
              )
    BEGIN
        INSERT INTO [#PublicationsArticles]
            (
                [publication_id]
              , [article_id]
              , [publication]
              , [article]
              , [source_table]
              , [destination_table]
              , [vertical_partition]
              , [type]
              , [sync_object]
              , [ins_cmd]
              , [del_cmd]
              , [upd_cmd]
              , [creation_script]
              , [description]
              , [pre_creation_cmd]
              , [filter_clause]
              , [schema_option]
              , [destination_owner]
              , [status]
              , [force_invalidate_snapshot]
              , [use_default_datatypes]
              , [publisher]
              , [fire_triggers_on_snapshot]
              , [ReferncedObjectId]
            )
        SELECT
            [sp].[pubid]                                                                        AS [publication_id]
          , [sa].[artid]                                                                        AS [article_id]
          , [sp].[name]                                                                         AS [publication]
          , [sa].[name]                                                                         AS [article]
          , OBJECT_NAME([sa].[objid])                                                           AS [source_table]
          , [sa].[dest_table]                                                                   AS [destination_table]
          , 'false'                                                                             AS [vertical_partition] --= nchar(5) N'false' [vertical partition] from [sys].[sp_helparticle]
          , CASE [sa].[type]
                WHEN 1 THEN 'logbased'
                WHEN 2 THEN 'logbased manualfilter'
                WHEN 5 THEN 'logbased manualview'
                WHEN 7 THEN 'logbased manualboth'
                WHEN 8 THEN 'proc exec'
                WHEN 24 THEN 'serializable proc exec'
                WHEN 32 THEN 'proc schema only'
                WHEN 64 THEN 'view schema only'
                WHEN 128 THEN 'func schema only'
                ELSE NULL
            END                                                                                 AS [type]
          , OBJECT_NAME([sa].[sync_objid])                                                      AS [sync_object]
          , [sa].[ins_cmd]                                                                      AS [ins_cmd]
          , [sa].[del_cmd]                                                                      AS [del_cmd]
          , [sa].[upd_cmd]                                                                      AS [upd_cmd]
          , [sa].[creation_script]                                                              AS [creation_script]
          , [sa].[description]                                                                  AS [description]
          , CASE [sa].[pre_creation_cmd]
                WHEN 0 THEN 'none'
                WHEN 1 THEN 'drop'
                WHEN 2 THEN 'delete'
                WHEN 3 THEN 'truncate'
                ELSE '-unknown-'
            END                                                                                 AS [pre_creation_cmd]
          , [sa].[filter_clause]                                                                AS [filter_clause]
          , [sa].[schema_option]                                                                AS [schema_option]
          , [sa].[dest_owner]                                                                   AS [destination_owner]
          , [sa].[status]                                                                       AS [status]
          , 0                                                                                   AS [force_invalidate_snapshot]
          , 1                                                                                   AS [use_default_datatypes]     
          , NULL                                                                                AS [publisher] -- SYSNAME /* Specifies a non-SQL Server Publisher, shouldn't be used when adding an article to a SQL Server Publisher */
          , IIF([sa].[fire_triggers_on_snapshot] = 1, 'true', 'false')                          AS [fire_triggers_on_snapshot] -- nvarchar(5)
          , [st].[ObjectID]                                                                     AS [ReferncedObjectId]    
        FROM [dbo].[sysarticles] AS [sa]
        JOIN [dbo].[syspublications] AS [sp]
            ON [sp].[pubid] = [sa].[pubid]
        JOIN sys.objects AS [so] 
            ON [so].[object_id] = [sa].[objid]
        JOIN sys.schemas AS [ss]
            ON [ss].[schema_id] = [so].[schema_id]
        JOIN [#SelectedTables] AS [st]
            ON [st].[ObjectID] = [so].[object_id];
        
        SELECT @CountPublishedArticlesFound = COUNT([Id]) FROM [#PublicationsArticles];
        PRINT (CONCAT(N'/* Found: ', @CountPublishedArticlesFound, ' Published Articles within the set of: ', @CountPublishedTablesFound, ' published tables selected for truncation in : [', DB_NAME(), N'] db */'));
            
        UPDATE [st]
        SET [st].[NumPublArtReferencing] = [art].[ArticleCnt]
        FROM [#SelectedTables] AS [st]
        CROSS APPLY (
                       SELECT COUNT([pub].[article_id]) AS [ArticleCnt]
                       FROM [#PublicationsArticles] AS [pub]
                       WHERE [pub].[ReferncedObjectId] = [st].[ObjectID]
                       AND [st].[IsToBeTruncated] = 1
                    ) [art];
        
        SELECT @CountTblsReferencedByArticles = COUNT([Id]) FROM [#SelectedTables] AS [st] WHERE [st].[NumPublArtReferencing] > 0;
        
        IF (@CountTblsReferencedByArticles <> @CountPublishedTablesFound)
        BEGIN
            SELECT @ErrorMessage = CONCAT(
                                            'Number of [#SelectedTables] Referenced by Published Articles: ('
                                           , @CountTblsReferencedByArticles
                                           , ') does not match the Number of [#SelectedTables].[IsPublished] flag: ('
                                           , @CountPublishedTablesFound, ')'
                                         ), @ParamDefinition = NULL, @Id = NULL;
            GOTO ERROR;       
        END;
        
        SELECT @ErrorMessage = CASE
                                   WHEN @DbEngineVersion < 14 THEN STUFF((SELECT ', ' + [TableName] FROM [#SelectedTables] FOR XML PATH(''), TYPE).[value]('.', 'NVARCHAR(MAX)'), 1, 2, '')
                                   ELSE STRING_AGG([TableName], ', ')
                               END
        FROM [#SelectedTables]
        WHERE [IsToBeTruncated] = 1
        AND   [IsPublished] = 1
        AND   [NumPublArtReferencing] < 1;

        IF (@ErrorMessage IS NOT NULL)
        BEGIN
            SELECT @ErrorMessage = CONCAT('The following tables: ', @ErrorMessage
                                        , ' are flagged as published but could not be matched to any published articles'), @ParamDefinition = NULL, @Id = NULL;
            GOTO ERROR; 
        END;
                
        SELECT @publication_id = MIN([publication_id]), @max_publication_id = MAX([publication_id]) FROM [#PublicationsArticles];
        WHILE (@publication_id <= @max_publication_id)
        BEGIN
            SELECT @publication = [publication] FROM [#PublicationsArticles] WHERE [publication_id] = @publication_id;
            
            TRUNCATE TABLE [#sp_helparticle];
            INSERT INTO [#sp_helparticle]
                (
                    [article id]
                  , [article name]
                  , [base object]
                  , [destination object]
                  , [synchronization object]
                  , [type]
                  , [status]
                  , [filter]
                  , [description]
                  , [insert_command]
                  , [update_command]
                  , [delete_command]
                  , [creation script path]
                  , [vertical partition]
                  , [pre_creation_cmd]
                  , [filter_clause]
                  , [schema_option]
                  , [dest_owner]
                  , [source_owner]
                  , [unqua_source_object]
                  , [sync_object_owner]
                  , [unqualified_sync_object]
                  , [filter_owner]
                  , [unqua_filter]
                  , [auto_identity_range]
                  , [publisher_identity_range]
                  , [identity_range]
                  , [threshold]
                  , [identityrangemanagementoption]
                  , [fire_triggers_on_snapshot]
                )
            EXEC sys.sp_helparticle @publication = @publication;
            UPDATE  [pa]
            SET 
                    [pa].[filter]                               = [sp].[filter]
                  , [pa].[source_owner]                         = [sp].[source_owner]        
                  , [pa].[sync_object_owner]                    = [sp].[sync_object_owner]
                  , [pa].[filter_owner]                         = [sp].[filter_owner]
                  , [pa].[source_object]                        = [sp].[unqua_source_object]
                  , [pa].[auto_identity_range]                  = IIF([sp].[auto_identity_range] = 1, 'true', 'false')
                  , [pa].[pub_identity_range]                   = [sp].[publisher_identity_range]
                  , [pa].[identity_range]                       = [sp].[identity_range]
                  , [pa].[threshold]                            = [sp].[threshold]
                  , [pa].[identityrangemanagementoption]        = CASE [sp].[identityrangemanagementoption]
                                                                       WHEN 0 THEN 'none'
                                                                       WHEN 1 THEN 'auto'
                                                                       WHEN 2 THEN 'manual'
                                                                       ELSE '-unknown-'
                                                                   END     
            FROM    [#PublicationsArticles] AS [pa]
            JOIN    [#sp_helparticle] AS [sp]
                ON  [pa].[article_id] = [sp].[article id]
                AND [pa].[publication_id] = @publication_id;        
            
            SELECT @publication_id = MIN([publication_id]) FROM [#PublicationsArticles] WHERE [publication_id] > @publication_id;
        END;
    END;
END;

/* ==================================================================================================================== */
/* ----------------------------------------- END OF COLLECTING METADATA  ---------------------------------------------- */
/* ==================================================================================================================== */

/* ==================================================================================================================== */
/* ----------------------------------------- DROPPING AND DISABLING: -------------------------------------------------- */
/* ==================================================================================================================== */

BEGIN TRANSACTION;

IF (@CountFKFound > 0)
BEGIN
    PRINT ('/*--------------------------------------- DROPPING FK CONSTRAINTS: -----------------------------------------------*/');

    SELECT @Id = MIN([Id]), @IdMax = MAX([Id]) FROM [#ForeignKeyConstraintDefinitions];
    WHILE (@Id <= @IdMax)
    BEGIN               

        SELECT @SqlDropConstraint = [DropConstraintCommand] FROM [#ForeignKeyConstraintDefinitions] WHERE [Id] = @Id;

        IF (@WhatIf = 1)
        BEGIN
            PRINT(@SqlDropConstraint);
        END;
        ELSE
        BEGIN TRY
            EXEC sys.sp_executesql @stmt = @SqlDropConstraint;
            IF (@@ERROR = 0) 
            BEGIN
                SELECT @CountFKDropped = @CountFKDropped + 1;

                -- update NumFkDropped:
                UPDATE [st] SET [st].[NumFkDropped] = COALESCE([st].[NumFkDropped], 0) + 1 
                FROM [#SelectedTables] AS [st] 
                JOIN [#ForeignKeyConstraintDefinitions] AS [fkc] ON [fkc].[ObjectIdTrgt] = [st].[ObjectID] AND [st].[IsToBeTruncated] = 1
                WHERE [fkc].[Id] = @Id;            
            END;
        END TRY
        BEGIN CATCH
                SET @ErrorMessage = CONCAT(ERROR_MESSAGE(), ' - when executing: ', @SqlDropConstraint);
                GOTO ERROR;       
        END CATCH;
        
        SELECT @Id = COALESCE(MIN([Id]), @Id + 1) FROM [#ForeignKeyConstraintDefinitions] WHERE [Id] > @Id;
        IF  (@Id < @IdMax)
        AND (@Id * 100) / @IdMax <> @PercentProcessed AND @WhatIf <> 1
        BEGIN
            SET @PercentProcessed = (@Id * 100) / @IdMax;
            PRINT (CONCAT(@PercentProcessed, ' percent processed.'));
        END;
    END;
    IF (@WhatIf <> 1) AND (@CountFKFound <> @CountFKDropped)
    BEGIN
        SET @ErrorMessage = CONCAT('Number of FK Constraints Found: ', @CountFKFound, ' does not match the Number of FK Constraints dropped: ', @CountFKDropped);
        GOTO ERROR;
    END;
    ELSE
    BEGIN
        IF (@WhatIf <> 1) PRINT (CONCAT('/* Successfully dropped: ', COALESCE(@CountFKDropped, 0), ' FK Constraints (matches the number of FK Constraints Found). */'));
    END;

    PRINT ('/*--------------------------------------- END OF DROPPING FK CONSTRAINTS -----------------------------------------*/');
END;

IF (@CountSchBvFound > 0)
BEGIN
    PRINT ('/*--------------------------------------- DROPPING SCHEMA-BOUND VIEWS: -------------------------------------------*/');

    SELECT @Id = MIN([Id]), @IdMax = MAX([Id]) FROM [#SchemaBoundViews];
    WHILE (@Id <= @IdMax)
    BEGIN             

        SELECT @SqlDropView = [DropViewCommand] FROM [#SchemaBoundViews] WHERE [Id] = @Id;

        IF (@WhatIf = 1)
        BEGIN
            PRINT(@SqlDropView);
            UPDATE [#SchemaBoundViews] SET [Dropped] = 1 WHERE [Id] = @Id
        END;
        ELSE
        BEGIN TRY
            EXEC sys.sp_executesql @stmt = @SqlDropView;
            IF (@@ERROR = 0) 
            BEGIN
                SELECT @CountSchBvDropped = @CountSchBvDropped + 1;
                
                UPDATE [#SchemaBoundViews] SET [Dropped] = 1 WHERE [Id] = @Id

                -- update NumSchBvDropped:
                UPDATE [st]
                SET [st].[NumSchBvDropped] = COALESCE([st].[NumSchBvDropped], 0) + 1
                FROM [#SelectedTables] AS [st]
                WHERE [st].[IsToBeTruncated] = 1 
                AND EXISTS (
                                 SELECT 1
                                 FROM [#SbvToSelTablesLink] AS [sbvcr]
                                 JOIN [#SchemaBoundViews] AS [sbv]
                                     ON  [sbvcr].[SbvObjectId] = [sbv].[SbvObjectId]
                                     AND [sbvcr].[ReferncedObjectId] = [st].[ObjectID]
                                     AND [sbv].[Id] = @Id
                             );            
            END;
        END TRY
        BEGIN CATCH
                SET @ErrorMessage = CONCAT(ERROR_MESSAGE(), ' - when executing: ', @SqlDropView);
                GOTO ERROR;        
        END CATCH;
        
        SELECT @Id = COALESCE(MIN([Id]), @Id + 1) FROM [#SchemaBoundViews] WHERE [Id] > @Id;
    END;
    IF (@WhatIf <> 1) AND (@CountSchBvFound <> @CountSchBvDropped)
    BEGIN
        SET @ErrorMessage = CONCAT('Number of Schema-Bound Views Found: ', @CountSchBvFound, ' does not match the Number of Schema-Bound Views dropped: ', @CountSchBvDropped);
        GOTO ERROR;
    END;
    ELSE
    BEGIN
        IF (@WhatIf <> 1) PRINT (CONCAT('/* Successfully dropped: ', COALESCE(@CountSchBvDropped, 0), ' Schema-Bound Views (matches the number of Schema-Bound Views Found). */'));
    END;

    PRINT ('/*--------------------------------------- END OF DROPPING SCHEMA-BOUND VIEWS -------------------------------------*/');
END;

IF (@CountTblsCDCEnabled > 0)
BEGIN
    PRINT ('/*--------------------------------------- DISABLING CDC Instances: ------------------------------------------------*/');

    SELECT @Id = MIN([Id]), @IdMax = MAX([Id]) FROM [#CDCInstances];
    WHILE (@Id <= @IdMax)
    BEGIN                  

        SELECT
              @CDC_source_schema    = [source_schema]                   
            , @CDC_source_name      = [source_name]           
            , @CDC_capture_instance = [capture_instance]      
        FROM [#CDCInstances]
        WHERE [Id] = @Id;

        IF (@WhatIf = 1)
        BEGIN
            PRINT(CONCAT(
                           'EXEC sys.sp_cdc_disable_table @source_schema = '''
                         , @CDC_source_schema
                         , ''', @source_name = '''
                         , @CDC_source_name
                         , ''', @capture_instance = '''
                         , @CDC_capture_instance, ''''
                 ));                
        END;
        ELSE
        BEGIN TRY
            EXEC sys.sp_cdc_disable_table @source_schema = @CDC_source_schema
                                        , @source_name = @CDC_source_name
                                        , @capture_instance = @CDC_capture_instance;       
            IF (@@ERROR = 0) 
            BEGIN
                SELECT @CountCDCInstDisabled = @CountCDCInstDisabled + 1;
                
                UPDATE [st]
                SET [st].[NumCDCInstDisabled] = COALESCE([st].[NumCDCInstDisabled], 0) + 1
                FROM [#SelectedTables] AS [st]
                WHERE [st].[IsToBeTruncated] = 1 
                AND EXISTS (
                              SELECT 1
                              FROM [#CDCInstances] AS [cdc]
                              WHERE [cdc].[ReferncedObjectId] = [st].[ObjectID]
                              AND [cdc].[Id] = @Id
                           );
            END;
        END TRY
        BEGIN CATCH
                SET @ErrorMessage
                    = CONCAT(   ERROR_MESSAGE()
                              , ' - Error while executing sys.sp_cdc_disable_table with parameters @source_schema: '
                              , @CDC_source_schema
                              , ' @source_name: '
                              , @CDC_source_name
                              , ' @capture_instance: '
                              , @CDC_capture_instance
                            );
                GOTO ERROR;
        END CATCH;            

        SELECT @Id = COALESCE(MIN([Id]), @Id + 1) FROM [#CDCInstances] WHERE [Id] > @Id;
        IF  (@Id < @IdMax)
        AND (@Id * 100) / @IdMax <> @PercentProcessed AND @WhatIf <> 1
        BEGIN
            SET @PercentProcessed = (@Id * 100) / @IdMax;
            PRINT (CONCAT(@PercentProcessed, ' percent processed.'));
        END;
    END;
    IF (@WhatIf <> 1) AND (@CountCDCInstFound <> @CountCDCInstDisabled)
    BEGIN
        SET @ErrorMessage = CONCAT('Number of CDC-Instances Found: ', @CountCDCInstFound, ' does not match the Number of CDC-Instances Disabled: ', @CountCDCInstDisabled);
        GOTO ERROR;
    END;
    ELSE
    BEGIN
        IF (@WhatIf <> 1) PRINT (CONCAT('/* Successfully disabled: ', COALESCE(@CountCDCInstDisabled, 0), ' CDC Instances (matches the number of CDC-Instances Found). */'));
    END;

    PRINT ('/*--------------------------------------- END OF DISABLING CDC Instances ------------------------------------------*/');
END;

IF (@CountPublishedArticlesFound > 0)
BEGIN
    PRINT ('/*--------------------------------------- DROPPING PUBLISHED ARTICLES: -------------------------------------------*/');

    SELECT @Id = MIN([Id]), @IdMax = MAX([Id]) FROM [#PublicationsArticles];
    WHILE (@Id <= @IdMax)
    BEGIN        
        
        SELECT @publication = [publication]
             , @article = [article]
        FROM [#PublicationsArticles]
        WHERE [Id] = @Id;

        IF (@WhatIf = 1)
        BEGIN
        PRINT(CONCAT(
                         'EXEC sys.sp_droparticle @publication = '''
                       , @publication
                       , ''', @article = '''
                       , @article, ''''
                    ));
        END;
        ELSE 
        BEGIN TRY
            EXEC sys.sp_droparticle @publication               = @publication
                                  , @article                   = @article
                                  , @force_invalidate_snapshot = 1;       
            IF (@@ERROR = 0) 
            BEGIN
                SELECT @CountPublishedArticlesDropped = @CountPublishedArticlesDropped + 1;
                -- update PublishedArticlesDropped:
                UPDATE [st]
                SET [st].[NumPublArtDropped] = COALESCE([st].[NumPublArtDropped], 0) + 1
                FROM [#SelectedTables] AS [st]
                WHERE [st].[IsToBeTruncated] = 1 
                AND EXISTS (
                                 SELECT 1
                                 FROM [#PublicationsArticles] AS [pa]
                                 WHERE [pa].[ReferncedObjectId] = [st].[ObjectID]
                                 AND [pa].[Id] = @Id
                             ); 
            END;
        END TRY
        BEGIN CATCH
                SET @ErrorMessage
                    = CONCAT( ERROR_MESSAGE()
                              , ' - Error while executing sys.sp_droparticle with parameters @publication: '
                              , @publication
                              , ' @article: '
                              , @article
                            );
                GOTO ERROR;        
        END CATCH;
                
        SELECT @Id = COALESCE(MIN([Id]), @Id + 1) FROM [#PublicationsArticles] WHERE [Id] > @Id;
        IF  (@Id < @IdMax)
        AND (@Id * 100) / @IdMax <> @PercentProcessed AND @WhatIf <> 1
        BEGIN
            SET @PercentProcessed = (@Id * 100) / @IdMax;
            PRINT (CONCAT(@PercentProcessed, ' percent processed.'));
        END;
    END;
    IF (@WhatIf <> 1) AND (@CountPublishedArticlesFound <> @CountPublishedArticlesDropped)
    BEGIN
        SET @ErrorMessage = CONCAT('Number of Published Articles Found: ', @CountPublishedArticlesFound, ' does not match the Number of Published Articles Dropped: ', @CountPublishedArticlesDropped);
        GOTO ERROR;
    END;
    ELSE
    BEGIN
        IF (@WhatIf <> 1) PRINT (CONCAT('/* Successfully dropped : ', COALESCE(@CountPublishedArticlesDropped, 0), ' Published Articles (matches the number of Published Articles Found). */'));
    END;

    PRINT ('/*--------------------------------------- END OF DROPPING PUBLISHED ARTICLES -------------------------------------*/');
END;

/* ==================================================================================================================== */
/* ----------------------------------------- END OF DROPPING AND DISABLING  ------------------------------------------- */
/* ==================================================================================================================== */

/* ==================================================================================================================== */
/* ----------------------------------------- TRUNCATING TABLES: ------------------------------------------------------- */
/* ==================================================================================================================== */

IF (@CountTablesSelected > 0)
BEGIN
    PRINT ('/*--------------------------------------- TRUNCATING TABLES: -----------------------------------------------------*/');

    SELECT @Id = MIN([Id]), @IdMax = MAX([Id]) FROM [#SelectedTables] WHERE [IsToBeTruncated] = 1;
    WHILE (@Id <= @IdMax)
    BEGIN        

        SELECT @SchemaName = [SchemaName]
             , @TableName = [TableName]
             , @TemporalType = [TemporalType]
        FROM [#SelectedTables]
        WHERE [IsToBeTruncated] = 1 AND [Id] = @Id;

        --SELECT * FROM [#SelectedTables]

        IF (@TemporalType = 1)
        BEGIN
            SET @ErrorMessage = CONCAT('Temporal Table name: ', QUOTENAME(@SchemaName), '.', QUOTENAME(@TableName)
                                    , ' is of type 1 HISTORY_TABLE and can not be truncated, remove it from the list of @TableNames')
            GOTO ERROR
        END        

        IF (@TemporalType = 2)
        BEGIN
            PRINT(CONCAT('Temporal Table name: ', QUOTENAME(@SchemaName), '.', QUOTENAME(@TableName), '; '))
        END
        
        SELECT @SqlTruncateTable = CONCAT('TRUNCATE TABLE ', QUOTENAME(@SchemaName), '.', QUOTENAME(@TableName), '; ')
             , @SqlSetIsTruncated = CONCAT('IF EXISTS (SELECT 1 FROM [', [SchemaName], '].[', [TableName], ']) SET @_IsTruncated = 0 ELSE SET @_IsTruncated = 1;')
        FROM [#SelectedTables]
        WHERE [IsToBeTruncated] = 1 AND [Id] = @Id;
        
        IF (@WhatIf = 1)
        BEGIN
            PRINT(@SqlTruncateTable);
            PRINT('GO');
        END;
        ELSE        
        BEGIN TRY
            EXEC sys.sp_executesql @stmt = @SqlTruncateTable;
        END TRY
        BEGIN CATCH
                SET @ErrorMessage = CONCAT(ERROR_MESSAGE(), ' - when executing: ', @SqlTruncateTable);
                GOTO ERROR;
        END CATCH;  
        
        SET @ParamDefinition = N'@_IsTruncated BIT OUTPUT';
        EXEC sys.sp_executesql @stmt = @SqlSetIsTruncated, @params = @ParamDefinition, @_IsTruncated = @IsTruncated OUTPUT;

        IF (@IsTruncated = 1) OR (@WhatIf = 1)
        BEGIN                       
            SELECT @CountTablesTruncated = @CountTablesTruncated + 1;
            UPDATE [#SelectedTables] SET [IsTruncated] = @IsTruncated WHERE [IsToBeTruncated] = 1 AND [Id] = @Id;                            
            SELECT @SqlUpdateStatistics = CONCAT('UPDATE STATISTICS ', QUOTENAME(@SchemaName), '.', QUOTENAME(@TableName), ' WITH ROWCOUNT = 0;');

            IF (@WhatIf = 1)
            BEGIN
                PRINT(@SqlUpdateStatistics);
                PRINT('GO');
            END;
            ELSE
            BEGIN TRY
                EXEC sys.sp_executesql @stmt = @SqlUpdateStatistics;       
            END TRY
            BEGIN CATCH
                    SET @ErrorMessage = CONCAT(ERROR_MESSAGE(), ' - when executing: ', @SqlUpdateStatistics);
                    GOTO ERROR;
            END CATCH;                                   
        END;
        
        SELECT @Id = COALESCE(MIN([Id]), @Id + 1) FROM [#SelectedTables] WHERE [IsToBeTruncated] = 1 AND [Id] > @Id;
        IF (@Id < @IdMax) AND (@Id * 100) / @IdMax <> @PercentProcessed AND @WhatIf <> 1
        BEGIN
            SET @PercentProcessed = (@Id * 100) / @IdMax;
            PRINT (CONCAT(@PercentProcessed, ' percent processed.'));
        END;
    END;

    IF (@WhatIf <> 1) AND (@CountTablesTruncated <> @CountTablesSelected)
    BEGIN
        SET @ErrorMessage = CONCAT('Number of Tables truncated: ', @CountTablesTruncated, ' does not match the Number of Tables Selected: ', @CountTablesSelected);      
        GOTO ERROR;
    END;
    ELSE
    BEGIN
        IF (@WhatIf <> 1) PRINT (CONCAT('/* Successfully truncated : ', COALESCE(@CountTablesTruncated, 0), ' Tables (matches the number of Tables Selected). */'));
    END;
    PRINT ('/*--------------------------------------- END OF TRUNCATING TABLES -----------------------------------------------*/');
END;
ELSE
BEGIN
    SET @ErrorMessage = CONCAT('@CountTablesSelected = ', @CountTablesSelected, ', nothing to truncate - check @RowCountThreshold: ', @RowCountThreshold, ' and [RowCountBefore] values'
                             , IIF(LEN(@SchemaNamesExpt) > 0 OR LEN(@TableNamesExpt) > 0, ' as well as your @SchemaNamesExpt/@TableNamesExpt', ''));
    RAISERROR(@ErrorMessage, @ErrorSeverity11, 1) WITH NOWAIT;
    SET @ErrorMessage = NULL;
END;

/* ==================================================================================================================== */
/* ----------------------------------------- END OF TRUNCATING TABLES  ------------------------------------------------ */
/* ==================================================================================================================== */

/* ==================================================================================================================== */
/* ----------------------------------------- RECREATING AND RE-ENABLING: ---------------------------------------------- */
/* ==================================================================================================================== */

/* from this point on @ContinueOnError determines if an error rolls-back the whole transaction 
    or if the error is logged into a temp table and the execution continues
*/

IF (@CountPublishedArticlesFound > 0) AND (@RecreatePublishedArticles = 1)
BEGIN
    PRINT ('/*--------------------------------------- RECREATING PUBLISHED ARTICLES: -----------------------------------------*/');

    SELECT @Id = MIN([Id]), @IdMax = MAX([Id]) FROM [#PublicationsArticles];
    WHILE (@Id <= @IdMax)
    BEGIN        
        
        SELECT @SqlRecreatePublishedArticle = CONCAT('EXEC sys.sp_addarticle '
        , @crlf, '  @publication                    = N''', [publication]                                    , ''''
        , @crlf, ', @article                        = N''', [article]                                        , ''''
        , @crlf, ', @destination_table              = ', COALESCE('N'''+[destination_table]             +'''', 'NULL')
        , @crlf, ', @vertical_partition             = N''', [vertical_partition]                             , ''''
        , @crlf, ', @type                           = ', COALESCE('N'''+[type]                          +'''', 'NULL')
        , @crlf, ', @filter                         = ', COALESCE('N'''+[filter]                        +'''', 'NULL')
        , @crlf, ', @ins_cmd                        = ', COALESCE('N'''+[ins_cmd]                       +'''', 'NONE')
        , @crlf, ', @del_cmd                        = ', COALESCE('N'''+[del_cmd]                       +'''', 'NONE')
        , @crlf, ', @upd_cmd                        = ', COALESCE('N'''+[upd_cmd]                       +'''', 'NONE')
        , @crlf, ', @creation_script                = ', COALESCE('N'''+[creation_script]               +'''', 'NULL')
        , @crlf, ', @description                    = ', COALESCE('N'''+[description]                   +'''', 'NULL')
        , @crlf, ', @pre_creation_cmd               = ', COALESCE('N'''+[pre_creation_cmd]              +'''', 'NULL')
        , @crlf, ', @filter_clause                  = ', COALESCE('N'''+[filter_clause]                 +'''', 'NULL')
        , @crlf, ', @schema_option                  = ', COALESCE(CONVERT(NVARCHAR(MAX), [schema_option], 1) , 'NULL')
        , @crlf, ', @destination_owner              = ', COALESCE('N'''+[destination_owner]             +'''', 'NULL')
        --, @crlf, ', @status                         = ', [status]
        , @crlf, ', @source_owner                   = ', COALESCE('N'''+[source_owner]                  +'''', 'NULL')
        , @crlf, ', @sync_object_owner              = ', COALESCE('N'''+[sync_object_owner]             +'''', 'NULL')
        , @crlf, ', @filter_owner                   = ', COALESCE('N'''+[filter_owner]                  +'''', 'NULL')
        , @crlf, ', @source_object                  = ', COALESCE('N'''+[source_object]                 +'''', 'NULL')
        , @crlf, ', @auto_identity_range            = ', COALESCE('N'''+[auto_identity_range]           +'''', 'NULL')
        , @crlf, ', @pub_identity_range             = ', COALESCE(CAST([pub_identity_range] AS VARCHAR(32))  , 'NULL')
        , @crlf, ', @identity_range                 = ', COALESCE(CAST([identity_range] AS VARCHAR(32))      , 'NULL')
        , @crlf, ', @threshold                      = ', COALESCE(CAST([threshold] AS VARCHAR(32))           , 'NULL')
        , @crlf, ', @force_invalidate_snapshot      = ', [force_invalidate_snapshot]
        , @crlf, ', @use_default_datatypes          = ', [use_default_datatypes]
        , @crlf, ', @identityrangemanagementoption  = ', COALESCE('N'''+[identityrangemanagementoption] +'''', 'NULL')
        , @crlf, ', @publisher                      = ', COALESCE('N'''+[publisher]                     +'''', 'NULL')
        , @crlf, ', @fire_triggers_on_snapshot      = ', COALESCE('N'''+[fire_triggers_on_snapshot]     +'''', 'NULL')
        )
        FROM [#PublicationsArticles] WHERE [Id] = @Id;

        IF (@WhatIf = 1)
        BEGIN
            PRINT(@SqlRecreatePublishedArticle);
            PRINT('GO');
        END;
        ELSE                 
        BEGIN TRY 
            IF (@ContinueOnError = 1 AND XACT_STATE() <> 0 AND @@TRANCOUNT > 0)
            BEGIN
                COMMIT TRANSACTION;
                BEGIN TRANSACTION;
            END;       
            EXEC sys.sp_executesql @stmt = @SqlRecreatePublishedArticle;
            IF (@ContinueOnError = 1 AND XACT_STATE() <> 0 AND @@TRANCOUNT > 0)
            BEGIN
                COMMIT TRANSACTION;
                BEGIN TRANSACTION;
            END;            
            IF (@@ERROR = 0) 
            BEGIN
                SELECT @CountPublishedArticlesRecreated = @CountPublishedArticlesRecreated + 1;
                
                UPDATE [st]
                SET [st].[NumPublArtRecreated] = COALESCE([st].[NumPublArtRecreated], 0) + 1
                FROM [#SelectedTables] AS [st]
                WHERE [st].[IsToBeTruncated] = 1 
                AND EXISTS (
                              SELECT 1
                              FROM [#PublicationsArticles] AS [pa]
                              WHERE [pa].[ReferncedObjectId] = [st].[ObjectID]
                              AND [pa].[Id] = @Id
                           );
            END;
        END TRY        
        BEGIN CATCH 
              SET @ErrorMessage = CONCAT(ERROR_MESSAGE(), ' when executing: ', @SqlRecreatePublishedArticle);
              IF (@ContinueOnError <> 1)
                  GOTO ERROR;
              ELSE /* continue execution but log the error: */
              BEGIN
                RAISERROR(@ErrorMessage, @ErrorSeverity11, 1) WITH NOWAIT;                
                IF (XACT_STATE() <> 0 AND @@TRANCOUNT > 0)
                BEGIN
                    ROLLBACK TRANSACTION;
                    BEGIN TRANSACTION;
                END;
                
                SET @ParamDefinition = '@_ErrorMessage NVARCHAR(4000), @_Id INT';
                SET @SqlLogError = 'UPDATE [#PublicationsArticles] SET [ErrorMessage] = CONCAT([ErrorMessage]+''; '', @_ErrorMessage) WHERE [Id] = @_Id';
                EXEC sys.sp_executesql @stmt = @SqlLogError, @params = @ParamDefinition, @_ErrorMessage = @ErrorMessage, @_Id = @Id;
                SET @ErrorMessage = NULL
                
                IF (XACT_STATE() <> 0 AND @@TRANCOUNT > 0)
                BEGIN
                    COMMIT TRANSACTION;
                    BEGIN TRANSACTION;
                END;
                SET @ErrorMessage = NULL;
              END;
        END CATCH; 
        
        SELECT @Id = COALESCE(MIN([Id]), @Id + 1) FROM [#PublicationsArticles] WHERE [Id] > @Id;
        IF  (@Id < @IdMax)
        AND (@Id * 100) / @IdMax <> @PercentProcessed AND @WhatIf <> 1
        BEGIN
            SET @PercentProcessed = (@Id * 100) / @IdMax;
            PRINT (CONCAT(@PercentProcessed, ' percent processed.'));
        END;
    END;
    IF (@WhatIf <> 1) AND (COALESCE(@CountPublishedArticlesRecreated, 0) <> COALESCE(@CountPublishedArticlesDropped, 0))
    BEGIN        
        SET @ErrorMessage = CONCAT('Number of Published Articles Recreated: ', COALESCE(@CountPublishedArticlesRecreated, 0), ' does not match the Number of Published Articles Dropped: ', COALESCE(@CountPublishedArticlesDropped, 0)); 
        
        IF (@ContinueOnError <> 1)
            GOTO ERROR;
        ELSE /* continue execution but log the error: */
        BEGIN
          RAISERROR(@ErrorMessage, @ErrorSeverity11, 1) WITH NOWAIT;
          
          SET @ParamDefinition = '@_ErrorMessage NVARCHAR(4000), @_Id INT';        
          SET @SqlLogError = 'UPDATE [#PublicationsArticles] SET [ErrorMessage] = CONCAT([ErrorMessage]+''; '', @_ErrorMessage);';        
          EXEC sys.sp_executesql @stmt = @SqlLogError, @params = @ParamDefinition, @_ErrorMessage = @ErrorMessage, @_Id = @Id;
          SET @ErrorMessage = NULL
          
          IF (XACT_STATE() <> 0 AND @@TRANCOUNT > 0)
          BEGIN
              COMMIT TRANSACTION;
              BEGIN TRANSACTION;
          END;          
          SET @ErrorMessage = NULL;
        END;
    END;
    ELSE
    BEGIN
        IF (@WhatIf <> 1) PRINT (CONCAT('/* Successfully recreated : ', COALESCE(@CountPublishedArticlesRecreated, 0), ' Published Articles (matches the number of Published Articles Dropped). */'));
    END;
END;

IF  (@CountTblsCDCEnabled > 0) AND (@ReenableCDC = 1)
BEGIN
    PRINT ('/*--------------------------------------- RE-ENABLING CDC: -------------------------------------------------------*/');

    SELECT @Id = MIN([Id]), @IdMax = MAX([Id]) FROM [#CDCInstances];
    WHILE (@Id <= @IdMax)
    BEGIN
    
        SELECT @SqlReenableCDCInstance = CONCAT('EXEC sys.sp_cdc_enable_table '
             , @crlf, '  @source_schema          = ', COALESCE('N'''+[source_schema]          +'''', 'NULL')
             , @crlf, ', @source_name            = ', COALESCE('N'''+[source_name]            +'''', 'NULL')
             , @crlf, ', @capture_instance       = ', COALESCE('N'''+[capture_instance]       +'''', 'NULL')
             , @crlf, ', @supports_net_changes   = ', [supports_net_changes]
             , @crlf, ', @role_name              = ', COALESCE('N'''+[role_name]              +'''', 'NULL')
             , @crlf, ', @index_name             = ', COALESCE('N'''+[index_name]             +'''', 'NULL')
             , @crlf, ', @captured_column_list   = ', COALESCE('N'''+[captured_column_list]   +'''', 'NULL')
             , @crlf, ', @filegroup_name         = ', COALESCE('N'''+[filegroup_name]         +'''', 'NULL')
             , @crlf, ', @allow_partition_switch = ', [allow_partition_switch]
             )
        FROM [#CDCInstances]
        WHERE [Id] = @Id;

        IF (@WhatIf = 1)
        BEGIN
            PRINT(@SqlReenableCDCInstance);
            PRINT('GO');
        END;
        ELSE         
        BEGIN TRY            
            IF (@ContinueOnError = 1 AND XACT_STATE() <> 0 AND @@TRANCOUNT > 0)
            BEGIN
                COMMIT TRANSACTION;
                BEGIN TRANSACTION;
            END;
            EXEC sys.sp_executesql @stmt = @SqlReenableCDCInstance;
            IF (@ContinueOnError = 1 AND XACT_STATE() <> 0 AND @@TRANCOUNT > 0)
            BEGIN
                COMMIT TRANSACTION;
                BEGIN TRANSACTION;
            END;            
            IF (@ErrorMessage IS NULL) 
            BEGIN
                SELECT @CountCDCInstReenabled = @CountCDCInstReenabled + 1;
                
                UPDATE [st]
                SET [st].[NumCDCInstReenabled] = COALESCE([st].[NumCDCInstReenabled], 0) + 1
                FROM [#SelectedTables] AS [st]
                WHERE [st].[IsToBeTruncated] = 1 
                AND EXISTS (
                              SELECT 1
                              FROM [#CDCInstances] AS [cdc]
                              WHERE [cdc].[ReferncedObjectId] = [st].[ObjectID]
                              AND [cdc].[Id] = @Id
                           );
            END;
        END TRY
        BEGIN CATCH 
              SET @ErrorMessage = CONCAT(ERROR_MESSAGE(), ' when executing: ', @SqlReenableCDCInstance);
              IF (@ContinueOnError <> 1)
                  GOTO ERROR;
              ELSE /* continue execution but log the error: */
              BEGIN
                RAISERROR(@ErrorMessage, @ErrorSeverity11, 1) WITH NOWAIT;                
                IF (XACT_STATE() <> 0 AND @@TRANCOUNT > 0)
                BEGIN
                    ROLLBACK TRANSACTION;
                    BEGIN TRANSACTION;
                END;
                
                SET @ParamDefinition = '@_ErrorMessage NVARCHAR(4000), @_Id INT';
                SET @SqlLogError = 'UPDATE [#CDCInstances] SET [ErrorMessage] = CONCAT([ErrorMessage]+''; '', @_ErrorMessage) WHERE [Id] = @_Id';
                EXEC sys.sp_executesql @stmt = @SqlLogError, @params = @ParamDefinition, @_ErrorMessage = @ErrorMessage, @_Id = @Id;
                SET @ErrorMessage = NULL
                
                IF (XACT_STATE() <> 0 AND @@TRANCOUNT > 0)
                BEGIN
                    COMMIT TRANSACTION;
                    BEGIN TRANSACTION;
                END;
                SET @ErrorMessage = NULL;
              END;
        END CATCH; 

        SELECT @Id = COALESCE(MIN([Id]), @Id + 1) FROM [#CDCInstances] WHERE [Id] > @Id;
        IF  (@Id < @IdMax)
        AND (@Id * 100) / @IdMax <> @PercentProcessed AND @WhatIf <> 1
        BEGIN
            SET @PercentProcessed = (@Id * 100) / @IdMax;
            PRINT (CONCAT(@PercentProcessed, ' percent processed.'));
        END;
    END;
    IF (@WhatIf <> 1) AND (COALESCE(@CountCDCInstReenabled, 0) <> COALESCE(@CountCDCInstDisabled, 0))
    BEGIN        
        SET @ErrorMessage = CONCAT('Number of CDC-Instances Reenabled: ', COALESCE(@CountCDCInstReenabled, 0), ' does not match the Number of CDC-Instances Disabled: ', COALESCE(@CountCDCInstDisabled, 0)); 
        
        IF (@ContinueOnError <> 1)
            GOTO ERROR;
        ELSE /* continue execution but log the error: */
        BEGIN
          RAISERROR(@ErrorMessage, @ErrorSeverity11, 1) WITH NOWAIT;
          
          SET @ParamDefinition = '@_ErrorMessage NVARCHAR(4000), @_Id INT';        
          SET @SqlLogError = 'UPDATE [#CDCInstances] SET [ErrorMessage] = CONCAT([ErrorMessage]+''; '', @_ErrorMessage);';        
          EXEC sys.sp_executesql @stmt = @SqlLogError, @params = @ParamDefinition, @_ErrorMessage = @ErrorMessage, @_Id = @Id;
          SET @ErrorMessage = NULL
          
          IF (XACT_STATE() <> 0 AND @@TRANCOUNT > 0)
          BEGIN
              COMMIT TRANSACTION;
              BEGIN TRANSACTION;
          END;          
          SET @ErrorMessage = NULL;
        END;
    END;
    ELSE
    BEGIN
        IF (@WhatIf <> 1) PRINT (CONCAT('/* Successfully re-enabled : ', COALESCE(@CountCDCInstReenabled, 0), ' CDC-Instances (matches the number of CDC-Instances Disabled). */'));
    END;

    PRINT ('/*--------------------------------------- END OF RE-ENABLING CDC -------------------------------------------------*/');
END;

IF (@CountSchBvFound > 0)
BEGIN
    PRINT ('/*--------------------------------------- RECREATING SCHEMA-BOUND VIEWS: -----------------------------------------*/');

    SELECT @Id = MIN([Id]), @IdMax = MAX([Id]) FROM [#SchemaBoundViews];
    WHILE (@Id <= @IdMax)
    BEGIN
        SELECT  @SchBvName = [ReferencingObjectName]
              , @SqlRecreateView = [RecreateViewCommand] 
              , @IsEncrypted = [IsEncrypted]
        FROM    [#SchemaBoundViews] 
        WHERE   [Id] = @Id;
        
        IF (@IsEncrypted = 1 AND @SqlRecreateView IS NULL)
        BEGIN
            SELECT @ErrorMessage = CONCAT('Definition of Schema-Bound View: ', QUOTENAME(@SchBvName), ' is encrypted, unable to recreate this view and any indexes/triggers depending on it');
            IF (@WhatIf = 1)
            BEGIN 
                PRINT(CONCAT('/* !!! Warning: ', @ErrorMessage, ' */'))
            END
            ELSE 
            BEGIN
                IF (@ContinueOnError <> 1) GOTO ERROR
                ELSE 
                BEGIN
                        RAISERROR(@ErrorMessage, @ErrorSeverity11, 1) WITH NOWAIT;  
                        SET @ParamDefinition = '@_ErrorMessage NVARCHAR(4000), @_Id INT';
                        SET @SqlLogError = 'UPDATE [#SchemaBoundViews] SET [ErrorMessage] = CONCAT([ErrorMessage]+''; '', @_ErrorMessage) WHERE [Id] = @_Id';
                        EXEC sys.sp_executesql @stmt = @SqlLogError, @params = @ParamDefinition, @_ErrorMessage = @ErrorMessage, @_Id = @Id;
                        SET @ErrorMessage = NULL
                END            
            END 
        END
        ELSE 
        BEGIN               
            IF (@WhatIf = 1)
            BEGIN
                PRINT(@SqlRecreateView);
                PRINT('GO');
                UPDATE [#SchemaBoundViews] SET [Recreated] = 1 WHERE [Id] = @Id
            END;
            ELSE         
            BEGIN TRY            

                IF (@ContinueOnError = 1 AND XACT_STATE() <> 0 AND @@TRANCOUNT > 0)
                BEGIN
                    COMMIT TRANSACTION;
                    BEGIN TRANSACTION;
                END;
                EXEC sys.sp_executesql @stmt = @SqlRecreateView;
                IF (@ContinueOnError = 1 AND XACT_STATE() <> 0 AND @@TRANCOUNT > 0)
                BEGIN
                    COMMIT TRANSACTION;
                    BEGIN TRANSACTION;
                END;            
                IF (@ErrorMessage IS NULL) 
                BEGIN
                    SELECT @CountSchBvRecreated = @CountSchBvRecreated + 1;
                    
                    UPDATE [#SchemaBoundViews] SET [Recreated] = 1 WHERE [Id] = @Id
                    
                    UPDATE [st]
                    SET [st].[NumSchBvRecreated] = COALESCE([st].[NumSchBvRecreated], 0) + 1
                    FROM [#SelectedTables] AS [st]
                    WHERE [IsToBeTruncated] = 1 
                    AND EXISTS (
                                     SELECT 1
                                     FROM [#SbvToSelTablesLink] AS [sbvcr]
                                     JOIN [#SchemaBoundViews] AS [sbv]
                                         ON  [sbvcr].[SbvObjectId] = [sbv].[SbvObjectId]
                                         AND [sbvcr].[ReferncedObjectId] = [st].[ObjectID]
                                         AND [sbv].[Id] = @Id
                                 );
                END;
            END TRY
            BEGIN CATCH 
                  SET @ErrorMessage = CONCAT(ERROR_MESSAGE(), ' when executing: ', @SqlRecreateView);
                  IF (@ContinueOnError <> 1)
                      GOTO ERROR;
                  ELSE /* continue execution but log the error: */
                  BEGIN
                    RAISERROR(@ErrorMessage, @ErrorSeverity11, 1) WITH NOWAIT;                
                    IF (XACT_STATE() <> 0 AND @@TRANCOUNT > 0)
                    BEGIN
                        ROLLBACK TRANSACTION;
                        BEGIN TRANSACTION;
                    END;
                    
                    SET @ParamDefinition = '@_ErrorMessage NVARCHAR(4000), @_Id INT';
                    SET @SqlLogError = 'UPDATE [#SchemaBoundViews] SET [ErrorMessage] = CONCAT([ErrorMessage]+''; '', @_ErrorMessage) WHERE [Id] = @_Id';
                    EXEC sys.sp_executesql @stmt = @SqlLogError, @params = @ParamDefinition, @_ErrorMessage = @ErrorMessage, @_Id = @Id;
                    SET @ErrorMessage = NULL
                    
                    IF (XACT_STATE() <> 0 AND @@TRANCOUNT > 0)
                    BEGIN
                        COMMIT TRANSACTION;
                        BEGIN TRANSACTION;
                    END;
                    SET @ErrorMessage = NULL;
                  END;
            END CATCH; 
            

            SELECT @SqlXtndProperties = [XtdProperties] FROM [#SchemaBoundViews] WHERE [Id] = @Id;

            IF (@SqlXtndProperties IS NOT NULL) AND (@WhatIf = 1)
            BEGIN
                PRINT(@SqlXtndProperties);
                PRINT('GO');
            END;
            ELSE
            BEGIN TRY
                IF (@ContinueOnError = 1 AND XACT_STATE() <> 0 AND @@TRANCOUNT > 0)
                BEGIN
                    COMMIT TRANSACTION;
                    BEGIN TRANSACTION;
                END;
                EXEC sys.sp_executesql @stmt = @SqlXtndProperties;
                IF (@ContinueOnError = 1 AND XACT_STATE() <> 0 AND @@TRANCOUNT > 0)
                BEGIN
                    COMMIT TRANSACTION;
                    BEGIN TRANSACTION;
                END;
            END TRY
            BEGIN CATCH 
                  SET @ErrorMessage = CONCAT(ERROR_MESSAGE(), ' when executing: ', @SqlXtndProperties);
                  IF (@ContinueOnError <> 1)
                      GOTO ERROR;
                  ELSE /* continue execution but log the error: */
                  BEGIN
                    RAISERROR(@ErrorMessage, @ErrorSeverity11, 1) WITH NOWAIT;                
                    IF (XACT_STATE() <> 0 AND @@TRANCOUNT > 0)
                    BEGIN
                        ROLLBACK TRANSACTION;
                        BEGIN TRANSACTION;
                    END;
                    
                    SET @ParamDefinition = '@_ErrorMessage NVARCHAR(4000), @_Id INT';
                    SET @SqlLogError = 'UPDATE [#SchemaBoundViews] SET [ErrorMessage] = CONCAT([ErrorMessage]+''; '', @_ErrorMessage) WHERE [Id] = @_Id';
                    EXEC sys.sp_executesql @stmt = @SqlLogError, @params = @ParamDefinition, @_ErrorMessage = @ErrorMessage, @_Id = @Id;
                    SET @ErrorMessage = NULL
                    
                    IF (XACT_STATE() <> 0 AND @@TRANCOUNT > 0)
                    BEGIN
                        COMMIT TRANSACTION;
                        BEGIN TRANSACTION;
                    END;
                    SET @ErrorMessage = NULL;
                  END;
            END CATCH;
        END;
        
        SELECT @Id = COALESCE(MIN([Id]), @Id + 1) FROM [#SchemaBoundViews] WHERE [Id] > @Id;
        IF  (@Id < @IdMax) AND (@Id * 100) / @IdMax <> @PercentProcessed AND @WhatIf <> 1
        BEGIN
            SET @PercentProcessed = (@Id * 100) / @IdMax;
            PRINT (CONCAT(@PercentProcessed, ' percent processed.'));
        END;
    END;
    IF (@WhatIf <> 1) AND (COALESCE(@CountSchBvRecreated, 0) <> COALESCE(@CountSchBvDropped, 0))
    BEGIN        
        SET @ErrorMessage = CONCAT('Number of Schema-Bound Views Recreated: ', COALESCE(@CountSchBvRecreated, 0), ' does not match the Number of Schema-Bound Views Dropped: ', COALESCE(@CountSchBvDropped, 0)); 
        
        IF (@ContinueOnError <> 1)
            GOTO ERROR;
        ELSE /* continue execution but log the error: */
        BEGIN
          RAISERROR(@ErrorMessage, @ErrorSeverity11, 1) WITH NOWAIT;
          
          SET @ParamDefinition = '@_ErrorMessage NVARCHAR(4000), @_Id INT';        
          SET @SqlLogError = 'UPDATE [#SchemaBoundViews] SET [ErrorMessage] = CONCAT([ErrorMessage]+''; '', @_ErrorMessage);';        
          EXEC sys.sp_executesql @stmt = @SqlLogError, @params = @ParamDefinition, @_ErrorMessage = @ErrorMessage, @_Id = @Id;
          SET @ErrorMessage = NULL
          
          IF (XACT_STATE() <> 0 AND @@TRANCOUNT > 0)
          BEGIN
              COMMIT TRANSACTION;
              BEGIN TRANSACTION;
          END;          
          SET @ErrorMessage = NULL;
        END;
    END;
    ELSE
    BEGIN
        IF (@WhatIf <> 1) PRINT (CONCAT('/* Successfully recreated: ', COALESCE(@CountSchBvRecreated, 0), ' Schema-Bound Views (matches the number of Schema-Bound Views previously Dropped). */'));
    END;
    PRINT ('/*--------------------------------------- END OF RECREATING SCHEMA-BOUND VIEWS -----------------------------------*/');

    
    IF EXISTS (
                SELECT 1 FROM [#IndexesOnSchemaBoundViews] AS [isbv]
                JOIN [#SchemaBoundViews] AS [sbv] ON [sbv].[SbvObjectId] = [isbv].[ReferencedViewObjectId]
                WHERE [sbv].[Recreated] = 1
              )
    BEGIN
        PRINT ('/*--------------------------------------- RECREATING INDEXES ON SCHEMA-BOUND VIEWS: ------------------------------*/');

        SELECT @Id = MIN([isbv].[Id]), @IdMax = MAX([isbv].[Id])
        FROM [#IndexesOnSchemaBoundViews] AS [isbv]
        JOIN [#SchemaBoundViews] AS [sbv]
            ON [sbv].[SbvObjectId] = [isbv].[ReferencedViewObjectId]
        WHERE [sbv].[Recreated] = 1;
    
        WHILE (@Id <= @IdMax)
        BEGIN
            SELECT @SqlRecreateIdxOnSchBv = CONCAT(
                   'CREATE ' 
                 , [IsUnique]
                 , [IndexType]
                 , ' INDEX ' 
                 , [IndexName], ' '
                 , [OnView]
                 , [ColumnNames]
            ) 
            FROM [#IndexesOnSchemaBoundViews] WHERE [Id] = @Id;
        
            IF (@WhatIf = 1)
            BEGIN
                PRINT(@SqlRecreateIdxOnSchBv);
                PRINT('GO');
            END
            ELSE 
            BEGIN TRY
                IF (@ContinueOnError = 1 AND XACT_STATE() <> 0 AND @@TRANCOUNT > 0)
                BEGIN
                    COMMIT TRANSACTION;
                    BEGIN TRANSACTION;
                END;
                EXEC sys.sp_executesql @stmt = @SqlRecreateIdxOnSchBv;
                IF (@ContinueOnError = 1 AND XACT_STATE() <> 0 AND @@TRANCOUNT > 0)
                BEGIN
                    COMMIT TRANSACTION;
                    BEGIN TRANSACTION;
                END;
            END TRY
            BEGIN CATCH 
                  SET @ErrorMessage = CONCAT(ERROR_MESSAGE(), ' when executing: ', @SqlRecreateIdxOnSchBv);
                  IF (@ContinueOnError <> 1)
                      GOTO ERROR;
                  ELSE /* continue execution but log the error: */
                  BEGIN
                    RAISERROR(@ErrorMessage, @ErrorSeverity11, 1) WITH NOWAIT;                
                    IF (XACT_STATE() <> 0 AND @@TRANCOUNT > 0)
                    BEGIN
                        ROLLBACK TRANSACTION;
                        BEGIN TRANSACTION;
                    END;
                    
                    SET @ParamDefinition = '@_ErrorMessage NVARCHAR(4000), @_Id INT';
                    SET @SqlLogError = 'UPDATE [#IndexesOnSchemaBoundViews] SET [ErrorMessage] = CONCAT([ErrorMessage]+''; '', @_ErrorMessage) WHERE [Id] = @_Id';
                    EXEC sys.sp_executesql @stmt = @SqlLogError, @params = @ParamDefinition, @_ErrorMessage = @ErrorMessage, @_Id = @Id;
                    SET @ErrorMessage = NULL
                    
                    IF (XACT_STATE() <> 0 AND @@TRANCOUNT > 0)
                    BEGIN
                        COMMIT TRANSACTION;
                        BEGIN TRANSACTION;
                    END;
                    SET @ErrorMessage = NULL;
                  END;
            END CATCH; 
        
            SELECT @Id = MIN([isbv].[Id])
            FROM [#IndexesOnSchemaBoundViews] AS [isbv]
            JOIN [#SchemaBoundViews] AS [sbv]
                ON [sbv].[SbvObjectId] = [isbv].[ReferencedViewObjectId]
            WHERE [sbv].[Recreated] = 1 AND [isbv].[Id] > @Id;
        END
    END

    IF EXISTS (
                SELECT 1 FROM [#TriggersOnSchemaBoundViews] AS [tsbv]
                JOIN [#SchemaBoundViews] AS [sbv] ON [sbv].[SbvObjectId] = [tsbv].[ReferencedViewObjectId]
                WHERE [sbv].[Recreated] = 1
              )
    BEGIN
        PRINT ('/* -------------------------------------- RECREATING TRIGGERS ON SCHEMA-BOUND VIEWS: -----------------------------*/');

        SELECT @Id = MIN([tsbv].[Id]), @IdMax = MAX([tsbv].[Id]) 
        FROM [#TriggersOnSchemaBoundViews] AS [tsbv]
        JOIN [#SchemaBoundViews] AS [sbv] 
            ON [sbv].[SbvObjectId] = [tsbv].[ReferencedViewObjectId]
        WHERE [sbv].[Recreated] = 1

        WHILE (@Id <= @IdMax)
        BEGIN
            BEGIN                
                SELECT 
                       @TriggerName = [TriggerName]
                     , @IsEncrypted = [IsEncrypted]
                     , @TriggerId = [TriggerId]
                FROM [#TriggersOnSchemaBoundViews] 
                WHERE [Id] = @Id

                IF (@IsEncrypted = 1)
                BEGIN
                    SELECT @ErrorMessage = CONCAT('Definition of Trigger: ', @TriggerName, ' is encrypted, unable to recreate it');
                    IF (@WhatIf = 1)
                    BEGIN 
                        PRINT(CONCAT('/* !!! Warning: ', @ErrorMessage, ' */'))
                    END
                    ELSE 
                    BEGIN
                        IF (@ContinueOnError <> 1) GOTO ERROR
                        ELSE 
                        BEGIN
                                RAISERROR(@ErrorMessage, @ErrorSeverity11, 1) WITH NOWAIT;  
                                SET @ParamDefinition = '@_ErrorMessage NVARCHAR(4000), @_Id INT';
                                SET @SqlLogError = 'UPDATE [#TriggersOnSchemaBoundViews] SET [ErrorMessage] = CONCAT([ErrorMessage]+''; '', @_ErrorMessage) WHERE [Id] = @_Id';
                                EXEC sys.sp_executesql @stmt = @SqlLogError, @params = @ParamDefinition, @_ErrorMessage = @ErrorMessage, @_Id = @Id;
                                SET @ErrorMessage = NULL
                        END            
                    END 
                END
                
                IF (@IsEncrypted = 0)
                BEGIN               
                    SELECT @LineOfCodeId = MIN([LineId]), @LineOfCodeIdMax = MAX([LineId]) 
                    FROM [#TriggerDefinitions]
                    WHERE [TriggerId] = @TriggerId       

                    WHILE (@LineOfCodeId <= @LineOfCodeIdMax)
                    BEGIN                                              
                        SELECT @SqlRecreateTrgOnSchBv = CONCAT(@SqlRecreateTrgOnSchBv, TRIM([LineOfCode]), @crlf) 
                        FROM [#TriggerDefinitions] 
                        WHERE [TriggerId] = @TriggerId AND [LineId] = @LineOfCodeId

                        SELECT @LineOfCodeId = MIN([LineId]) FROM [#TriggerDefinitions]
                        WHERE [TriggerId] = @TriggerId AND [LineId] > @LineOfCodeId
                    END

                    IF (@WhatIf = 0)
                    BEGIN TRY            
                        IF (@ContinueOnError = 1 AND XACT_STATE() <> 0 AND @@TRANCOUNT > 0)
                        BEGIN
                            COMMIT TRANSACTION;
                            BEGIN TRANSACTION;
                        END;            
                        EXEC sys.sp_executesql @stmt = @SqlRecreateTrgOnSchBv;
                        IF (@ContinueOnError = 1 AND XACT_STATE() <> 0 AND @@TRANCOUNT > 0)
                        BEGIN
                            COMMIT TRANSACTION;
                            BEGIN TRANSACTION;
                        END;
                        PRINT (CONCAT('Successfully recreated trigger: ', @TriggerName));
                    END TRY
                    BEGIN CATCH
                          SET @ErrorMessage = CONCAT('Error: ', ERROR_MESSAGE(), ' Failed executing: ', @SqlRecreateTrgOnSchBv);
                          IF (@ContinueOnError <> 1)
                              GOTO ERROR;
                          ELSE /* continue execution but log the error: */
                          BEGIN
                            RAISERROR(@ErrorMessage, @ErrorSeverity11, 1) WITH NOWAIT;
                            
                            IF (XACT_STATE() <> 0 AND @@TRANCOUNT > 0)
                            BEGIN
                                ROLLBACK TRANSACTION;
                                BEGIN TRANSACTION;
                            END;
                            
                            SET @ParamDefinition = '@_ErrorMessage NVARCHAR(4000), @_Id INT';
                            SET @SqlLogError = 'UPDATE [#TriggersOnSchemaBoundViews] SET [ErrorMessage] = CONCAT([ErrorMessage]+''; '', @_ErrorMessage) WHERE [Id] = @_Id;';
                            EXEC sys.sp_executesql @stmt = @SqlLogError, @params = @ParamDefinition, @_ErrorMessage = @ErrorMessage, @_Id = @Id;
                            SET @ErrorMessage = NULL

                            IF (XACT_STATE() <> 0 AND @@TRANCOUNT > 0)
                            BEGIN
                                COMMIT TRANSACTION;
                                BEGIN TRANSACTION;
                            END;
                          END;
                    END CATCH
                    ELSE IF (@WhatIf = 1)
                    BEGIN
                        PRINT(CONCAT(@SqlRecreateTrgOnSchBv, 'GO'));
                    END
                END
            END;            

            SET @SqlRecreateTrgOnSchBv = NULL

            SELECT @Id = MIN([tsbv].[Id])
            FROM [#TriggersOnSchemaBoundViews] AS [tsbv]
            JOIN [#SchemaBoundViews] AS [sbv] 
                ON [sbv].[SbvObjectId] = [tsbv].[ReferencedViewObjectId]
            WHERE [sbv].[Recreated] = 1 AND [tsbv].[Id] > @Id;
        END;
    END
END;

IF (@CountFKFound > 0)
BEGIN
    PRINT ('/* -------------------------------------- RECREATING FK CONSTRAINTS: ---------------------------------------------*/');
    SELECT @Id = MIN([Id]), @IdMax = MAX([Id]) FROM [#ForeignKeyConstraintDefinitions];
    WHILE (@Id <= @IdMax)
    BEGIN
        SELECT @SqlRecreateConstraint = [RecreateConstraintCommand]
        FROM [#ForeignKeyConstraintDefinitions]
        WHERE [Id] = @Id;

        IF (@WhatIf = 1)
        BEGIN
            PRINT(@SqlRecreateConstraint);
            PRINT('GO');
        END;
        ELSE
        BEGIN TRY   
            IF (@ContinueOnError = 1 AND XACT_STATE() <> 0 AND @@TRANCOUNT > 0)
            BEGIN
                COMMIT TRANSACTION;
                BEGIN TRANSACTION;
            END;
            EXEC sys.sp_executesql @stmt = @SqlRecreateConstraint;
            IF (@ContinueOnError = 1 AND XACT_STATE() <> 0 AND @@TRANCOUNT > 0)
            BEGIN
                COMMIT TRANSACTION;
                BEGIN TRANSACTION;
            END;            
            IF (@ErrorMessage IS NULL) 
            BEGIN
                SELECT @CountFKRecreated = @CountFKRecreated + 1;
                
                -- update NumFkRecreated:
                UPDATE [st] SET [st].[NumFkRecreated] = COALESCE([st].[NumFkRecreated], 0) + 1 
                FROM [#SelectedTables] AS [st] 
                JOIN [#ForeignKeyConstraintDefinitions] AS [fkc] ON [fkc].[ObjectIdTrgt] = [st].[ObjectID] AND [st].[IsToBeTruncated] = 1
                WHERE [fkc].[Id] = @Id; 
            END;
        END TRY
        BEGIN CATCH 
              SET @ErrorMessage = CONCAT(ERROR_MESSAGE(), ' when executing: ', @SqlRecreateConstraint);
              IF (@ContinueOnError <> 1)
                  GOTO ERROR;
              ELSE /* continue execution but log the error: */
              BEGIN
                RAISERROR(@ErrorMessage, @ErrorSeverity11, 1) WITH NOWAIT;                
                IF (XACT_STATE() <> 0 AND @@TRANCOUNT > 0)
                BEGIN
                    ROLLBACK TRANSACTION;
                    BEGIN TRANSACTION;
                END;
                
                SET @ParamDefinition = '@_ErrorMessage NVARCHAR(4000), @_Id INT';
                SET @SqlLogError = 'UPDATE [#ForeignKeyConstraintDefinitions] SET [ErrorMessage] = CONCAT([ErrorMessage]+''; '', @_ErrorMessage) WHERE [Id] = @_Id';
                EXEC sys.sp_executesql @stmt = @SqlLogError, @params = @ParamDefinition, @_ErrorMessage = @ErrorMessage, @_Id = @Id;
                SET @ErrorMessage = NULL
                
                IF (XACT_STATE() <> 0 AND @@TRANCOUNT > 0)
                BEGIN
                    COMMIT TRANSACTION;
                    BEGIN TRANSACTION;
                END;
                SET @ErrorMessage = NULL;
              END;
        END CATCH; 

        SELECT @Id = COALESCE(MIN([Id]), @Id + 1) FROM [#ForeignKeyConstraintDefinitions] WHERE [Id] > @Id;
        IF  (@Id < @IdMax) AND (@Id * 100) / @IdMax <> @PercentProcessed AND @WhatIf <> 1
        BEGIN
            SET @PercentProcessed = (@Id * 100) / @IdMax;
            PRINT (CONCAT(@PercentProcessed, ' percent processed.'));
        END;
    END;
    IF (@WhatIf <> 1) AND COALESCE(@CountFKRecreated, 0) <> COALESCE(@CountFKDropped, 0)
    BEGIN        
        SET @ErrorMessage = CONCAT('Number of FK Constraints Re-Created: ', COALESCE(@CountFKRecreated, 0), ' does not match the Number of FK Constraints Dropped: ', COALESCE(@CountFKDropped, 0)); 
        
        IF (@ContinueOnError <> 1)
            GOTO ERROR;
        ELSE /* continue execution but log the error: */
        BEGIN
          RAISERROR(@ErrorMessage, @ErrorSeverity11, 1) WITH NOWAIT;
          
          SET @ParamDefinition = '@_ErrorMessage NVARCHAR(4000), @_Id INT';        
          SET @SqlLogError = 'UPDATE [#ForeignKeyConstraintDefinitions] SET [ErrorMessage] = CONCAT([ErrorMessage]+''; '', @_ErrorMessage);';        
          EXEC sys.sp_executesql @stmt = @SqlLogError, @params = @ParamDefinition, @_ErrorMessage = @ErrorMessage, @_Id = @Id;
          SET @ErrorMessage = NULL
          
          IF (XACT_STATE() <> 0 AND @@TRANCOUNT > 0)
          BEGIN
              COMMIT TRANSACTION;
              BEGIN TRANSACTION;
          END;          
          SET @ErrorMessage = NULL;
        END;
    END;
    ELSE
    BEGIN
        IF (@WhatIf <> 1) PRINT (CONCAT('/* Successfully recreated: ', COALESCE(@CountFKRecreated, 0), ' FK Constraints (matches the number of FK Constraints Dropped) */'));
    END;

    PRINT ('/*--------------------------------------- END OF RECREATING FK CONSTRAINTS ---------------------------------------*/');
END;

/* ==================================================================================================================== */
/* ----------------------------------------- END OF RECREATING AND RE-ENABLING  --------------------------------------- */
/* ==================================================================================================================== */

/* ==================================================================================================================== */
/* ----------------------------------------- COLLECTING [RowCountAfter] AFTER TRUNCATE:  ------------------------------ */
/* ==================================================================================================================== */

PRINT ('/*--------------------------------------- UPDATING [RowCountAfter] OF [#SelectedTables] AFTER TRUNCATE: ----------*/');
TRUNCATE TABLE [#TableRowCounts];
SELECT @Id = MIN([Id]), @IdMax = MAX([Id]) FROM [#SelectedTables] WHERE [IsToBeTruncated] = 1;
WHILE (@Id <= @IdMax)
BEGIN
    SELECT @SqlTableCounts = CASE WHEN @DbEngineVersion < 14
          /* For SQL Versions older than 14 (2017) use FOR XML PATH instead of STRING_AGG(): */
          THEN STUFF(
                      (
                          SELECT @UnionAll + ' SELECT ' + CAST([ObjectID] AS NVARCHAR(MAX)) + ' AS [ObjectID], ' + '''' + CAST(QUOTENAME([SchemaName]) AS NVARCHAR(MAX)) + '.'
                                 + CAST(QUOTENAME([TableName]) AS NVARCHAR(MAX)) + ''' AS [TableName], COUNT_BIG(1) AS [RowCount] FROM ' + CAST(QUOTENAME([SchemaName]) AS NVARCHAR(MAX)) + '.'
                                 + CAST(QUOTENAME([TableName]) AS NVARCHAR(MAX))
                          FROM [#SelectedTables]
                          WHERE [IsToBeTruncated] = 1 AND [Id] BETWEEN @Id AND (@Id + @BatchSize)
                          FOR XML PATH(''), TYPE
                      ).[value]('.', 'NVARCHAR(MAX)')
                    , 1
                    , LEN(@UnionAll)
                    , ''
                    )
          ELSE /* For SQL Versions 14+ (2017+) use STRING_AGG(): */
                    STRING_AGG(
                                  CONCAT(
                                            'SELECT '
                                          , CAST([ObjectID] AS NVARCHAR(MAX))
                                          , ' AS [ObjectID], '
                                          , ''''
                                          , CAST(QUOTENAME([SchemaName]) AS NVARCHAR(MAX))
                                          , '.'
                                          , CAST(QUOTENAME([TableName]) AS NVARCHAR(MAX))
                                          , ''' AS [TableName], COUNT_BIG(1) AS [RowCount] FROM '
                                          , CAST(QUOTENAME([SchemaName]) AS NVARCHAR(MAX))
                                          , '.'
                                          , CAST(QUOTENAME([TableName]) AS NVARCHAR(MAX))
                                        )
                                , @UnionAll
                              )
          END
    FROM  [#SelectedTables]
    WHERE [IsToBeTruncated] = 1 AND [Id] BETWEEN @Id AND (@Id + @BatchSize);

    IF (@SqlTableCounts IS NOT NULL)
    BEGIN 
        SET @SqlTableCounts = CONCAT(N'INSERT INTO [#TableRowCounts] ([ObjectID], [TableName], [RowCount]) ', @crlf, '(', @SqlTableCounts, ');');

        BEGIN TRY
            --SET @SqlTableCounts = REPLACE(@SqlTableCounts, 'TableRowCounts', 'FooBar') -- simulated error for debugging
            IF (@ContinueOnError = 1 AND XACT_STATE() <> 0 AND @@TRANCOUNT > 0)
            BEGIN
                COMMIT TRANSACTION;
                BEGIN TRANSACTION;
            END;
            EXEC sys.sp_executesql @stmt = @SqlTableCounts;
            IF (@ContinueOnError = 1 AND XACT_STATE() <> 0 AND @@TRANCOUNT > 0)
            BEGIN
                COMMIT TRANSACTION;
                BEGIN TRANSACTION;
            END;
        END TRY
        BEGIN CATCH
              SET @ErrorMessage = CONCAT(ERROR_MESSAGE(), ' when executing: ', @SqlTableCounts);
              IF (@ContinueOnError <> 1)
                  GOTO ERROR;
              ELSE /* continue execution but log the error: */
              BEGIN
                RAISERROR(@ErrorMessage, @ErrorSeverity11, 1) WITH NOWAIT;                
                IF (XACT_STATE() <> 0 AND @@TRANCOUNT > 0)
                BEGIN
                    ROLLBACK TRANSACTION;
                    BEGIN TRANSACTION;
                END;
                
                SET @ParamDefinition = '@_ErrorMessage NVARCHAR(4000), @_Id INT';
                SET @SqlLogError = 'UPDATE [#SelectedTables] SET [ErrorMessage] = CONCAT([ErrorMessage]+''; '', @_ErrorMessage) WHERE [Id] = @_Id';
                EXEC sys.sp_executesql @stmt = @SqlLogError, @params = @ParamDefinition, @_ErrorMessage = @ErrorMessage, @_Id = @Id;
                SET @ErrorMessage = NULL
                
                IF (XACT_STATE() <> 0 AND @@TRANCOUNT > 0)
                BEGIN
                    COMMIT TRANSACTION;
                    BEGIN TRANSACTION;
                END;
                SET @ErrorMessage = NULL;
              END;
        END CATCH;
    END;

    SELECT @Id = MIN([Id]) FROM [#SelectedTables] WHERE [IsToBeTruncated] = 1 AND [Id] > (@Id + @BatchSize);
    IF  (@Id < @IdMax)
    AND (@Id * 100) / @IdMax <> @PercentProcessed AND @WhatIf <> 1
    BEGIN
        SET @PercentProcessed = (@Id * 100) / @IdMax;
        PRINT (CONCAT(@PercentProcessed, ' percent processed.'));
    END;
END;

UPDATE [st]
SET [st].[RowCountAfter] = [trc].[RowCount]
FROM [#SelectedTables] AS [st]
JOIN [#TableRowCounts] AS [trc]
    ON [trc].[ObjectID] = [st].[ObjectID]
    AND [st].[IsToBeTruncated] = 1;
PRINT ('/*--------------------------------------- END OF UPDATING [RowCountAfter] OF [#SelectedTables] AFTER TRUNCATE ----*/');

/* ==================================================================================================================== */
/* ----------------------------------------- END OF COLLECTING [RowCountAfter] AFTER TRUNCATE   ----------------------- */
/* ==================================================================================================================== */

IF (XACT_STATE() <> 0 AND @@TRANCOUNT > 0 AND @@ERROR = 0) 
BEGIN
    IF (@WhatIf <> 1) PRINT('/* Committing the transaction */');
    COMMIT TRANSACTION;
END;
GOTO SUMMARY;

ERROR:
BEGIN
    IF (XACT_STATE() <> 0 AND @@TRANCOUNT > 0)
    BEGIN
        ROLLBACK TRANSACTION;
        PRINT (CONCAT('/* Rolling back transaction. ', @ErrorMessage, ' */'));
    END;
    RAISERROR(@ErrorMessage, @ErrorSeverity18, @ErrorState) WITH NOWAIT;
    GOTO FINISH;
END;

/* ==================================================================================================================== */
/* ----------------------------------------- PRINTING SUMMARY OUTPUT TABLE: ------------------------------------------- */
/* ==================================================================================================================== */

SUMMARY:
BEGIN
    ;WITH [TablesWithErrors]
     AS (
         SELECT '[#SelectedTables]' AS [TableName] FROM [#SelectedTables] WHERE [ErrorMessage] IS NOT NULL
         UNION
         SELECT '[#SchemaBoundViews]' AS [TableName] FROM [#SchemaBoundViews] WHERE [ErrorMessage] IS NOT NULL
         UNION
         SELECT '[#ForeignKeyConstraintDefinitions]' AS [TableName]
         FROM [#ForeignKeyConstraintDefinitions]
         WHERE [ErrorMessage] IS NOT NULL
         UNION
         SELECT '[#PublicationsArticles]' AS [TableName] FROM [#PublicationsArticles] WHERE [ErrorMessage] IS NOT NULL
         UNION
         SELECT '[#CDCInstances]' AS [TableName] FROM [#CDCInstances] WHERE [ErrorMessage] IS NOT NULL
         UNION 
         SELECT '[#TriggersOnSchemaBoundViews]' AS [TableName] FROM [#TriggersOnSchemaBoundViews] WHERE [ErrorMessage] IS NOT NULL         
        )
    SELECT @ErrorMessage
        = CASE
              WHEN @DbEngineVersion < 14 THEN STUFF((SELECT ', ' + [TablesWithErrors].[TableName] FROM [TablesWithErrors] FOR XML PATH(''), TYPE).[value]('.', 'NVARCHAR(MAX)'), 1, 2, '')
              ELSE STRING_AGG([TablesWithErrors].[TableName], ', ')
          END
    FROM [TablesWithErrors];

    IF (@ErrorMessage IS NOT NULL AND @WhatIf <> 1)
    BEGIN
        PRINT CONCAT('/* Errors encountered. Tables containing value in [ErrorMessage] or [IsTruncated] flag(s) = 0: ', @ErrorMessage, ' */');
        SELECT [Id]
             , CONCAT([SchemaName], '.', [TableName]) AS [ObjectName]
             , [ErrorMessage] FROM [#SelectedTables] WHERE [ErrorMessage] IS NOT NULL
        UNION
        SELECT [Id], [ReferencingObjectName] AS [ObjectName], [ErrorMessage] FROM [#SchemaBoundViews] WHERE [ErrorMessage] IS NOT NULL
        UNION
        SELECT [Id], [ForeignKeyName] AS [ObjectName], [ErrorMessage] FROM [#ForeignKeyConstraintDefinitions] WHERE [ErrorMessage] IS NOT NULL
        UNION
        SELECT [Id], [article] AS [ObjectName], [ErrorMessage] FROM [#PublicationsArticles] WHERE [ErrorMessage] IS NOT NULL
        UNION
        SELECT [Id], [capture_instance] AS [ObjectName], [ErrorMessage] FROM [#CDCInstances] WHERE [ErrorMessage] IS NOT NULL
        UNION 
        SELECT [Id], [TriggerName] AS [TableName], [ErrorMessage] FROM [#TriggersOnSchemaBoundViews] WHERE [ErrorMessage] IS NOT NULL         
    END
    
    IF (@ErrorMessage IS NULL AND @WhatIf <> 1)
    BEGIN
        PRINT ('/* Script completed successfully. */');                        
    END;

    SELECT [Id]
         , [SchemaID]
         , [ObjectID]
         , [SchemaName]
         , [TableName]
         , [IsToBeTruncated]
         , [IsOnExceptionList]
         , [IsTruncated]
         , CONCAT([RowCountBefore], IIF(@RowCountThreshold > 0 AND [RowCountBefore] < @RowCountThreshold, CONCAT(' below Threshld: ', @RowCountThreshold), '')) AS [RowCntBefore]
         , [RowCountAfter]         
         , [ErrorMessage]
         , [IsReferencedByFk]
         , [IsReferencedBySchBv]
         , [IsCDCEnabled]
         , [IsPublished]
         , [NumFkReferencing]
         , [NumFkDropped]
         , [NumFkRecreated]
         , [NumSchBvReferencing]
         , [NumSchBvDropped]
         , [NumSchBvRecreated]
         , [NumCDCInstReferencing]
         , [NumCDCInstDisabled]
         , [NumCDCInstReenabled]
         , [NumPublArtReferencing]
         , [NumPublArtDropped]
         , [NumPublArtRecreated]
    FROM [#SelectedTables]
    ORDER BY [RowCountBefore] DESC
           , [TableName];
END;
FINISH:
END;