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
/* Date:       User:           Version:  Change:                                                                        */
/* -------------------------------------------------------------------------------------------------------------------- */
/* 2024-11-21  CleanSql.com    1.0       Created                                                                        */
/* -------------------------------------------------------------------------------------------------------------------- */
/* ==================================================================================================================== */
/* Example use:
                                                                                                                     
   USE [AdventureWorks2019];                                                                                           
                                                                                                                       
   EXEC [dbo].[sp_ForceTruncate]                                                                                       
     @SchemaNames = N'Sales'                                                                                           
   , @TableNames  = N'SalesOrderHeader,SalesOrderHeaderSalesReason,Customer,CreditCard,PersonCreditCard,CurrencyRate'
   , @ContinueOnError = 1
*/
/*	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO    */
/*  THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE      */
/*	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, */
/*  TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE      */
/*	SOFTWARE.                                                                                                           */
/* ==================================================================================================================== */

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
  , @BatchSize                       INT           = 10
  , @ReenableCDC                     BIT           = 1
  , @RecreatePublishedArticles       BIT           = 1
