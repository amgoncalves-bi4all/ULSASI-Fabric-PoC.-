CREATE PROCEDURE [log].[uspInsertCopyDataLog] 
@Model varchar(200),
@DestinationPath varchar(200),
@SourceLocationName varchar(200),
@ObjectName varchar(200),
@Status varchar(200),
@StartDate datetime,
@RowCount int,
@SourceReadCommand varchar(4000)

AS BEGIN

INSERT INTO [log].[copyDataLog]
           ([model]
      ,[destinationPath]
      ,[sourceLocationName]
      ,[objectName]
      ,[status]
      ,[startDate]
      ,[rowCount]
      ,[sourceReadCommand]
      ,[endDate]
      ,[duration])
     VALUES
           (@model
		   ,@destinationPath
           ,@sourceLocationName
           ,@objectName
		   ,@status
           ,@startDate
		   ,@rowCount
		   ,@sourceReadCommand
		   ,GETDATE()
		   ,DATEDIFF(second,@startDate,GETDATE()))

END