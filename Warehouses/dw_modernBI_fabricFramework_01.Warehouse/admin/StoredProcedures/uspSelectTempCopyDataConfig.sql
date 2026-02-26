CREATE PROCEDURE [admin].[uspSelectTempCopyDataConfig]
(
-- Add the parameters for the stored procedure here
 @model varchar(64),
 @sourceObjectName varchar(256)
 )

AS

--Delete rows from a project preventing some unfinished process
DELETE FROM [temp].[copyDataConfig]
WHERE model = @model and sourceObjectName = @sourceObjectName

 -- Concatenate the values
DECLARE @startDate datetime
DECLARE @endDate datetime

BEGIN

	SET @startDate = GETUTCDATE()
	SET @endDate = @startDate

	INSERT INTO [temp].[copyDataConfig]
		   SELECT [configId]
				,[model]
				,[sourceSystemName]
				,[sourceSystemType]
				,[sourceLocationName]
				,[sourceObjectName]
				,[sourceSelectColumns]
				,[sourceKeyColumns]
				,[destinationSystemName]
				,[destinationSystemType]
				,[destinationObjectPattern] + '_' + CONVERT(varchar(50), @startDate, 112) + '_' + RIGHT('0' + CONVERT(varchar(2), DATEPART(HOUR, @startDate)), 2) + RIGHT('0' + CONVERT(varchar(2), DATEPART(MINUTE, @startDate)), 2) + '.' + [destinationObjectType] AS [destinationObjectPattern]
				,[destinationDirectoryPattern] + CONVERT(varchar(50), @startDate, 111) + '/' + RIGHT('0' + CONVERT(varchar(2), DATEPART(HOUR, @startDate)), 2) + '/' + RIGHT('0' + CONVERT(varchar(2), DATEPART(MINUTE, @startDate)), 2) AS [destinationDirectoryPattern]
				,[destinationObjectType]
				,[extractType]
				,[deltaStartDate]
				,CASE WHEN deltaEndDate IS NULL THEN @endDate
					WHEN deltaEndDate IS NOT NULL THEN deltaEndDate
				END AS [deltaEndDate]
				,[deltaDateColumn]
				,[deltaFilterCondition]
				,[flagBlock]
				,[blockSize]
				,[blockColumn]
				,CASE
				--sourceSystemType = xlsx
					WHEN LOWER([sourceSystemType]) ='xlsx' AND [flagBlock] = 0 THEN 'Not Aplicable'

				--sourceSystemType = SQL and ExtractType = FULL 
				WHEN LOWER([sourceSystemType]) = 'sql' and LOWER([extractType]) = 'full' and [sourceSelectColumns] is null and [flagBlock] = 0 
					THEN 'SELECT * FROM ' + [sourceObjectName]			
				WHEN LOWER([sourceSystemType]) = 'sql' and LOWER([extractType]) = 'full' and [sourceSelectColumns] is not null and [flagBlock] = 0 
					THEN 'SELECT ' + [sourceSelectColumns] + ' FROM '  + [sourceObjectName] 

				--sourceSystemType = SQL and ExtractType = DELTA
				WHEN LOWER([sourceSystemType]) = 'sql' and LOWER([extractType]) = 'delta' and [deltaFilterCondition] is not null and [deltaEndDate] is null and [deltaDateColumn] is not null and [sourceSelectColumns] is null and [flagBlock] = 0 
					THEN 'SELECT * FROM ' + [sourceObjectName] + ' WHERE ' + [deltaFilterCondition] + ' AND ' + [deltaDateColumn]  + ' >= ' + ''''+ convert(varchar(50), [deltaStartDate], 121)+'''' + ' AND ' + [deltaDateColumn]  + ' < ' + '''' + convert(varchar(50), @EndDate, 121) + ''''
				WHEN LOWER([sourceSystemType]) = 'sql' and LOWER([extractType]) = 'delta' and [deltaFilterCondition] is null and [deltaEndDate] is null and [deltaDateColumn] is not null and [sourceSelectColumns] is null and [flagBlock] = 0 
					THEN 'SELECT * FROM ' + [sourceObjectName] + ' WHERE ' + [deltaDateColumn]  + ' >= ' +''''+ convert(varchar(50), [deltaStartDate], 121)+''''+ ' AND ' + [deltaDateColumn]  + ' < ' + '''' + convert(varchar(50), @EndDate, 121) + ''''

				WHEN LOWER([sourceSystemType]) = 'sql' and LOWER([extractType]) = 'delta' and [deltaFilterCondition] is not null and [deltaEndDate] is null and [deltaDateColumn]  is not null and [sourceSelectColumns] is not null and [flagBlock] = 0 
					THEN 'SELECT ' + [sourceSelectColumns] + ' FROM ' + [sourceObjectName] + ' WHERE ' + [deltaFilterCondition] + ' AND ' + [deltaDateColumn]  + ' >= ' +''''+ convert(varchar(50), [deltaStartDate], 121)+''''+ ' AND ' + [deltaDateColumn]  + ' < ' + '''' + convert(varchar(50), @EndDate, 121) + ''''
				WHEN LOWER([sourceSystemType]) = 'sql' and LOWER([extractType]) = 'delta' and [deltaFilterCondition] is null and [deltaEndDate] is null and [deltaDateColumn]  is not null and [sourceSelectColumns] is not null and [flagBlock] = 0 
					THEN 'SELECT ' + [sourceSelectColumns] + ' FROM ' + [sourceObjectName] + ' WHERE ' + [deltaDateColumn]  + ' >= ' +''''+ convert(varchar(50), [deltaStartDate], 121)+''''+ ' AND ' + [deltaDateColumn]  + ' < ' + '''' + convert(varchar(50), @EndDate, 121) + ''''

				WHEN LOWER([sourceSystemType]) = 'sql' and LOWER([extractType]) = 'delta' and [deltaFilterCondition] is not null and [deltaEndDate] is not null and [deltaDateColumn]  is not null and [sourceSelectColumns] is null and [flagBlock] = 0 
					THEN 'SELECT * FROM ' + [sourceObjectName] + ' WHERE ' + [deltaFilterCondition] + ' AND ' + [deltaDateColumn]  + ' >= ' +''''+ convert(varchar(50), [deltaStartDate], 121)+''''+ ' AND ' + [deltaDateColumn]  + ' < ' + '''' + convert(varchar(50), [deltaEndDate], 121) + ''''
				WHEN LOWER([sourceSystemType]) = 'sql' and LOWER([extractType]) = 'delta' and [deltaFilterCondition] is null and [deltaEndDate] is not null and [deltaDateColumn]  is not null and [sourceSelectColumns] is null and [flagBlock] = 0 
					THEN 'SELECT * FROM ' + [sourceObjectName] + ' WHERE ' + [deltaDateColumn]  + ' >= ' +''''+ convert(varchar(50), [deltaStartDate], 121)+''''+ ' AND ' + [deltaDateColumn]  + ' < ' + '''' + convert(varchar(50), [deltaEndDate], 121) + ''''

				WHEN LOWER([sourceSystemType]) = 'sql' and LOWER([extractType]) = 'delta' and [deltaFilterCondition] is not null and [deltaEndDate] is not null and [deltaDateColumn]  is not null and [sourceSelectColumns] is not null and [flagBlock] = 0 
					THEN 'SELECT ' + [sourceSelectColumns] + ' FROM ' + [sourceObjectName] + ' WHERE ' + [deltaFilterCondition] + ' AND ' + [deltaDateColumn]  + ' >= ' +''''+ convert(varchar(50), [deltaStartDate], 121)+''''+ ' AND ' + [deltaDateColumn]  + ' < ' + '''' + convert(varchar(50), [deltaEndDate], 121) + ''''
				WHEN LOWER([sourceSystemType]) = 'sql' and LOWER([extractType]) = 'delta' and [deltaFilterCondition] is null and [deltaEndDate] is not null and [deltaDateColumn]  is not null and [sourceSelectColumns] is not null and [flagBlock] = 0 
					THEN 'SELECT ' + [sourceSelectColumns] + ' FROM ' + [sourceObjectName] + ' WHERE ' + [deltaDateColumn]  + ' >= ' +''''+ convert(varchar(50), [deltaStartDate], 121)+''''+ ' AND ' + [deltaDateColumn]  + ' < ' + '''' + convert(varchar(50), [deltaEndDate], 121) + ''''

				WHEN LOWER([sourceSystemType]) = 'sql' and LOWER([extractType]) = 'delta' and [deltaFilterCondition] is not null and [sourceSelectColumns] is null and [flagBlock] = 0 
					THEN 'SELECT * FROM ' + [sourceObjectName] + ' WHERE ' + [deltaFilterCondition]
				WHEN LOWER([sourceSystemType]) = 'sql' and LOWER([extractType]) = 'delta' and [deltaFilterCondition] is not null and [sourceSelectColumns] is not null and [flagBlock] = 0 
					THEN 'SELECT ' + [sourceSelectColumns] + ' FROM ' + [sourceObjectName] + ' WHERE ' + [deltaFilterCondition] 
					
					
				--sourceSystemType = SOQL and ExtractType = FULL 	
				WHEN LOWER([sourceSystemType]) = 'soql' and LOWER([extractType]) = 'full' and [sourceSelectColumns] is not null and [flagBlock] = 0 
					THEN 'SELECT ' + [sourceSelectColumns] + ' FROM '  + [sourceObjectName] 

				--sourceSystemType = SOQL and ExtractType = DELTA
				WHEN LOWER([sourceSystemType]) = 'soql' and LOWER([extractType]) = 'delta' and [deltaFilterCondition] is not null and [deltaEndDate] is null and [deltaDateColumn]  is not null and [sourceSelectColumns] is not null and [flagBlock] = 0 
					THEN 'SELECT ' + [sourceSelectColumns] + ' FROM ' + [sourceObjectName] + ' WHERE ' + [deltaFilterCondition] + ' AND ' + [deltaDateColumn]  + ' >= ' +''+ convert(varchar(19), [deltaStartDate], 127) + 'Z'+''+ ' AND ' + [deltaDateColumn]  + ' < ' + '' + convert(varchar(19), @EndDate, 127) + 'Z' + ''
				WHEN LOWER([sourceSystemType]) = 'soql' and LOWER([extractType]) = 'delta' and [deltaFilterCondition] is null and [deltaEndDate] is null and [deltaDateColumn]  is not null and [sourceSelectColumns] is not null and [flagBlock] = 0 
					THEN 'SELECT ' + [sourceSelectColumns] + ' FROM ' + [sourceObjectName] + ' WHERE ' + [deltaDateColumn]  + ' >= ' +''+ convert(varchar(19), [deltaStartDate], 127) + 'Z'+''+ ' AND ' + [deltaDateColumn]  + ' < ' + '' + convert(varchar(19), @EndDate, 127) + 'Z' + ''

				WHEN LOWER([sourceSystemType]) = 'soql' and LOWER([extractType]) = 'delta' and [deltaFilterCondition] is not null and [deltaEndDate] is not null and [deltaDateColumn]  is not null and [sourceSelectColumns] is not null and [flagBlock] = 0 
					THEN 'SELECT ' + [sourceSelectColumns] + ' FROM ' + [sourceObjectName] + ' WHERE ' + [deltaFilterCondition] + ' AND ' + [deltaDateColumn]  + ' >= ' +''+ convert(varchar(19), [deltaStartDate], 127) + 'Z'+''+ ' AND ' + [deltaDateColumn]  + ' < ' + '' + convert(varchar(19), [deltaEndDate], 127) + 'Z' + ''
				WHEN LOWER([sourceSystemType]) = 'soql' and LOWER([extractType]) = 'delta' and [deltaFilterCondition] is null and [deltaEndDate] is not null and [deltaDateColumn]  is not null and [sourceSelectColumns] is not null and [flagBlock] = 0 
					THEN 'SELECT ' + [sourceSelectColumns] + ' FROM ' + [sourceObjectName] + ' WHERE ' + [deltaDateColumn]  + ' >= ' +''+ convert(varchar(19), [deltaStartDate], 127) + 'Z'+''+ ' AND ' + [deltaDateColumn]  + ' < ' + '' + convert(varchar(19), [deltaEndDate], 127) + 'Z' + ''


				WHEN LOWER([sourceSystemType]) = 'soql' and LOWER([extractType]) = 'delta' and [deltaFilterCondition] is not null and [sourceSelectColumns] is not null and [flagBlock] = 0 
					THEN 'SELECT ' + [sourceSelectColumns] + ' FROM ' + [sourceObjectName] + ' WHERE ' + [deltaFilterCondition] 

				END AS [sourceReadCommand]
				,[flagActive]
				,@startDate AS [startDate]
		   FROM [admin].[copyDataConfig]

		WHERE [model] = @model AND [sourceObjectName] = @sourceObjectName AND [flagActive] = 1
		

    SELECT [configId]
      ,[model]
      ,[sourceSystemName]	 
      ,[sourceSystemType]
      ,[sourceLocationName]
      ,[sourceObjectName]
	  ,[sourceSelectColumns]
      ,[destinationSystemName]
      ,[destinationSystemType]
	  ,[destinationObjectPattern]
	  ,[destinationDirectoryPattern]
      ,[destinationObjectType]
	  ,[extractType]
	  ,[deltaStartDate]
	  ,[deltaEndDate] 
	  ,[deltaDateColumn]
	  ,[deltaFilterCondition]
	  ,[flagBlock]
	  ,[blockSize]
	  ,[blockColumn]
	  ,[flagActive]
	  ,[sourceReadCommand]
	  ,[startDate]
	FROM [temp].[copyDataConfig]
	WHERE [model] = @model AND [sourceObjectName] = @sourceObjectName AND [flagActive] = 1
   
END