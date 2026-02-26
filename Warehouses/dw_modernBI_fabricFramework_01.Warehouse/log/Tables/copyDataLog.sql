CREATE TABLE [log].[copyDataLog] (

	[model] varchar(256) NOT NULL, 
	[destinationPath] varchar(600) NULL, 
	[sourceLocationName] varchar(200) NULL, 
	[objectName] varchar(200) NULL, 
	[status] varchar(200) NULL, 
	[startDate] datetime2(3) NULL, 
	[rowCount] int NULL, 
	[sourceReadCommand] varchar(4000) NULL, 
	[endDate] datetime2(3) NULL, 
	[duration] int NULL
);