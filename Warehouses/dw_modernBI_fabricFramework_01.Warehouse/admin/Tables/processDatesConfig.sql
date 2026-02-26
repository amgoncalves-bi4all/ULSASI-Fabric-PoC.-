CREATE TABLE [admin].[processDatesConfig] (

	[model] varchar(200) NULL, 
	[tableName] varchar(200) NULL, 
	[scope] varchar(200) NULL, 
	[fullProcess] bit NULL, 
	[dateType] varchar(100) NULL, 
	[filterColumn] varchar(200) NULL, 
	[dateColumnFormat] varchar(200) NULL, 
	[dateUnit] int NULL, 
	[date] datetime2(0) NULL
);