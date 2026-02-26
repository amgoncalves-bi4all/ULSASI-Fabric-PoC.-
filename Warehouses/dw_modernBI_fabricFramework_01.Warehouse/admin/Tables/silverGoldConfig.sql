CREATE TABLE [admin].[silverGoldConfig] (

	[model] varchar(200) NOT NULL, 
	[sourceSystemName] varchar(200) NULL, 
	[sourceLocationName] varchar(200) NULL, 
	[sourceDirectoryPattern] varchar(1000) NULL, 
	[objectName] varchar(200) NOT NULL, 
	[keyColumns] varchar(200) NULL, 
	[partitionColumns] varchar(500) NULL, 
	[extractType] varchar(50) NULL, 
	[loadType] varchar(50) NULL, 
	[destinationObjectPattern] varchar(200) NULL, 
	[destinationDatabase] varchar(200) NULL, 
	[notebookName] varchar(200) NULL, 
	[layer] varchar(200) NULL, 
	[flagActive] bit NOT NULL
);