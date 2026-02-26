CREATE PROCEDURE [admin].[uspUpdateProcessDatesConfig]
(
 @Model varchar(25),
 @TableName varchar(200),
 @ProcessDate datetime
)

AS
 BEGIN
    update [admin].[processDatesConfig] set [date] = @ProcessDate where model = @Model and tableName = @TableName


END