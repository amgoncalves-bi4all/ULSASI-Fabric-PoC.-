CREATE PROCEDURE [admin].[uspUpdateCopyDataConfig]
(
 @Model varchar(25),
 @SourceObjectName varchar(200),
 @StartDate datetime
)

AS
 BEGIN
    update [admin].[copyDataConfig] set deltaStartDate = @StartDate, deltaEndDate = null where model = @Model and sourceObjectName = @SourceObjectName 


END