CREATE PROCEDURE [admin].[uspSelectCopyDataConfig]
(
-- Add the parameters for the stored procedure here
 @model varchar(64)
)
AS

BEGIN

	SELECT * FROM admin.copyDataConfig WHERE model=@model and flagActive = 1
   
END