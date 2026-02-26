CREATE PROCEDURE [admin].[uspSelectSilverGoldConfig]
(
-- Add the parameters for the stored procedure here
 @model varchar(64),
 @layer varchar(64)
)
AS

BEGIN

	SELECT * FROM admin.silverGoldConfig WHERE model=@model and layer=@layer and flagActive = 1
   
END