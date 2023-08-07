

CREATE procedure [ETLProcess].[AzureFunctionLogInsert]  
(  
       @Guid UNIQUEIDENTIFIER =null,  
       @Message varchar(8000)= null
)  
as
begin
    INSERT INTO [ETLProcess].[AzureFunctionLog]
        (GUID, MessageText )
    Values
        (@Guid, @Message);
end