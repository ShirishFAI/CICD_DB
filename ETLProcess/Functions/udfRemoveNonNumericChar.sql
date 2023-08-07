create function ETLProcess.udfRemoveNonNumericChar(@str varchar(500))  
returns varchar(500)  
begin  
declare @startingIndex int  
set @startingIndex=0  
while 1=1  
begin  
    set @startingIndex= patindex('%[^0-9]%',@str)  
    if @startingIndex <> 0  
    begin  
        set @str = replace(@str,substring(@str,@startingIndex,1),'')  
    end  
    else    break;   
end  
return @str  
end