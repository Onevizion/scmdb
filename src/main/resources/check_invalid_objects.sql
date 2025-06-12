set serveroutput on size unlimited
set feedback off

declare
    v_invalid_objects_found BOOLEAN := FALSE;
    v_object_count NUMBER := 0;
begin
    for obj in (
        select owner, object_type, object_name
          from all_objects
         where status = 'INVALID'
           and object_name not like 'BIN$%'
           and (
             owner = user
             or owner like '%\_USER' escape '\'
             or owner like '%\_RPT' escape '\'
             or owner like '%\_PKG' escape '\'
           )
         order by owner, object_type, object_name
      ) loop
        if not v_invalid_objects_found then
           dbms_output.put_line('');
           dbms_output.put_line('==================================================');
           dbms_output.put_line('WARNING: Invalid objects found in database:');
           dbms_output.put_line('==================================================');
           v_invalid_objects_found := TRUE;
        end if;

    dbms_output.put_line('  - ' || obj.owner || '.' || obj.object_name || ' (' || obj.object_type || ')');
    v_object_count := v_object_count + 1;
    end loop;

    if v_invalid_objects_found then
       dbms_output.put_line('==================================================');
       dbms_output.put_line('Total invalid objects: ' || v_object_count);
       dbms_output.put_line('==================================================');
       dbms_output.put_line('');
    end if;
end;
/