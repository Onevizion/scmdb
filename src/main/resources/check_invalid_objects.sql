declare
  v_invalid_objects_found BOOLEAN := FALSE;
  v_output VARCHAR2(4000);
begin
  dbms_output.put_line(chr(10) || '==================================================');
  dbms_output.put_line('WARNING: Invalid objects found in database:');
  dbms_output.put_line('==================================================');

for obj in (select object_type, object_name
              from all_objects
             where status = 'INVALID'
               and owner = user
             order by object_type, object_name) loop
    dbms_output.put_line('  - ' || user || '.' || obj.object_name || ' (' || obj.object_type || ')');
    v_invalid_objects_found := TRUE;
end loop;

for obj in (select owner, object_type, object_name
              from all_objects
             where status = 'INVALID'
               and owner like '%\_USER' escape '\'
             order by owner, object_type, object_name) loop
    dbms_output.put_line('  - ' || obj.owner || '.' || obj.object_name || ' (' || obj.object_type || ')');
    v_invalid_objects_found := TRUE;
end loop;

for obj in (select owner, object_type, object_name
              from all_objects
             where status = 'INVALID'
               and owner like '%\_RPT' escape '\'
             order by owner, object_type, object_name) loop
    dbms_output.put_line('  - ' || obj.owner || '.' || obj.object_name || ' (' || obj.object_type || ')');
    v_invalid_objects_found := TRUE;
end loop;

for obj in (select owner, object_type, object_name
              from all_objects
             where status = 'INVALID'
               and owner like '%\_PKG' escape '\'
             order by owner, object_type, object_name) loop
    dbms_output.put_line('  - ' || obj.owner || '.' || obj.object_name || ' (' || obj.object_type || ')');
    v_invalid_objects_found := TRUE;
end loop;

    if v_invalid_objects_found then
    dbms_output.put_line('==================================================');
    dbms_output.put_line(chr(10));
else
    dbms_output.put_line('No invalid objects found.' || chr(10));
end if;
end;
/