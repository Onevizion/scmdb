set serveroutput on size unlimited
set feedback off

declare
    v_invalid_objects_found boolean := false;
    v_object_count number := 0;
    v_owner_schema varchar2(128) := user;
begin
    for rec_obj in (select owner, object_type, object_name
                      from dba_objects
                     where status = 'INVALID'
                       and object_name not like 'BIN$%'
                       and object_type <> 'SYNONYM' -- Will be resolved when accessing the object
                       and owner in (v_owner_schema,
                                     v_owner_schema || '_USER',
                                     v_owner_schema || '_PKG',
                                     v_owner_schema || '_RPT')
                     order by owner, object_type, object_name)
    loop
        if not v_invalid_objects_found then
            dbms_output.put_line('');
            dbms_output.put_line('==================================================');
            dbms_output.put_line('WARNING: Invalid objects found in database:');
            dbms_output.put_line('==================================================');
            v_invalid_objects_found := true;
        end if;

        dbms_output.put_line('  - ' || rec_obj.owner || '.' || rec_obj.object_name || ' (' || rec_obj.object_type || ')');
        v_object_count := v_object_count + 1;
    end loop;

    if v_invalid_objects_found then
        dbms_output.put_line('==================================================');
        dbms_output.put_line('Total invalid objects: ' || v_object_count);
        dbms_output.put_line('==================================================');
        dbms_output.put_line('');
    else
        dbms_output.put_line('No invalid objects found in OWNER and _rpt, _pkg and _user schemas');
    end if;
end;
/