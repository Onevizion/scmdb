declare
    v_invalid_object_title VARCHAR2(200);
    v_invalid_object_names VARCHAR2(200);
begin
    select chr(10) || 'Invalid objects in [' || user || ']:' || chr(10)
      into v_invalid_object_title
      from dual;

    select listagg(concat(concat(object_type || ' ', object_name), ' is invalid.'), chr(10))
      into v_invalid_object_names
      from user_objects
     where status <> 'VALID' and object_name not like 'BIN$%';

    if v_invalid_object_names != ' ' then
        raise_application_error( -20001, v_invalid_object_title || v_invalid_object_names);
    end if;
end;
/