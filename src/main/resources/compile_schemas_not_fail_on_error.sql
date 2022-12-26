set PAGESIZE 0
set VERIFY OFF

set serveroutput on
set FEEDBACK OFF

declare
    v_cnt   number;
begin

    select count(*) into v_cnt from all_users where username = '&_USER._USER';
    if v_cnt = 1 then
        dbms_utility.compile_schema('&_USER._USER', false);
    end if;

    select count(*) into v_cnt from all_users where username = '&_USER._RPT';
    if v_cnt = 1 then
        dbms_utility.compile_schema('&_USER._RPT', false);
    end if;

    select count(*) into v_cnt from all_users where username = '&_USER._PKG';
    if v_cnt = 1 then
        dbms_utility.compile_schema('&_USER._PKG', false);
    end if;

    select count(*) into v_cnt from all_users where username = '&_USER._PERFSTAT';
    if v_cnt = 1 then
        dbms_utility.compile_schema('&_USER._PERFSTAT', false);
    end if;

end;