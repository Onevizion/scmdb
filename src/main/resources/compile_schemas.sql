set PAGESIZE 0
set VERIFY OFF

set serveroutput on
set FEEDBACK OFF

declare
    -- The expected exception with the -20000 code is
    -- "Unable to set values for index UTL_RECOMP_COMP_IDX1: does not exist or insufficient privileges".
    -- If it was thrown, it will wait 10 seconds and attempt to compile again.
    c_expected_error_code number := -20000;
    v_cnt   number;
begin

select count(*) into v_cnt from all_users where username = '&_USER._USER';
if v_cnt = 1 then
    begin
        dbms_utility.compile_schema('&_USER._USER', false);
    exception
        when others then
            if sqlcode = c_expected_error_code then
                dbms_lock.sleep(10);
                dbms_utility.compile_schema('&_USER._USER', false);
            else
                raise;
            end if;
    end;
end if;

select count(*) into v_cnt from all_users where username = '&_USER._RPT';
if v_cnt = 1 then
    begin
        dbms_utility.compile_schema('&_USER._RPT', false);
    exception
        when others then
            if sqlcode = c_expected_error_code then
                dbms_lock.sleep(10);
                dbms_utility.compile_schema('&_USER._RPT', false);
            else
                raise;
            end if;
    end;
end if;

select count(*) into v_cnt from all_users where username = '&_USER._PKG';
if v_cnt = 1 then
    begin
        dbms_utility.compile_schema('&_USER._PKG', false);
    exception
        when others then
            if sqlcode = c_expected_error_code then
                dbms_lock.sleep(10);
                dbms_utility.compile_schema('&_USER._PKG', false);
            else
                raise;
            end if;
    end;
end if;

end;