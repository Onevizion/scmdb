set PAGESIZE 0
set VERIFY OFF

set serveroutput on
set FEEDBACK OFF

declare
    -- The expected exception with the -20000 code is
    -- "Unable to set values for index UTL_RECOMP_COMP_IDX1: does not exist or insufficient privileges".
    -- If it was thrown, it will wait 10 seconds and attempt to compile again, up to 3 times.
    e_expected_recomp_exception exception;
    pragma exception_init(e_expected_recomp_exception, c_expected_error_code);
    c_expected_error_code constant number := -20000;
    c_utl_recomp_index_name constant varchar2(20) := 'UTL_RECOMP_COMP_IDX1';
    c_seconds_to_wait constant number := 10;
    c_max_attempts constant number := 3;
    v_attempt number;
    v_cnt   number;
begin

    select count(*) into v_cnt from all_users where username = '&_USER._USER';
    if v_cnt = 1 then
        v_attempt := 0;
        while true loop
            begin
                dbms_utility.compile_schema('&_USER._USER', false);
                exit;

            exception
                when e_expected_recomp_exception then
                    v_attempt := v_attempt + 1;
                    if instr(sqlerrm, c_utl_recomp_index_name) > 0 and v_attempt <= c_max_attempts then
                        dbms_lock.sleep(c_seconds_to_wait);
                    else
                        raise;
                    end if;
            end;
        end loop;
    end if;

    select count(*) into v_cnt from all_users where username = '&_USER._RPT';
    if v_cnt = 1 then
        v_attempt := 0;
        while true loop
            begin
                dbms_utility.compile_schema('&_USER._RPT', false);
                exit;

            exception
                when e_expected_recomp_exception then
                    v_attempt := v_attempt + 1;
                    if instr(sqlerrm, c_utl_recomp_index_name) > 0 and v_attempt <= c_max_attempts then
                        dbms_lock.sleep(c_seconds_to_wait);
                    else
                        raise;
                    end if;
            end;
        end loop;
    end if;

    select count(*) into v_cnt from all_users where username = '&_USER._PKG';
    if v_cnt = 1 then
        v_attempt := 0;
        while true loop
            begin
                dbms_utility.compile_schema('&_USER._PKG', false);
                exit;

            exception
                when e_expected_recomp_exception then
                    v_attempt := v_attempt + 1;
                    if instr(sqlerrm, c_utl_recomp_index_name) > 0 and v_attempt <= c_max_attempts then
                        dbms_lock.sleep(c_seconds_to_wait);
                    else
                        raise;
                    end if;
            end;
        end loop;
    end if;
end;