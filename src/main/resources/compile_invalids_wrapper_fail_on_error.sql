whenever SQLERROR EXIT SQL.SQLCODE
set PAGESIZE 0
set VERIFY OFF

SET SERVEROUTPUT ON SIZE UNLIMITED

@@ &1

set serveroutput on
set FEEDBACK OFF
declare
  v_invalid_cnt             number;
  v_invalid_cnt_prev        number;
  v_cnt                     number;
  v_invalid_cnt_not_changed boolean := false;
  v_sql                     varchar2(2000);
begin
  for i in 1..10 loop
    select count(*)
    into v_invalid_cnt
    from user_objects
    where status <> 'VALID'
          and object_type in
              ('TRIGGER', 'PROCEDURE', 'FUNCTION', 'PACKAGE', 'PACKAGE BODY', 'TYPE', 'TYPE BODY', 'INDEX', 'VIEW', 'SYNONYM');

    exit when v_invalid_cnt = 0;

    for rec in (
    select *
    from user_objects
    where status <> 'VALID'
          and object_type in
              ('TRIGGER', 'PROCEDURE', 'FUNCTION', 'PACKAGE', 'PACKAGE BODY', 'TYPE', 'TYPE BODY', 'INDEX', 'VIEW', 'SYNONYM')) loop

      case rec.object_type
        when 'TRIGGER'
        then
          v_sql := 'alter trigger ' || rec.object_name || ' compile';
        when 'PROCEDURE'
        then
          v_sql := 'alter procedure ' || rec.object_name || ' compile';
        when 'FUNCTION'
        then
          v_sql := 'alter function ' || rec.object_name || ' compile';
        when 'PACKAGE'
        then
          v_sql := 'alter package ' || rec.object_name || ' compile specification';
        when 'PACKAGE BODY'
        then
          v_sql := 'alter package ' || rec.object_name || ' compile body';
        when 'TYPE'
        then
          v_sql := 'alter type ' || rec.object_name || ' compile specification';
        when 'TYPE BODY'
        then
          v_sql := 'alter type ' || rec.object_name || ' compile body';
        when 'INDEX'
        then
          v_sql := 'alter index ' || rec.object_name || ' rebuild';
        when 'VIEW'
        then
          v_sql := 'alter view ' || rec.object_name || ' compile';
        when 'SYNONYM'
        then
          v_sql := 'select 1 from ' || rec.object_name;
      end case;

      begin
        execute immediate v_sql;
        exception when others then
        null;
      end;

    end loop;

    v_invalid_cnt_prev := v_invalid_cnt;

    select count(*)
    into v_invalid_cnt
    from user_objects
    where status <> 'VALID'
          and object_type in
              ('TRIGGER', 'PROCEDURE', 'FUNCTION', 'PACKAGE', 'PACKAGE BODY', 'TYPE', 'TYPE BODY', 'INDEX', 'VIEW', 'SYNONYM');

    -- exit when 2 time through yield same number of invalid objects
    exit when v_invalid_cnt = v_invalid_cnt_prev and v_invalid_cnt_not_changed;

    if v_invalid_cnt = v_invalid_cnt_prev and not v_invalid_cnt_not_changed
    then
      v_invalid_cnt_not_changed := true;
    else
      v_invalid_cnt_not_changed := false;
    end if;

  end loop;

end;
/

set heading off
select 'Invalid objects in [' || user || ']:'
from dual;
column object_type format a15
column object_name format a30 wra
column invalid_message format a15
set tab off
select
  substr(object_type, 1, 15) object_type,
  substr(object_name, 1, 30) object_name,
  ' is invalid.' invalid_message
from user_objects
where status <> 'VALID' and object_name not like 'BIN$%';
