create table db_script(
    db_script_id number not null,
    name varchar(400) not null,
    file_hash varchar2(400) not null,
    text clob null,
    ts date default sysdate not null,
    output clob null,
    type number not null,
    status number not null,
    constraint pk_db_script primary key (db_script_id)
);

create unique index u1_db_script on db_script (name, file_hash);
create sequence seq_db_script_id;

create or replace trigger tib_db_script before insert on db_script
    for each row
begin
    if (:new.db_script_id is null) then
        select seq_db_script_id.nextval into :new.db_script_id from dual;
    end if;
end;
/