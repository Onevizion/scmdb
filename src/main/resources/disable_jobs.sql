set serveroutput on
exec dbms_output.put_line('Suspend DB Jobs');
exec dbms_output.put_line(pkg_job_control.disable_jobs(30, 1));