-- [Protecting Sensitive Information with Secure UDFs and Stored Procedures](https://docs.snowflake.com/en/developer-guide/secure-udf-procedure)
-- [Understanding caller’s rights and owner’s rights stored procedures](https://docs.snowflake.com/en/developer-guide/stored-procedure/stored-procedures-rights)
use  Snowpark_db.Sourced;
use role accountadmin;

create or replace table employees2 clone employees;
table employees2;

create or replace secure procedure increase_salary(name1 string, percent int)
    returns string
    execute as owner
as begin
    update employees2
    set salary = salary * :percent / 100
    where employee_name = :name1;
    return 'Done';
end;
call increase_salary('Bruce Ernst', 20);

grant usage on warehouse compute_wh to sysadmin;
grant usage on database snowpark_db to sysadmin;
grant usage on schema snowpark_db.sourced to sysadmin;
grant usage on procedure snowpark_db.sourced.increase_salary(string, int) to sysadmin;

use role sysadmin;
call increase_salary('Bruce Ernst', 20);
select get_ddl('procedure', 'snowpark_db.sourced.increase_salary(string, int)');

use role accountadmin;
alter procedure snowpark_db.sourced.increase_salary(string, int) execute as caller;

use role sysadmin;
call increase_salary('Bruce Ernst', 20);

use role accountadmin;
grant select,update on table snowpark_db.sourced.employees2 to sysadmin;

use role sysadmin;
call increase_salary('Bruce Ernst', 20);
