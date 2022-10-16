drop table if exists dbgen_version_tmp;
create table dbgen_version_tmp
(
    dv_version                string                  ,
    dv_create_date            date                          ,
    dv_create_time            timestamp                          ,
    dv_cmdline_args           string                 
) using csv options(header "false", delimiter "|", path "${path}/${name}");
drop table if exists dbgen_version;
create table dbgen_version using parquet as (select * from dbgen_version_tmp);
drop table if exists dbgen_version_tmp;