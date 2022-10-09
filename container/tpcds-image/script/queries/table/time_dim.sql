drop table if exists time_dim_tmp;
create table time_dim_tmp (
  t_time_sk integer, t_time_id string, 
  t_time integer, t_hour integer, t_minute integer, 
  t_second integer, t_am_pm string, 
  t_shift string, t_sub_shift string, 
  t_meal_time string
) using csv options(header "false", delimiter "|", path "${path}/${name}");
drop table if exists time_dim;
create table time_dim using parquet as (select * from time_dim_tmp);
drop table if exists time_dim_tmp;