drop table if exists date_dim_tmp;
create table date_dim_tmp (
  d_date_sk integer, d_date_id string, 
  d_date date, d_month_seq integer, 
  d_week_seq integer, d_quarter_seq integer, 
  d_year integer, d_dow integer, d_moy integer, 
  d_dom integer, d_qoy integer, d_fy_year integer, 
  d_fy_quarter_seq integer, d_fy_week_seq integer, 
  d_day_name string, d_quarter_name string, 
  d_holiday string, d_weekend string, 
  d_following_holiday string, d_first_dom integer, 
  d_last_dom integer, d_same_day_ly integer, 
  d_same_day_lq integer, d_current_day string, 
  d_current_week string, d_current_month string, 
  d_current_quarter string, d_current_year string
) using csv options(header "false", delimiter "|", path "${path}/${name}");
drop table if exists date_dim;
create table date_dim using parquet as (select * from date_dim_tmp);
drop table if exists date_dim_tmp;
