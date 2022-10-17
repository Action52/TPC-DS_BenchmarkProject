drop table if exists household_demographics_tmp;
create table household_demographics_tmp (
  hd_demo_sk integer, hd_income_band_sk integer, 
  hd_buy_potential string, hd_dep_count integer, 
  hd_vehicle_count integer
) using csv options(header "false", delimiter "|", path "${path}/${name}.dat");
drop table if exists household_demographics;
create table household_demographics using parquet location '${path}/${name}/parquet' as (select * from household_demographics_tmp);
drop table if exists household_demographics_tmp;
