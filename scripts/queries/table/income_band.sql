drop table if exists income_band_tmp;
create table income_band_tmp (
  ib_income_band_sk integer, ib_lower_bound integer, 
  ib_upper_bound integer
) using csv options(header "false", delimiter "|", path "${path}/${name}.dat");
drop table if exists income_band;
create table income_band using parquet location '${path}/${name}/parquet' as (select * from income_band_tmp);
drop table if exists income_band_tmp;
