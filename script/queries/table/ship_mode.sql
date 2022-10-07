drop table if exists ship_mode_tmp;
create table ship_mode_tmp (
  sm_ship_mode_sk integer, sm_ship_mode_id string, 
  sm_type string, sm_code string, sm_carrier string, 
  sm_contract string
) using csv options(header "false", delimiter "|", path "${path}/${name}");
drop table if exists ship_mode;
create table ship_mode using parquet as (select * from ship_mode_tmp);
drop table if exists ship_mode_tmp;