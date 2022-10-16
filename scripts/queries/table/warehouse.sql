drop table if exists warehouse_tmp;
create table warehouse_tmp (
  w_warehouse_sk integer, 
  w_warehouse_id string, 
  w_warehouse_name string, 
  w_warehouse_sq_ft integer, 
  w_street_number string, 
  w_street_name string, 
  w_street_type string, 
  w_suite_number string, 
  w_city string, 
  w_county string, 
  w_state string, 
  w_zip string, 
  w_country string, 
  w_gmt_offset decimal(5, 2)
) using csv options(header "false", delimiter "|", path "${path}/${name}");
drop table if exists warehouse;
create table warehouse using parquet as (select * from warehouse_tmp);
drop table warehouse_tmp;