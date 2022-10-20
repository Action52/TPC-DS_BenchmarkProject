drop table if exists call_center_tmp;
create table call_center_tmp (
  cc_call_center_sk integer, 
  cc_call_center_id string, 
  cc_rec_start_date date,
  cc_rec_end_date date, 
  cc_closed_date_sk integer, 
  cc_open_date_sk integer, 
  cc_name string, 
  cc_class string, 
  cc_employees integer, 
  cc_sq_ft integer, 
  cc_hours string, 
  cc_manager string, 
  cc_mkt_id integer, 
  cc_mkt_class string, 
  cc_mkt_desc string, 
  cc_market_manager string, 
  cc_division integer, 
  cc_division_name string, 
  cc_company integer, 
  cc_company_name string, 
  cc_street_number string, 
  cc_street_name string, 
  cc_street_type string, 
  cc_suite_number string, 
  cc_city string, 
  cc_county string, 
  cc_state string, 
  cc_zip string, 
  cc_country string, 
  cc_gmt_offset decimal(5, 2), 
  cc_tax_percentage decimal(5, 2)
) using csv options(header "false", delimiter "|", path "${path}/${name}.dat");
drop table if  exists call_center;
create table call_center using parquet location '${path}/${name}/parquet' as (select * from call_center_tmp);
drop table if exists call_center_tmp;