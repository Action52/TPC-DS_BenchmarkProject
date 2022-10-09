drop table if exists customer_address_tmp;
create table customer_address_tmp (
  ca_address_sk integer, 
  ca_address_id string, 
  ca_street_number string, 
  ca_street_name string, 
  ca_street_type string, 
  ca_suite_number string, 
  ca_city string, 
  ca_county string, 
  ca_state string, 
  ca_zip string, 
  ca_country string, 
  ca_gmt_offset decimal(5, 2), 
  ca_location_type string
) using csv options(header "false", delimiter "|", path "${path}/${name}");
drop table if exists customer_address;
create table customer_address using parquet as (select * from customer_address_tmp);
drop table if exists customer_address_tmp;
