drop table if exists catalog_page_tmp;
create table catalog_page_tmp (
  cp_catalog_page_sk integer, cp_catalog_page_id string, 
  cp_start_date_sk integer, cp_end_date_sk integer, 
  cp_department string, cp_catalog_number integer, 
  cp_catalog_page_number integer, 
  cp_description string, cp_type string
) using csv options(header "false", delimiter "|", path "${path}/${name}.dat");
drop table if exists catalog_page;
create table catalog_page using parquet location '${path}/${name}/parquet' as (select * from catalog_page_tmp);
drop table if exists catalog_page_tmp;
