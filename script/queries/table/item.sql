drop table if exists item_tmp;
create table item_tmp (
  i_item_sk integer, 
  i_item_id string, 
  i_rec_start_date date, 
  i_rec_end_date date, 
  i_item_desc string, 
  i_current_price decimal(7, 2), 
  i_wholesale_cost decimal(7, 2), 
  i_brand_id integer, 
  i_brand string, 
  i_class_id integer, 
  i_class string, 
  i_category_id integer, 
  i_category string, 
  i_manufact_id integer, 
  i_manufact string, 
  i_size string, 
  i_formulation string, 
  i_color string, 
  i_units string, 
  i_container string, 
  i_manager_id integer, 
  i_product_name string
) using csv options(header "false", delimiter "|", path "${path}/${name}");
drop table if exists item;
create table item using parquet as (select * from item_tmp);
drop table if exists item_tmp;
