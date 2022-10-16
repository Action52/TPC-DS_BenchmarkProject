drop table if exists inventory_tmp;
create table inventory_tmp (
  inv_date_sk integer, inv_item_sk integer, 
  inv_warehouse_sk integer, inv_quantity_on_hand integer
) using csv options(header "false", delimiter "|", path "${path}/${name}");
drop table if exists inventory;
create table inventory using parquet as (select * from inventory_tmp);
drop table if exists inventory_tmp;