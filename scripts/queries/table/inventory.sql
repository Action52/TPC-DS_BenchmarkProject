drop table if exists inventory_tmp;
create table inventory_tmp (
  inv_date_sk integer, inv_item_sk integer, 
  inv_warehouse_sk integer, inv_quantity_on_hand integer
) using csv options(header "false", delimiter "|", path "${path}/${name}.dat");
drop table if exists inventory;
create table inventory using parquet location '${path}/${name}/parquet' as (select * from inventory_tmp);
drop table if exists inventory_tmp;