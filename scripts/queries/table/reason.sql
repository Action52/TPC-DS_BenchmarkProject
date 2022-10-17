drop table if exists reason_tmp;
create table reason_tmp (
  r_reason_sk integer,
  r_reason_id string,
  r_reason_desc string
)
using csv options(header "false", delimiter "|", path "${path}/${name}.dat");
drop table if exists reason;
create table reason using parquet location '${path}/${name}/parquet' as select * from reason_tmp;
drop table if exists reason_tmp;