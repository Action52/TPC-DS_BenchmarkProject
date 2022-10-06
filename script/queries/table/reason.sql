drop table if exists reason;
create table reason (
  r_reason_sk integer, r_reason_id string, 
  r_reason_desc string
) using csv options(header "false", delimiter "|", path "${path}/${name}");