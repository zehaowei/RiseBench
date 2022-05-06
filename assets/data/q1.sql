statement
create materialized view tpch_q1 as
select
  l_returnflag,
  l_linestatus,
  sum(l_quantity) as sum_qty,
  sum(l_extendedprice) as sum_base_price,
  sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
  sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
  round(avg(l_quantity), 4) as avg_qty,
  round(avg(l_extendedprice), 4) as avg_price,
  round(avg(l_discount), 4) as avg_disc,
  count(*) as count_order
  from lineitem where l_shipdate <= date '1998-12-01' - interval '71' day
  group by
  l_returnflag,
  l_linestatus
order by
  l_returnflag,
  l_linestatus;
