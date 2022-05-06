statement
create materialized view tpch_q17 as
select
	ROUND(sum(l_extendedprice) / 7.0, 16) as avg_yearly
from
	lineitem,
	part
where
	p_partkey = l_partkey
	and p_brand = 'Brand#13'
	and p_container = 'JUMBO PKG'
	and l_quantity < (
		select
			0.2 * avg(l_quantity)
		from
			lineitem
		where
			l_partkey = p_partkey
	);