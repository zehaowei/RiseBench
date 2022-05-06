statement
CREATE source lineitem (
    l_orderkey BIGINT,
    l_partkey INTEGER,
    l_suppkey INTEGER,
    l_linenumber INTEGER,
    l_quantity NUMERIC,
    l_extendedprice NUMERIC,
    l_discount NUMERIC,
    l_tax NUMERIC,
    l_returnflag VARCHAR(1),
    l_linestatus VARCHAR(1),
    l_shipdate DATE,
    l_commitdate DATE,
    l_receiptdate DATE,
    l_shipinstruct VARCHAR(25),
    l_shipmode VARCHAR(10),
    l_comment VARCHAR(44))
    with (
    'connector'='kafka',
    'kafka.topic'='lineitem',
    'kafka.brokers'='localhost:9092',
    'kafka.consumer.group'='lineitem_consumer'
    ) row format JSON

statement
create source supplier (
    s_suppkey INTEGER NOT NULL,
    s_name VARCHAR(25) NOT NULL,
    s_address VARCHAR(40) NOT NULL,
    s_nationkey INTEGER NOT NULL,
    s_phone VARCHAR(15) NOT NULL,
    s_acctbal NUMERIC NOT NULL,
    s_comment VARCHAR(101) NOT NULL)
    with (
    'connector'='kafka',
    'kafka.topic'='supplier',
    'kafka.brokers'='localhost:9092',
    'kafka.consumer.group'='supplier_consumer'
    ) row format JSON

statement
create source part (
    p_partkey INTEGER NOT NULL,
    p_name VARCHAR(55) NOT NULL,
    p_mfgr VARCHAR(25) NOT NULL,
    p_brand VARCHAR(10) NOT NULL,
    p_type VARCHAR(25) NOT NULL,
    p_size INTEGER NOT NULL,
    p_container VARCHAR(10) NOT NULL,
    p_retailprice NUMERIC NOT NULL,
    p_comment VARCHAR(23) NOT NULL)
    with (
    'connector'='kafka',
    'kafka.topic'='part',
    'kafka.brokers'='localhost:9092',
    'kafka.consumer.group'='part_consumer'
    ) row format JSON

statement
create source partsupp (
    ps_partkey INTEGER NOT NULL,
    ps_suppkey INTEGER NOT NULL,
    ps_availqty INTEGER NOT NULL,
    ps_supplycost NUMERIC NOT NULL,
    ps_comment VARCHAR(199) NOT NULL)
    with (
    'connector'='kafka',
    'kafka.topic'='partsupp',
    'kafka.brokers'='localhost:9092',
    'kafka.consumer.group'='partsupp_consumer'
    ) row format JSON

statement
create source customer (
    c_custkey INTEGER NOT NULL,
    c_name VARCHAR(25) NOT NULL,
    c_address VARCHAR(40) NOT NULL,
    c_nationkey INTEGER NOT NULL,
    c_phone VARCHAR(15) NOT NULL,
    c_acctbal NUMERIC NOT NULL,
    c_mktsegment VARCHAR(10) NOT NULL,
    c_comment VARCHAR(117) NOT NULL)
    with (
    'connector'='kafka',
    'kafka.topic'='customer',
    'kafka.brokers'='localhost:9092',
    'kafka.consumer.group'='customer_consumer'
    ) row format JSON

statement
create source orders (
    o_orderkey BIGINT NOT NULL,
    o_custkey INTEGER NOT NULL,
    o_orderstatus VARCHAR(1) NOT NULL,
    o_totalprice NUMERIC NOT NULL,
    o_orderdate DATE NOT NULL,
    o_orderpriority VARCHAR(15) NOT NULL,
    o_clerk VARCHAR(15) NOT NULL,
    o_shippriority INTEGER NOT NULL,
    o_comment VARCHAR(79) NOT NULL)
    with (
    'connector'='kafka',
    'kafka.topic'='orders',
    'kafka.brokers'='localhost:9092',
    'kafka.consumer.group'='orders_consumer'
    ) row format JSON

statement
create source nation (
    n_nationkey INTEGER NOT NULL,
    n_name VARCHAR(25) NOT NULL,
    n_regionkey INTEGER NOT NULL,
    n_comment VARCHAR(152) NOT NULL)
    with (
    'connector'='kafka',
    'kafka.topic'='nation',
    'kafka.brokers'='localhost:9092',
    'kafka.consumer.group'='nation_consumer'
    ) row format JSON

statement
create source region (
    r_regionkey INTEGER NOT NULL,
    r_name VARCHAR(25) NOT NULL,
    r_comment VARCHAR(152) NOT NULL)
    with(
    'connector'='kafka',
    'kafka.topic'='region',
    'kafka.brokers'='localhost:9092',
    'kafka.consumer.group'='region_consumer'
    ) row format JSON