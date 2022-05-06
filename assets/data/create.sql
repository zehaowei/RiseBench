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
    l_returnflag CHAR(1),
    l_linestatus CHAR(1),
    l_shipdate DATE,
    l_commitdate DATE,
    l_receiptdate DATE,
    l_shipinstruct CHAR(25),
    l_shipmode CHAR(10),
    l_comment VARCHAR(44))
    with (
    'upstream.source' = 'kafka',
    'kafka.topic' = 'lineitem',
    'kafka.bootstrap.servers' = 'localhost:9092'
    ) row format 'json'

statement
create source supplier (
    s_suppkey INTEGER NOT NULL,
    s_name CHAR(25) NOT NULL,
    s_address VARCHAR(40) NOT NULL,
    s_nationkey INTEGER NOT NULL,
    s_phone CHAR(15) NOT NULL,
    s_acctbal NUMERIC NOT NULL,
    s_comment VARCHAR(101) NOT NULL)
    with (
    'upstream.source' = 'kafka',
    'kafka.topic' = 'supplier',
    'kafka.bootstrap.servers' = 'localhost:9092'
    ) row format 'json'

statement
create source part (
    p_partkey INTEGER NOT NULL,
    p_name VARCHAR(55) NOT NULL,
    p_mfgr CHAR(25) NOT NULL,
    p_brand CHAR(10) NOT NULL,
    p_type VARCHAR(25) NOT NULL,
    p_size INTEGER NOT NULL,
    p_container CHAR(10) NOT NULL,
    p_retailprice NUMERIC NOT NULL,
    p_comment VARCHAR(23) NOT NULL)
    with (
    'upstream.source' = 'kafka',
    'kafka.topic' = 'part',
    'kafka.bootstrap.servers' = 'localhost:9092'
    ) row format 'json'

statement
create source partsupp (
    ps_partkey INTEGER NOT NULL,
    ps_suppkey INTEGER NOT NULL,
    ps_availqty INTEGER NOT NULL,
    ps_supplycost NUMERIC NOT NULL,
    ps_comment VARCHAR(199) NOT NULL)
    with (
    'upstream.source' = 'kafka',
    'kafka.topic' = 'partsupp',
    'kafka.bootstrap.servers' =
    'localhost:9092'
    ) row format 'json'

statement
create source customer (
    c_custkey INTEGER NOT NULL,
    c_name VARCHAR(25) NOT NULL,
    c_address VARCHAR(40) NOT NULL,
    c_nationkey INTEGER NOT NULL,
    c_phone CHAR(15) NOT NULL,
    c_acctbal NUMERIC NOT NULL,
    c_mktsegment CHAR(10) NOT NULL,
    c_comment VARCHAR(117) NOT NULL)
    with (
    'upstream.source' = 'kafka',
    'kafka.topic' = 'customer',
    'kafka.bootstrap.servers' = 'localhost:9092'
    ) row format 'json'

statement
create source orders (
    o_orderkey BIGINT NOT NULL,
    o_custkey INTEGER NOT NULL,
    o_orderstatus CHAR(1) NOT NULL,
    o_totalprice NUMERIC NOT NULL,
    o_orderdate DATE NOT NULL,
    o_orderpriority CHAR(15) NOT NULL,
    o_clerk CHAR(15) NOT NULL,
    o_shippriority INTEGER NOT NULL,
    o_comment VARCHAR(79) NOT NULL)
    with (
    'upstream.source' = 'kafka',
    'kafka.topic' = 'orders',
    'kafka.bootstrap.servers' = 'localhost:9092'
    ) row format 'json'

statement
create source nation (
    n_nationkey INTEGER NOT NULL,
    n_name CHAR(25) NOT NULL,
    n_regionkey INTEGER NOT NULL,
    n_comment VARCHAR(152) NOT NULL)
    with (
    'upstream.source' = 'kafka',
    'kafka.topic' = 'nation',
    'kafka.bootstrap.servers' = 'localhost:9092'
    ) row format 'json'

statement
create source region (
    r_regionkey INTEGER NOT NULL,
    r_name CHAR(25) NOT NULL,
    r_comment VARCHAR(152) NOT NULL)
    with(
    'upstream.source' = 'kafka',
    'kafka.topic' = 'region',
    'kafka.bootstrap.servers' = 'localhost:9092'
    ) row format 'json'