package configs

import (
	"fmt"
)

type TpchTable string

const (
	TpchDistributionPath string    = "./assets/data/dists.dss"
	LineItem             TpchTable = "lineitem"
	Orders               TpchTable = "orders"
	Customer             TpchTable = "customer"
	Supplier             TpchTable = "supplier"
	Part                 TpchTable = "part"
	PartSupp             TpchTable = "partsupp"
	Nation               TpchTable = "nation"
	Region               TpchTable = "region"
)

// change here to run self-defined query
// you can remain these unchanged, and it will still work

// data of main table will be generated in real time,
// usually main table is the largest table in one query
var selfDefineMainTable = LineItem
var selfDefineTables = []TpchTable{
	Part, Supplier, LineItem, Orders, Customer, Nation, Region, PartSupp,
}

var tpchQueryTableMap = map[int][]TpchTable{
	-1: {Part, Supplier, LineItem, Orders, Customer, Nation, Region, PartSupp},
	1:  {LineItem},
	2:  {Part, Supplier, PartSupp, Nation, Region},
	3:  {Customer, Orders, LineItem},
	4:  {LineItem, Orders},
	5:  {Customer, Orders, LineItem, Supplier, Nation, Region},
	6:  {LineItem},
	7:  {Supplier, LineItem, Orders, Customer, Nation},
	8:  {Part, Supplier, LineItem, Orders, Customer, Nation, Region},
	9:  {Part, Supplier, LineItem, PartSupp, Orders, Nation},
	10: {Customer, Orders, LineItem, Nation},
	11: {PartSupp, Supplier, Nation},
	12: {Orders, LineItem},
	13: {Customer, Orders},
	14: {LineItem, Part},
	15: {Supplier, LineItem},
	16: {Part, PartSupp, Supplier},
	17: {LineItem, Part},
	18: {Customer, Orders, LineItem},
	19: {LineItem, Part},
	20: {Supplier, Nation, PartSupp, Part, LineItem},
	25: {LineItem},
}

var tpchQueryMainTableMap = map[int]TpchTable{
	-1: LineItem,
	1:  LineItem,
	2:  PartSupp,
	3:  LineItem,
	4:  LineItem,
	5:  LineItem,
	6:  LineItem,
	7:  LineItem,
	8:  LineItem,
	9:  LineItem,
	10: LineItem,
	11: PartSupp,
	12: LineItem,
	13: Orders,
	14: LineItem,
	15: LineItem,
	16: PartSupp,
	17: LineItem,
	18: LineItem,
	19: LineItem,
	20: LineItem,
	25: LineItem,
}

var TpchAllTables = []TpchTable{
	LineItem,
	Customer,
	Part,
	Supplier,
	PartSupp,
	Orders,
	Nation,
	Region,
}

type TpchBenchConfig struct {
	QueryName   string      // ex: q1
	Rate        int         // rate to generate data rows in main table
	ScaleFactor float64     // Base: 1.0 = 1,500,000 orders
	MainTable   TpchTable   // rate control related
	Tables      []TpchTable // tables involved in the query
	SqlConfig   *SqlConfig  // files containing ddl & query statements
}

func NewTpchConfig(queryId int, rate int, scale float64) *TpchBenchConfig {
	mainTable := selfDefineMainTable
	tables := selfDefineTables
	_, ok := tpchQueryMainTableMap[queryId]
	if ok {
		mainTable = tpchQueryMainTableMap[queryId]
		tables = tpchQueryTableMap[queryId]
	}
	return &TpchBenchConfig{
		fmt.Sprintf("q%d", queryId),
		rate,
		scale,
		mainTable,
		tables,
		NewTpchSqlConfig(queryId),
	}
}
