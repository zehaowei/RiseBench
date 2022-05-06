package configs

import "fmt"

const (
	LineItemSqlFilePath string = "./assets/data/ingest.sql"
	LineItemTblFilePath string = "./assets/data/lineitem.tbl"
)

type SQLStmtType string

const (
	SQLCreateSource SQLStmtType = "SQLCreateSource"
	SQLNormal       SQLStmtType = "SQLNormal"
)

var SqlCreatePath = "./assets/data/create_v2.sql"

type SqlConfig struct {
	SqlCreatePathPattern string
	SqlIngestPathPattern string
	SqlQueryPathPattern  string
	SqlCheckPathPattern  string
	SqlDropPathPattern   string
}

func NewTpchSqlConfig(queryId int) *SqlConfig {
	return &SqlConfig{
		SqlCreatePathPattern: SqlCreatePath,
		SqlIngestPathPattern: "",
		SqlQueryPathPattern:  fmt.Sprintf("./assets/data/q%d.sql", queryId),
		SqlCheckPathPattern:  "",
		SqlDropPathPattern:   "./assets/data/drop.sql",
	}
}
