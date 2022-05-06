package exec

import (
	"bufio"
	"database/sql"
	"fmt"
	"github.com/singularity-data/tpch-bench/pkg/configs"
	"github.com/singularity-data/tpch-bench/pkg/util"
	"regexp"
	"strings"
	"time"
)

type SQLType int32

const (
	SqlStatement SQLType = 0
	SqlQuery     SQLType = 1
)

type SQLStatement struct {
	sqlType        SQLType
	meta           string // file name & line number of sql
	sql            string // content of sql
	rowsNum        int64  // count of rows affected
	isExecuted     bool
	isSuccess      bool
	result         string // query result
	expectedResult string // ground truth
}

type SQLExecutor struct {
	db *sql.DB
}

func NewSQLExecutor(db *sql.DB) *SQLExecutor {
	return &SQLExecutor{
		db,
	}
}

func (s *SQLExecutor) ExecuteSQLStatement(sql string) error {
	util.LogInfo("Exec SQL statement from internal")
	sqlStmt := &SQLStatement{
		sqlType: SqlStatement,
		meta:    fmt.Sprintf("internal sql"),
		sql:     sql,
	}
	if err := s.executeStatement(sqlStmt); err != nil {
		return err
	}
	return nil
}

func (s *SQLExecutor) ExecuteSQLQuery(sql string) (error, string) {
	util.LogInfo("Exec SQL query from internal")
	sqlStmt := &SQLStatement{
		sqlType: SqlQuery,
		meta:    fmt.Sprintf("internal sql"),
		sql:     sql,
	}
	if err := s.executeQuery(sqlStmt); err != nil {
		return err, ""
	}
	return nil, sqlStmt.result
}

func (s *SQLExecutor) ExecuteSQLFile(scanner *bufio.Scanner, fname string, typ configs.SQLStmtType) error {
	util.LogInfo("Exec SQL file: %s", fname)
	parser := NewSQLFileParser(scanner)
	for parser.NextLine() {
		line := parser.Text()
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}

		cmd := fields[0]
		if strings.HasPrefix(cmd, "#") {
			// skip comment lines.
			continue
		}
		if len(fields) != 1 {
			return util.Errorf("SQLFile fmt error, expect [cmd], found [%s]", cmd)
		}
		sqlStmt := &SQLStatement{
			meta: fmt.Sprintf("File name: %s, Line number: %d", fname, parser.LineNumber()),
		}
		switch cmd {
		case "statement":
			sqlStmt.sqlType = SqlStatement
			err := parser.parseStatement(sqlStmt)
			if err != nil {
				return err
			}
			if typ == configs.SQLCreateSource {
				s.warpKafkaStatement(sqlStmt)
			}
			if err := s.executeStatement(sqlStmt); err != nil {
				return err
			}
		case "query":
			sqlStmt.sqlType = SqlQuery
			err := parser.parseStatement(sqlStmt)
			if err != nil {
				return err
			}
			err = s.executeQuery(sqlStmt)
			if err != nil {
				return err
			}
		default:
			return util.Errorf("unknown command: %s", cmd)
		}
		util.LogInfo("ok... rows affected: %d", sqlStmt.rowsNum)
	}
	return nil
}

func (s *SQLExecutor) checkQueryResult(stmt *SQLStatement) bool {
	util.LogInfo("---result---\n%s", stmt.result)
	if stmt.sqlType == SqlQuery && stmt.expectedResult != "" {
		util.LogInfo("---expect---\n%s", stmt.expectedResult)
		var idx1, idx2 int
		for idx1 < len(stmt.result) && idx2 < len(stmt.expectedResult) {
			for stmt.result[idx1] == ' ' || stmt.result[idx1] == '\n' {
				idx1++
			}
			for stmt.expectedResult[idx2] == ' ' || stmt.expectedResult[idx2] == '\n' {
				idx2++
			}
			if idx1 >= len(stmt.result) || idx2 >= len(stmt.expectedResult) {
				break
			} else if stmt.result[idx1] != stmt.expectedResult[idx2] {
				return false
			}
			idx1++
			idx2++
		}
		for idx1 < len(stmt.result) && (stmt.result[idx1] == ' ' || stmt.result[idx1] == '\n') {
			idx1++
		}
		for idx2 < len(stmt.expectedResult) && (stmt.expectedResult[idx2] == ' ' || stmt.expectedResult[idx2] == '\n') {
			idx2++
		}
		if idx1 == len(stmt.result) && idx2 == len(stmt.expectedResult) {
			return true
		} else {
			return false
		}
	}
	return true
}

func (s *SQLExecutor) warpKafkaStatement(stmt *SQLStatement) {
	reg := regexp.MustCompile(`localhost:9092`)
	stmt.sql = reg.ReplaceAllString(stmt.sql, configs.KafkaAddrForFrontend)
}

func (s *SQLExecutor) executeStatement(stmt *SQLStatement) error {
	util.LogInfo("Exec SQL statement")
	start := time.Now()
	res, err := s.db.Exec(stmt.sql)
	duration := time.Now().Sub(start)
	util.LogInfo("duration: %f seconds", duration.Seconds())
	stmt.isExecuted = true
	if err != nil {
		return err
	}
	stmt.rowsNum, err = res.RowsAffected()
	if err != nil {
		return err
	}
	stmt.isSuccess = true
	return nil
}

func (s *SQLExecutor) executeQuery(query *SQLStatement) error {
	util.LogInfo("Exec SQL Query")
	rows, err := s.db.Query(query.sql)
	if err != nil {
		return err
	}
	defer rows.Close()

	query.isExecuted = true

	cols, err := rows.Columns()
	if err != nil {
		return err
	}

	var sb strings.Builder
	for rows.Next() {
		cells := make([]interface{}, len(cols))
		for i := range cells {
			cells[i] = new(string)
		}
		if err := rows.Scan(cells...); err != nil {
			return err
		}
		for _, cell := range cells {
			sb.WriteString(fmt.Sprintf("%s ", *cell.(*string)))
		}
		sb.WriteString("\n")
		query.rowsNum += 1
	}
	if err := rows.Err(); err != nil {
		return err
	}
	query.isSuccess = true
	query.result = strings.TrimSpace(sb.String())
	return nil
}
