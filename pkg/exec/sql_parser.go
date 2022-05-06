package exec

import (
	"bufio"
	"strings"
)

type SQLFileParser struct {
	scanner *bufio.Scanner
	lineNum int32
}

func NewSQLFileParser(s *bufio.Scanner) *SQLFileParser {
	return &SQLFileParser{
		scanner: s,
		lineNum: 0,
	}
}

func (s *SQLFileParser) NextLine() bool {
	ok := s.scanner.Scan()
	if ok {
		s.lineNum++
	}
	return ok
}

func (s *SQLFileParser) Text() string {
	return s.scanner.Text()
}

func (s *SQLFileParser) LineNumber() int32 {
	return s.lineNum
}

func (s *SQLFileParser) parseBlock(sb *strings.Builder, lineEnder byte, blockEnder []string) {
	for s.scanner.Scan() {
		s.lineNum++
		line := s.scanner.Text()
		for _, be := range blockEnder {
			if line == be {
				return
			}
		}
		sb.WriteString(line)
		sb.WriteByte(lineEnder)
	}
}

func (s *SQLFileParser) parseStatement(sqlStmt *SQLStatement) error {
	var sb strings.Builder
	s.parseBlock(&sb, ' ', []string{"", "----"})
	sqlStmt.sql = strings.TrimSpace(sb.String())
	if sqlStmt.sqlType == SqlQuery && s.scanner.Text() == "----" {
		sb.Reset()
		s.parseBlock(&sb, '\n', []string{""})
	}
	sqlStmt.expectedResult = strings.TrimSpace(sb.String())
	return nil
}
