package data

import (
	"encoding/json"
	"fmt"
	"sync"
)

func CalcuRowCnt(scaleBase int, factor float64, part int, partCnt int) int64 {
	rowCntTotal := float64(scaleBase) * factor
	rowCnt := int64(rowCntTotal / float64(partCnt))
	if part == partCnt {
		rowCnt += int64(rowCntTotal) % int64(partCnt)
	}
	return rowCnt
}

func CalcuStart(scaleBase int, factor float64, part int, partCnt int) int64 {
	rowCntTotal := float64(scaleBase) * factor
	rowCnt := int64(rowCntTotal / float64(partCnt))
	return rowCnt * int64(part-1)
}

const (
	BaseDate    int = 92001 // year: 92 day: 001
	CurrentDate int = 95168
	DateRange   int = 2557
)

var dayStartPerMonth = []int{0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365}
var onceDateMap sync.Once
var dateMap []string

func getDateMap() []string {
	onceDateMap.Do(func() {
		dateMap = make([]string, 0)
		for i := 0; i < DateRange; i++ {
			dateMap = append(dateMap, idxToDate(i+1))
		}
	})
	return dateMap
}

func idxToDate(idx int) string {
	y := transformIdx(idx+BaseDate-1) / 1000
	d := transformIdx(idx+BaseDate-1) % 1000

	m := 0
	for d > dayStartPerMonth[m]+adjust(y, m) {
		m++
	}
	d -= dayStartPerMonth[m-1]
	if isLeapYear(y) && m > 2 {
		d -= 1
	}
	return fmt.Sprintf("19%02d-%02d-%02d", y, m, d)
}

func adjust(y int, m int) int {
	if isLeapYear(y) && m >= 2 {
		return 1
	}
	return 0
}

func transformIdx(date int) int {
	offset := date - BaseDate
	re := BaseDate
	for {
		y := re / 1000
		leap := 0
		if isLeapYear(y) {
			leap = 1
		}
		yMax := y*1000 + 365 + leap
		if re+offset <= yMax {
			break
		}

		offset -= yMax - re + 1
		re += 1000
	}
	return re + offset
}

func isLeapYear(y int) bool {
	return y%4 == 0 && y%100 != 0
}

func DateToString(date int) string {
	return getDateMap()[date-BaseDate]
}

func IsPast(date int) bool {
	return transformIdx(date) <= CurrentDate
}

func GetDecimal(num int64) json.Number {
	mainPart := num / 100
	decimalPart := num % 100
	return json.Number(fmt.Sprintf("%d.%02d", mainPart, decimalPart))
}
