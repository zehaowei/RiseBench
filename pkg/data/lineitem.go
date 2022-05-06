package data

import (
	"encoding/json"
	"github.com/singularity-data/tpch-bench/pkg/configs"
	"github.com/singularity-data/tpch-bench/pkg/util"
	"regexp"
	"strconv"
	"strings"
)

type LineItem struct {
	RowId          int64       `json:"-"`
	LOrderkey      int64       `json:"l_orderkey"`
	LPartkey       int         `json:"l_partkey"`
	LSuppkey       int         `json:"l_suppkey"`
	LLinenumber    int         `json:"l_linenumber"`
	LQuantity      json.Number `json:"l_quantity"`
	LExtendedprice json.Number `json:"l_extendedprice"`
	LDiscount      json.Number `json:"l_discount"`
	LTax           json.Number `json:"l_tax"`
	LReturnflag    string      `json:"l_returnflag"`
	LLinestatus    string      `json:"l_linestatus"`
	LShipdate      string      `json:"l_shipdate"`
	LCommitdate    string      `json:"l_commitdate"`
	LReceiptdate   string      `json:"l_receiptdate"`
	LShipinstruct  string      `json:"l_shipinstruct"`
	LShipmode      string      `json:"l_shipmode"`
	LComment       string      `json:"l_comment"`
}

const (
	LineItemQtyMin         int = 1
	LineItemQtyMax         int = 50
	LineItemTaxMin         int = 0
	LineItemTaxMax         int = 8
	LineItemDiscountMin    int = 0
	LineItemDiscountMax    int = 10
	LineItemPartKeyMin     int = 1
	LineItemShipDateMin    int = 1
	LineItemShipDateMax    int = 121
	LineItemCommitDateMin  int = 30
	LineItemCommitDateMax  int = 90
	LineItemReceiptDateMin int = 1
	LineItemReceiptDateMax int = 30
	LineItemCommentAverLen int = 27
)

type LineItemGenerator struct {
	scaleFactor float64
	part        int
	partCnt     int
	distManager *DistributionManager
	textPool    *TextPool
	iter        *LineItemGeneratorIter
}

func NewLineItemGenerator(scaleFactor float64, part int, partCnt int) *LineItemGenerator {
	l := &LineItemGenerator{
		scaleFactor,
		part,
		partCnt,
		GetDistributionManager(),
		GetTextPool(),
		nil,
	}
	l.iter = NewLineItemGeneratorIter(l.distManager, l.textPool,
		CalcuStart(OrderScaleBase, scaleFactor, part, partCnt),
		CalcuRowCnt(OrderScaleBase, scaleFactor, part, partCnt),
		scaleFactor)
	return l
}

func (l *LineItemGenerator) Next() []byte {
	item := l.iter.Next()
	bytes, _ := json.Marshal(item)
	return bytes
}

func (l *LineItemGenerator) Capacity() int64 {
	return l.iter.rowCnt * 4
}

type LineItemGeneratorIter struct {
	idx             int64
	start           int64
	rowCnt          int64
	scaleFactor     float64
	orderDate       int
	lineCnt         int
	lineNumber      int
	orderDateRandom *BoundedRandomInt
	lineCntRandom   *BoundedRandomInt
	qty             *BoundedRandomInt
	discount        *BoundedRandomInt
	tax             *BoundedRandomInt
	linePartKey     *BoundedRandomLong
	supplierNumber  *BoundedRandomInt
	shipDate        *BoundedRandomInt
	commitDate      *BoundedRandomInt
	receiptDate     *BoundedRandomInt
	returnedFlag    *RandomString
	shipInstruction *RandomString
	shipMode        *RandomString
	comment         *RandomText
}

func NewLineItemGeneratorIter(distManager *DistributionManager, pool *TextPool, start int64, rowCnt int64, factor float64) *LineItemGeneratorIter {
	l := &LineItemGeneratorIter{
		0,
		start,
		rowCnt,
		factor,
		-1,
		-1,
		0,
		NewBoundedRandomInt(1066728069, 1, OrderDateMin, OrderDateMax),
		NewBoundedRandomInt(1434868289, 1, LineCntMin, LineCntMax),
		LineItemRandom("qty"),
		LineItemRandom("discount"),
		LineItemRandom("tax"),
		LinePartKey(factor),
		NewBoundedRandomInt(2095021727, LineCntMax, 0, 3),
		LineItemRandom("shipDate"),
		NewBoundedRandomInt(904914315, LineCntMax, LineItemCommitDateMin, LineItemCommitDateMax),
		NewBoundedRandomInt(373135028, LineCntMax, LineItemReceiptDateMin, LineItemReceiptDateMax),
		nil,
		nil,
		nil,
		NewRandomText(1095462486, float64(LineItemCommentAverLen), LineCntMax, pool),
	}
	flags, _ := distManager.GetDistribution("rflag")
	l.returnedFlag = NewRandomString(717419739, flags, LineCntMax)
	instructions, _ := distManager.GetDistribution("instruct")
	l.shipInstruction = NewRandomString(1371272478, instructions, LineCntMax)
	shipModes, _ := distManager.GetDistribution("smode")
	l.shipMode = NewRandomString(675466456, shipModes, LineCntMax)

	l.orderDate, _ = l.orderDateRandom.NextValue()
	l.lineCnt, _ = l.lineCntRandom.NextValue()
	l.lineCnt--
	return l
}

func (l *LineItemGeneratorIter) Next() *LineItem {
	if l.idx >= l.rowCnt {
		return nil
	}

	idx := l.start + l.idx + 1
	orderKey := MakeOrderKey(idx)

	quantity, _ := l.qty.NextValue()
	discount, _ := l.discount.NextValue()
	tax, _ := l.tax.NextValue()

	partKey, _ := l.linePartKey.NextValue()
	supplierNumber, _ := l.supplierNumber.NextValue()
	supplierKey := SelectPartSupp(partKey, int64(supplierNumber), l.scaleFactor)

	partPrice := CalcuPartPrice(partKey)
	extendPrice := partPrice * int64(quantity)

	shipDate, _ := l.shipDate.NextValue()
	shipDate += l.orderDate
	commitDate, _ := l.commitDate.NextValue()
	commitDate += l.orderDate
	receiptDate, _ := l.receiptDate.NextValue()
	receiptDate += shipDate

	returnedFlag := ""
	if IsPast(receiptDate) {
		returnedFlag, _ = l.returnedFlag.NextValue()
	} else {
		returnedFlag = "N"
	}

	status := ""
	if IsPast(shipDate) {
		status = "F"
	} else {
		status = "O"
	}

	shipInstruction, _ := l.shipInstruction.NextValue()
	shipMode, _ := l.shipMode.NextValue()
	comment, _ := l.comment.NextValue()

	lineItem := &LineItem{
		idx,
		orderKey,
		int(partKey),
		int(supplierKey),
		l.lineNumber + 1,
		json.Number(strconv.Itoa(quantity)),
		GetDecimal(extendPrice),
		GetDecimal(int64(discount)),
		GetDecimal(int64(tax)),
		returnedFlag,
		status,
		DateToString(shipDate),
		DateToString(commitDate),
		DateToString(receiptDate),
		shipInstruction,
		shipMode,
		comment,
	}
	l.lineNumber++

	if l.lineNumber > l.lineCnt {
		l.orderDateRandom.FinishRow()
		l.lineCntRandom.FinishRow()
		l.qty.FinishRow()
		l.discount.FinishRow()
		l.tax.FinishRow()
		l.linePartKey.FinishRow()
		l.supplierNumber.FinishRow()
		l.shipDate.FinishRow()
		l.commitDate.FinishRow()
		l.receiptDate.FinishRow()
		l.returnedFlag.FinishRow()
		l.shipInstruction.FinishRow()
		l.shipMode.FinishRow()
		l.comment.FinishRow()
		l.idx++

		l.lineCnt, _ = l.lineCntRandom.NextValue()
		l.lineCnt--
		l.orderDate, _ = l.orderDateRandom.NextValue()
		l.lineNumber = 0
	}
	return lineItem
}

func LineItemRandom(random string) *BoundedRandomInt {
	switch random {
	case "qty":
		return NewBoundedRandomInt(209208115, LineCntMax, LineItemQtyMin, LineItemQtyMax)
	case "discount":
		return NewBoundedRandomInt(554590007, LineCntMax, LineItemDiscountMin, LineItemDiscountMax)
	case "tax":
		return NewBoundedRandomInt(721958466, LineCntMax, LineItemTaxMin, LineItemTaxMax)
	case "shipDate":
		return NewBoundedRandomInt(1769349045, LineCntMax, LineItemShipDateMin, LineItemShipDateMax)
	default:
		return nil
	}
}

func LinePartKey(scale float64) *BoundedRandomLong {
	return NewBoundedRandomLong(scale >= 30000, 1808217256, LineCntMax, int64(LineItemPartKeyMin), int64(float64(PartScaleBase)*scale))
}

func LineItemsFromTblFile() ([][]byte, error) {
	scanner := util.ReadFile(configs.LineItemTblFilePath)
	lineItems := make([][]byte, 0)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		tokens := strings.Split(line, "|")

		field1, err := strconv.ParseInt(tokens[0], 10, 64)
		if err != nil {
			return nil, util.Errorf("Invalid LineItem field")
		}
		field2, err := strconv.ParseInt(tokens[1], 10, 32)
		if err != nil {
			return nil, util.Errorf("Invalid LineItem field")
		}
		field3, err := strconv.ParseInt(tokens[2], 10, 32)
		if err != nil {
			return nil, util.Errorf("Invalid LineItem field")
		}
		field4, err := strconv.ParseInt(tokens[3], 10, 32)
		if err != nil {
			return nil, util.Errorf("Invalid LineItem field")
		}
		lineItem := LineItem{
			0,
			field1,
			int(field2),
			int(field3),
			int(field4),
			json.Number(tokens[4]),
			json.Number(tokens[5]),
			json.Number(tokens[6]),
			json.Number(tokens[7]),
			tokens[8],
			tokens[9],
			tokens[10],
			tokens[11],
			tokens[12],
			tokens[13],
			tokens[14],
			tokens[15],
		}

		itemBytes, err := json.Marshal(lineItem)
		if err != nil {
			util.LogErr(err.Error())
			continue
		}
		lineItems = append(lineItems, itemBytes)
	}
	util.LogInfo("nums of lineItems: %d", len(lineItems))
	return lineItems, nil
}

func LineItemsFromSqlFile() ([][]byte, error) {
	scanner := util.ReadFile(configs.LineItemSqlFilePath)
	lineItems := make([][]byte, 0)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		line = strings.TrimLeft(line, "(")
		line = strings.TrimRight(line, "),")
		tokens := strings.SplitN(line, ",", 9)
		if len(tokens) != 9 {
			util.LogInfo("row: %s", line)
			return nil, util.Errorf("LineItem file format error, wrong number of fields")
		}

		line = tokens[8]
		r, _ := regexp.Compile("'[^']*'")
		tokens2 := r.FindAllString(line, -1)
		if len(tokens2) != 8 {
			util.LogInfo("row: %s", line)
			return nil, util.Errorf("LineItem file format error, wrong number of fields")
		}
		for i := range tokens2 {
			tokens2[i] = strings.TrimRight(strings.TrimLeft(tokens2[i], "'"), "'")
		}

		field1, err := strconv.ParseInt(tokens[0], 10, 64)
		if err != nil {
			return nil, util.Errorf("Invalid LineItem field")
		}
		field2, err := strconv.ParseInt(tokens[1], 10, 32)
		if err != nil {
			return nil, util.Errorf("Invalid LineItem field")
		}
		field3, err := strconv.ParseInt(tokens[2], 10, 32)
		if err != nil {
			return nil, util.Errorf("Invalid LineItem field")
		}
		field4, err := strconv.ParseInt(tokens[3], 10, 32)
		if err != nil {
			return nil, util.Errorf("Invalid LineItem field")
		}
		lineItem := LineItem{
			0,
			field1,
			int(field2),
			int(field3),
			int(field4),
			json.Number(tokens[4]),
			json.Number(tokens[5]),
			json.Number(tokens[6]),
			json.Number(tokens[7]),
			tokens2[0],
			tokens2[1],
			tokens2[2],
			tokens2[3],
			tokens2[4],
			tokens2[5],
			tokens2[6],
			tokens2[7],
		}

		itemBytes, err := json.Marshal(lineItem)
		if err != nil {
			util.LogErr(err.Error())
			continue
		}
		lineItems = append(lineItems, itemBytes)
	}
	return lineItems, nil
}
