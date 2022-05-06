package data

import (
	"encoding/json"
	"fmt"
)

type Customer struct {
	RowId       int64       `json:"-"`
	CCustkey    int64       `json:"c_custkey"`
	CName       string      `json:"c_name"`
	CAddress    string      `json:"c_address"`
	CNationkey  int64       `json:"c_nationkey"`
	CPhone      string      `json:"c_phone"`
	CAcctbal    json.Number `json:"c_acctbal"`
	CMktsegment string      `json:"c_mktsegment"`
	CComment    string      `json:"c_comment"`
}

const (
	CustomerScaleBase         int = 150000
	CustomerAccountBalanceMin int = -99999
	CustomerAccountBalanceMax int = 999999
	CustomerAddressAverLen    int = 25
	CustomerCommentAverLen    int = 73
)

type CustomerGenerator struct {
	scaleFactor float64
	part        int
	partCnt     int
	distManager *DistributionManager
	textPool    *TextPool
	iter        *CustomerGeneratorIter
}

func NewCustomerGenerator(scaleFactor float64, part int, partCnt int) *CustomerGenerator {
	c := &CustomerGenerator{
		scaleFactor,
		part,
		partCnt,
		GetDistributionManager(),
		GetTextPool(),
		nil,
	}
	c.iter = NewCustomerGeneratorIter(
		c.distManager,
		c.textPool,
		CalcuStart(CustomerScaleBase, scaleFactor, part, partCnt),
		CalcuRowCnt(CustomerScaleBase, scaleFactor, part, partCnt))
	return c
}

func (c *CustomerGenerator) Next() []byte {
	item := c.iter.Next()
	bytes, _ := json.Marshal(item)
	return bytes
}

func (c *CustomerGenerator) Capacity() int64 {
	return c.iter.rowCnt
}

type CustomerGeneratorIter struct {
	idx            int64
	start          int64
	rowCnt         int64
	addr           *RandomAlphaNumeric
	nationKey      *BoundedRandomInt
	phoneNumber    *RandomPhoneNumber
	accountBalance *BoundedRandomInt
	mktSegment     *RandomString
	comment        *RandomText
}

func NewCustomerGeneratorIter(distManager *DistributionManager, pool *TextPool, start int64, rowCnt int64) *CustomerGeneratorIter {
	c := new(CustomerGeneratorIter)
	c.idx = 0
	c.start = start
	c.rowCnt = rowCnt
	nations, _ := distManager.GetDistribution("nations")
	c.nationKey = NewBoundedRandomInt(1489529863, 1, 0, nations.Size()-1)
	mkts, _ := distManager.GetDistribution("msegmnt")
	c.mktSegment = NewRandomString(1140279430, mkts, 1)
	c.comment = NewRandomText(1335826707, float64(CustomerCommentAverLen), 1, pool)
	c.addr = NewRandomAlphaNumeric(881155353, CustomerAddressAverLen, 1)
	c.phoneNumber = NewRandomPhoneNumber(1521138112, 1)
	c.accountBalance = NewBoundedRandomInt(298370230, 1, CustomerAccountBalanceMin, CustomerAccountBalanceMax)

	c.addr.AdvanceRows(start)
	c.nationKey.AdvanceRows(start)
	c.phoneNumber.AdvanceRows(start)
	c.accountBalance.AdvanceRows(start)
	c.mktSegment.AdvanceRows(start)
	c.comment.AdvanceRows(start)
	return c
}

func (c *CustomerGeneratorIter) Next() *Customer {
	if c.idx >= c.rowCnt {
		return nil
	}

	customer := new(Customer)
	customer.RowId = c.start + c.idx + 1
	customer.CCustkey = c.start + c.idx + 1
	customer.CName = fmt.Sprintf("Customer#%09d", customer.CCustkey)
	customer.CAddress, _ = c.addr.NextValue()
	nationKey, _ := c.nationKey.NextValue()
	customer.CNationkey = int64(nationKey)
	customer.CPhone, _ = c.phoneNumber.NextValue(int64(nationKey))
	acctBal, _ := c.accountBalance.NextValue()
	customer.CAcctbal = GetDecimal(int64(acctBal))
	customer.CMktsegment, _ = c.mktSegment.NextValue()
	customer.CComment, _ = c.comment.NextValue()

	c.addr.FinishRow()
	c.nationKey.FinishRow()
	c.phoneNumber.FinishRow()
	c.accountBalance.FinishRow()
	c.mktSegment.FinishRow()
	c.comment.FinishRow()
	c.idx++
	return customer
}
