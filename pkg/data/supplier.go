package data

import (
	"encoding/json"
	"fmt"
)

type Supplier struct {
	RowId      int64       `json:"-"`
	SSuppkey   int64       `json:"s_suppkey"`
	SName      string      `json:"s_name"`
	SAddress   string      `json:"s_address"`
	SNationkey int64       `json:"s_nationkey"`
	SPhone     string      `json:"s_phone"`
	SAcctbal   json.Number `json:"s_acctbal"`
	SComment   string      `json:"s_comment"`
}

const (
	SupplierScaleBase         int = 10000
	SupplierAccountBalanceMin int = -99999
	SupplierAccountBalanceMax int = 999999
	SupplierAddressAverLen    int = 25
	SupplierCommentAverLen    int = 63
)

type SupplierGenerator struct {
	scaleFactor float64
	part        int
	partCnt     int
	distManager *DistributionManager
	textPool    *TextPool
	iter        *SupplierGeneratorIter
}

func NewSupplierGenerator(scaleFactor float64, part int, partCnt int) *SupplierGenerator {
	s := &SupplierGenerator{
		scaleFactor,
		part,
		partCnt,
		GetDistributionManager(),
		GetTextPool(),
		nil,
	}
	s.iter = NewSupplierGeneratorIter(
		s.distManager,
		s.textPool,
		CalcuStart(SupplierScaleBase, scaleFactor, part, partCnt),
		CalcuRowCnt(SupplierScaleBase, scaleFactor, part, partCnt))
	return s
}

func (s *SupplierGenerator) Next() []byte {
	item := s.iter.Next()
	bytes, _ := json.Marshal(item)
	return bytes
}

func (s *SupplierGenerator) Capacity() int64 {
	return s.iter.rowCnt
}

type SupplierGeneratorIter struct {
	idx            int64
	start          int64
	rowCnt         int64
	addr           *RandomAlphaNumeric
	nationKey      *BoundedRandomInt
	phoneNumber    *RandomPhoneNumber
	accountBalance *BoundedRandomInt
	comment        *RandomText
}

func NewSupplierGeneratorIter(distManager *DistributionManager, pool *TextPool, start int64, rowCnt int64) *SupplierGeneratorIter {
	s := new(SupplierGeneratorIter)
	s.idx = 0
	s.start = start
	s.rowCnt = rowCnt
	nations, _ := distManager.GetDistribution("nations")
	s.nationKey = NewBoundedRandomInt(110356601, 1, 0, nations.Size()-1)
	s.comment = NewRandomText(1341315363, float64(SupplierCommentAverLen), 1, pool)
	s.addr = NewRandomAlphaNumeric(706178559, SupplierAddressAverLen, 1)
	s.phoneNumber = NewRandomPhoneNumber(884434366, 1)
	s.accountBalance = NewBoundedRandomInt(962338209, 1, SupplierAccountBalanceMin, SupplierAccountBalanceMax)

	s.addr.AdvanceRows(start)
	s.nationKey.AdvanceRows(start)
	s.phoneNumber.AdvanceRows(start)
	s.accountBalance.AdvanceRows(start)
	s.comment.AdvanceRows(start)
	return s
}

func (s *SupplierGeneratorIter) Next() *Supplier {
	if s.idx >= s.rowCnt {
		return nil
	}

	supplier := new(Supplier)
	supplier.RowId = s.start + s.idx + 1
	supplier.SSuppkey = s.start + s.idx + 1
	supplier.SName = fmt.Sprintf("Supplier#%09d", supplier.SSuppkey)
	supplier.SAddress, _ = s.addr.NextValue()
	nationKey, _ := s.nationKey.NextValue()
	supplier.SNationkey = int64(nationKey)
	supplier.SPhone, _ = s.phoneNumber.NextValue(int64(nationKey))
	acctBal, _ := s.accountBalance.NextValue()
	supplier.SAcctbal = GetDecimal(int64(acctBal))
	supplier.SComment, _ = s.comment.NextValue()

	s.addr.FinishRow()
	s.nationKey.FinishRow()
	s.phoneNumber.FinishRow()
	s.accountBalance.FinishRow()
	s.comment.FinishRow()
	s.idx++
	return supplier
}
