package data

import "encoding/json"

type PartSupp struct {
	RowId        int64       `json:"-"`
	PSPartkey    int64       `json:"ps_partkey"`
	PSSuppkey    int64       `json:"ps_suppkey"`
	PSAvailqty   int         `json:"ps_availqty"`
	PSSupplycost json.Number `json:"ps_supplycost"`
	PSComment    string      `json:"ps_comment"`
}

const (
	PSSuppliersPerPart int = 4
	PSAvailableQtyMin  int = 1
	PSAvailableQtyMax  int = 9999
	PSSupplyCostMin    int = 100
	PSSupplyCostMax    int = 100000
	PSCommentAverLen   int = 124
)

type PartSuppGenerator struct {
	scaleFactor float64
	part        int
	partCnt     int
	textPool    *TextPool
	iter        *PartSuppGeneratorIter
}

func NewPartSuppGenerator(scaleFactor float64, part int, partCnt int) *PartSuppGenerator {
	ps := &PartSuppGenerator{
		scaleFactor,
		part,
		partCnt,
		GetTextPool(),
		nil,
	}
	ps.iter = NewPartSuppGeneratorIter(ps.textPool,
		CalcuStart(PartScaleBase, scaleFactor, part, partCnt),
		CalcuRowCnt(PartScaleBase, scaleFactor, part, partCnt),
		scaleFactor,
	)
	return ps
}

func (p *PartSuppGenerator) Next() []byte {
	item := p.iter.Next()
	bytes, _ := json.Marshal(item)
	return bytes
}

func (p *PartSuppGenerator) Capacity() int64 {
	return p.iter.rowCnt * 4
}

type PartSuppGeneratorIter struct {
	idx                int64
	start              int64
	rowCnt             int64
	scaleFactor        float64
	partSupplierNumber int
	availQty           *BoundedRandomInt
	supplyCost         *BoundedRandomInt
	comment            *RandomText
}

func NewPartSuppGeneratorIter(pool *TextPool, start int64, rowCnt int64, factor float64) *PartSuppGeneratorIter {
	ps := &PartSuppGeneratorIter{
		0,
		start,
		rowCnt,
		factor,
		0,
		NewBoundedRandomInt(1671059989, PSSuppliersPerPart, PSAvailableQtyMin, PSAvailableQtyMax),
		NewBoundedRandomInt(1051288424, PSSuppliersPerPart, PSSupplyCostMin, PSSupplyCostMax),
		NewRandomText(1961692154, float64(PSCommentAverLen), PSSuppliersPerPart, pool),
	}

	ps.availQty.AdvanceRows(start)
	ps.supplyCost.AdvanceRows(start)
	ps.comment.AdvanceRows(start)
	return ps
}

func (ps *PartSuppGeneratorIter) Next() *PartSupp {
	if ps.idx > ps.rowCnt {
		return nil
	}

	partSupp := new(PartSupp)
	partKey := ps.start + ps.idx + 1
	partSupp.RowId = partKey
	partSupp.PSPartkey = partKey
	partSupp.PSSuppkey = SelectPartSupp(partKey, int64(ps.partSupplierNumber), ps.scaleFactor)

	partSupp.PSAvailqty, _ = ps.availQty.NextValue()
	cost, _ := ps.supplyCost.NextValue()
	partSupp.PSSupplycost = GetDecimal(int64(cost))
	partSupp.PSComment, _ = ps.comment.NextValue()

	ps.partSupplierNumber++
	if ps.partSupplierNumber >= PSSuppliersPerPart {
		ps.availQty.FinishRow()
		ps.supplyCost.FinishRow()
		ps.comment.FinishRow()
		ps.idx++
		ps.partSupplierNumber = 0
	}
	return partSupp
}

func SelectPartSupp(partKey int64, supplierNumber int64, scale float64) int64 {
	suppCnt := int64(float64(SupplierScaleBase) * scale)
	tmp := supplierNumber * (suppCnt/int64(PSSuppliersPerPart) + (partKey-1)/suppCnt)
	tmp += partKey
	return tmp%suppCnt + 1
}
