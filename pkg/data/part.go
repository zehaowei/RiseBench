package data

import (
	"encoding/json"
	"fmt"
)

type Part struct {
	RowId        int64       `json:"-"`
	PPartkey     int64       `json:"p_partkey"`
	PName        string      `json:"p_name"`
	PMfgr        string      `json:"p_mfgr"`
	PBrand       string      `json:"p_brand"`
	PType        string      `json:"p_type"`
	PSize        int         `json:"p_size"`
	PContainer   string      `json:"p_container"`
	PRetailprice json.Number `json:"p_retailprice"`
	PComment     string      `json:"p_comment"`
}

const (
	PartScaleBase       int = 200000
	PartNameWords       int = 5
	PartManufacturerMin int = 1
	PartManufacturerMax int = 5
	PartBrandMin        int = 1
	PartBrandMax        int = 5
	PartSizeMin         int = 1
	partSizeMax         int = 50
	PartCommentAverLen  int = 14
)

type PartGenerator struct {
	scaleFactor float64
	part        int
	partCnt     int
	distManager *DistributionManager
	textPool    *TextPool
	iter        *PartGeneratorIter
}

func NewPartGenerator(scaleFactor float64, part int, partCnt int) *PartGenerator {
	p := &PartGenerator{
		scaleFactor,
		part,
		partCnt,
		GetDistributionManager(),
		GetTextPool(),
		nil,
	}
	p.iter = NewPartGeneratorIter(
		p.distManager,
		p.textPool,
		CalcuStart(PartScaleBase, scaleFactor, part, partCnt),
		CalcuRowCnt(PartScaleBase, scaleFactor, part, partCnt))
	return p
}

func (p *PartGenerator) Next() []byte {
	item := p.iter.Next()
	bytes, _ := json.Marshal(item)
	return bytes
}

func (p *PartGenerator) Capacity() int64 {
	return p.iter.rowCnt
}

type PartGeneratorIter struct {
	idx          int64
	start        int64
	rowCnt       int64
	name         *RandomStringSequence
	manufacturer *BoundedRandomInt
	brand        *BoundedRandomInt
	partType     *RandomString
	size         *BoundedRandomInt
	container    *RandomString
	comment      *RandomText
}

func NewPartGeneratorIter(distManager *DistributionManager, pool *TextPool, start int64, rowCnt int64) *PartGeneratorIter {
	colors, _ := distManager.GetDistribution("colors")
	types, _ := distManager.GetDistribution("p_types")
	containers, _ := distManager.GetDistribution("p_cntr")
	p := &PartGeneratorIter{
		0,
		start,
		rowCnt,
		NewRandomStringSequence(709314158, PartNameWords, colors, 1),
		NewBoundedRandomInt(1, 1, PartManufacturerMin, PartManufacturerMax),
		NewBoundedRandomInt(46831694, 1, PartBrandMin, PartBrandMax),
		NewRandomString(1841581359, types, 1),
		NewBoundedRandomInt(1193163244, 1, PartSizeMin, partSizeMax),
		NewRandomString(727633698, containers, 1),
		NewRandomText(804159733, float64(PartCommentAverLen), 1, pool),
	}

	p.name.AdvanceRows(start)
	p.manufacturer.AdvanceRows(start)
	p.brand.AdvanceRows(start)
	p.partType.AdvanceRows(start)
	p.size.AdvanceRows(start)
	p.container.AdvanceRows(start)
	p.comment.AdvanceRows(start)
	return p
}

func (p *PartGeneratorIter) Next() *Part {
	if p.idx >= p.rowCnt {
		return nil
	}

	mfgrKey, _ := p.manufacturer.NextValue()
	brandKey, _ := p.brand.NextValue()
	brandKey += mfgrKey * 10
	partKey := p.start + p.idx + 1

	part := new(Part)
	part.RowId = partKey
	part.PPartkey = partKey
	part.PName, _ = p.name.NextValue()
	part.PMfgr = fmt.Sprintf("Manufacturer#%d", mfgrKey)
	part.PBrand = fmt.Sprintf("Brand#%d", brandKey)
	part.PType, _ = p.partType.NextValue()
	part.PSize, _ = p.size.NextValue()
	part.PContainer, _ = p.container.NextValue()
	part.PComment, _ = p.comment.NextValue()
	part.PRetailprice = GetDecimal(CalcuPartPrice(partKey))

	p.name.FinishRow()
	p.manufacturer.FinishRow()
	p.brand.FinishRow()
	p.partType.FinishRow()
	p.size.FinishRow()
	p.container.FinishRow()
	p.comment.FinishRow()
	p.idx++
	return part
}

func CalcuPartPrice(partKey int64) int64 {
	price := int64(90000)
	price += (partKey / 10) % 20001
	price += (partKey % 1000) * 100
	return price
}
