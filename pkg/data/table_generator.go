package data

import "github.com/singularity-data/tpch-bench/pkg/configs"

type TableGeneratorConfig struct {
	ScaleFactor   float64
	TablePartsMap map[configs.TpchTable]int
}

// TableGenerator every specific table generator could generate data concurrently
// Table 'orders' 	BaseScale: 1,500,000
// Table 'lineitem' BaseScale: 6,000,000
// Table 'customer' BaseScale: 150,000
// Table 'part' 	BaseScale: 200,000
// Table 'supplier' BaseScale: 10,000
// Table 'partsupp' BaseScale: 800,000
// Table 'nation' 	fixed: 25
// Table 'region' 	fixed: 5
type TableGenerator struct {
	OrderGen    []*OrderGenerator
	LineItemGen []*LineItemGenerator
	CustomerGen []*CustomerGenerator
	SupplierGen []*SupplierGenerator
	PartGen     []*PartGenerator
	PartSuppGen []*PartSuppGenerator
	NationsGen  []*NationGenerator
	RegionsGen  []*RegionGenerator
}

func NewTableGeneratorDefault(scaleFactor float64) *TableGenerator {
	t := &TableGenerator{
		[]*OrderGenerator{NewOrderGenerator(scaleFactor, 1, 1)},
		[]*LineItemGenerator{NewLineItemGenerator(scaleFactor, 1, 1)},
		[]*CustomerGenerator{NewCustomerGenerator(scaleFactor, 1, 1)},
		[]*SupplierGenerator{NewSupplierGenerator(scaleFactor, 1, 1)},
		[]*PartGenerator{NewPartGenerator(scaleFactor, 1, 1)},
		[]*PartSuppGenerator{NewPartSuppGenerator(scaleFactor, 1, 1)},
		[]*NationGenerator{NewNationGenerator()},
		[]*RegionGenerator{NewRegionGenerator()},
	}
	return t
}

func NewTableGenerator(config *TableGeneratorConfig) *TableGenerator {
	t := &TableGenerator{}

	orderParts := config.TablePartsMap[configs.Orders]
	t.OrderGen = make([]*OrderGenerator, orderParts)
	for i := 0; i < orderParts; i++ {
		t.OrderGen[i] = NewOrderGenerator(config.ScaleFactor, i+1, orderParts)
	}

	lineItemParts := config.TablePartsMap[configs.LineItem]
	t.LineItemGen = make([]*LineItemGenerator, lineItemParts)
	for i := 0; i < lineItemParts; i++ {
		t.LineItemGen[i] = NewLineItemGenerator(config.ScaleFactor, i+1, lineItemParts)
	}

	customerParts := config.TablePartsMap[configs.Customer]
	t.CustomerGen = make([]*CustomerGenerator, customerParts)
	for i := 0; i < customerParts; i++ {
		t.CustomerGen[i] = NewCustomerGenerator(config.ScaleFactor, i+1, customerParts)
	}

	supplierParts := config.TablePartsMap[configs.Supplier]
	t.SupplierGen = make([]*SupplierGenerator, supplierParts)
	for i := 0; i < supplierParts; i++ {
		t.SupplierGen[i] = NewSupplierGenerator(config.ScaleFactor, i+1, supplierParts)
	}

	partParts := config.TablePartsMap[configs.Part]
	t.PartGen = make([]*PartGenerator, partParts)
	for i := 0; i < partParts; i++ {
		t.PartGen[i] = NewPartGenerator(config.ScaleFactor, i+1, partParts)
	}

	partSuppParts := config.TablePartsMap[configs.PartSupp]
	t.PartSuppGen = make([]*PartSuppGenerator, partSuppParts)
	for i := 0; i < partSuppParts; i++ {
		t.PartSuppGen[i] = NewPartSuppGenerator(config.ScaleFactor, i+1, partSuppParts)
	}

	t.NationsGen = make([]*NationGenerator, 1)
	t.NationsGen[0] = NewNationGenerator()
	t.RegionsGen = make([]*RegionGenerator, 1)
	t.RegionsGen[0] = NewRegionGenerator()

	return t
}

func (t *TableGenerator) GetSingleTableGenerator(table configs.TpchTable, i int) JsonIterable {
	switch table {
	case configs.LineItem:
		return t.LineItemGen[i]
	case configs.Orders:
		return t.OrderGen[i]
	case configs.Customer:
		return t.CustomerGen[i]
	case configs.Part:
		return t.PartGen[i]
	case configs.Supplier:
		return t.SupplierGen[i]
	case configs.PartSupp:
		return t.PartSuppGen[i]
	case configs.Nation:
		return t.NationsGen[0]
	case configs.Region:
		return t.RegionsGen[0]
	default:
		return nil
	}
}

// JsonIterable Make generator able to generator json format item
type JsonIterable interface {
	Next() []byte
	Capacity() int64
}
