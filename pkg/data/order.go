package data

import (
	"encoding/json"
	"fmt"
	"math"
)

type Order struct {
	RowId          int64       `json:"-"`
	OOrderkey      int64       `json:"o_orderkey"`
	OCustkey       int64       `json:"o_custkey"`
	OOrderstatus   string      `json:"o_orderstatus"`
	OTotalprice    json.Number `json:"o_totalprice"`
	OOrderdate     string      `json:"o_orderdate"`
	OOrderpriority string      `json:"o_orderpriority"`
	OClerk         string      `json:"o_clerk"`
	OShippriority  int64       `json:"o_shippriority"`
	OComment       string      `json:"o_comment"`
}

const (
	CustomerMortality   int = 3
	OrderScaleBase      int = 1_500_000
	OrderDateMin            = BaseDate
	OrderDateMax            = BaseDate + (DateRange - LineItemShipDateMax - LineItemReceiptDateMax - 1)
	ClerkScaleBase      int = 1000
	LineCntMin          int = 1
	LineCntMax          int = 7
	OrderCommentAverLen int = 49
	OrderKeySparseBits  int = 2
	OrderKeySparseKeep  int = 3
)

type OrderGenerator struct {
	scaleFactor float64
	part        int
	partCnt     int
	distManager *DistributionManager
	textPool    *TextPool
	iter        *OrderGeneratorIter
}

func NewOrderGenerator(scaleFactor float64, part int, partCnt int) *OrderGenerator {
	o := &OrderGenerator{
		scaleFactor,
		part,
		partCnt,
		GetDistributionManager(),
		GetTextPool(),
		nil,
	}
	o.iter = NewOrderGeneratorIter(o.distManager, o.textPool,
		CalcuStart(OrderScaleBase, scaleFactor, part, partCnt),
		CalcuRowCnt(OrderScaleBase, scaleFactor, part, partCnt),
		scaleFactor)
	return o
}

func (o *OrderGenerator) Next() []byte {
	item := o.iter.Next()
	bytes, _ := json.Marshal(item)
	return bytes
}

func (o *OrderGenerator) Capacity() int64 {
	return o.iter.rowCnt
}

type OrderGeneratorIter struct {
	idx            int64
	start          int64
	rowCnt         int64
	maxCustomerKey int64
	orderDate      *BoundedRandomInt
	lineCnt        *BoundedRandomInt
	customerKey    *BoundedRandomLong
	orderPriority  *RandomString
	clerk          *BoundedRandomInt
	comment        *RandomText
	lineQty        *BoundedRandomInt
	lineDiscount   *BoundedRandomInt
	lineTax        *BoundedRandomInt
	linePartKey    *BoundedRandomLong
	lineShipDate   *BoundedRandomInt
}

func NewOrderGeneratorIter(distManager *DistributionManager, pool *TextPool, start int64, rowCnt int64, factor float64) *OrderGeneratorIter {
	o := &OrderGeneratorIter{
		0,
		start,
		rowCnt,
		int64(float64(CustomerScaleBase) * factor),
		NewBoundedRandomInt(1066728069, 1, OrderDateMin, OrderDateMax),
		NewBoundedRandomInt(1434868289, 1, LineCntMin, LineCntMax),
		nil,
		nil,
		NewBoundedRandomInt(1171034773, 1, 1, int(math.Max(factor*float64(ClerkScaleBase), float64(ClerkScaleBase)))),
		NewRandomText(276090261, float64(OrderCommentAverLen), 1, pool),
		LineItemRandom("qty"),
		LineItemRandom("discount"),
		LineItemRandom("tax"),
		LinePartKey(factor),
		LineItemRandom("shipDate"),
	}
	o.customerKey = NewBoundedRandomLong(factor >= 30000, 851767375, 1, 1, o.maxCustomerKey)
	priorities, _ := distManager.GetDistribution("o_oprio")
	o.orderPriority = NewRandomString(591449447, priorities, 1)

	o.orderDate.AdvanceRows(start)
	o.lineCnt.AdvanceRows(start)
	o.customerKey.AdvanceRows(start)
	o.orderPriority.AdvanceRows(start)
	o.clerk.AdvanceRows(start)
	o.comment.AdvanceRows(start)
	o.lineQty.AdvanceRows(start)
	o.lineDiscount.AdvanceRows(start)
	o.lineTax.AdvanceRows(start)
	o.linePartKey.AdvanceRows(start)
	o.lineShipDate.AdvanceRows(start)
	return o
}

func (o *OrderGeneratorIter) Next() *Order {
	if o.idx >= o.rowCnt {
		return nil
	}

	idx := o.start + o.idx + 1
	orderKey := MakeOrderKey(idx)
	orderDate, _ := o.orderDate.NextValue()

	customerKey, _ := o.customerKey.NextValue()
	delta := int64(1)
	for customerKey%int64(CustomerMortality) == 0 {
		customerKey += delta
		customerKey = int64(math.Min(float64(customerKey), float64(o.maxCustomerKey)))
		delta *= -1
	}

	totalPrice := int64(0)
	shippedCnt := 0
	lineCnt, _ := o.lineCnt.NextValue()
	for i := 0; i < lineCnt; i++ {
		quantity, _ := o.lineQty.NextValue()
		discount, _ := o.lineDiscount.NextValue()
		tax, _ := o.lineTax.NextValue()
		partKey, _ := o.linePartKey.NextValue()
		partPrice := CalcuPartPrice(partKey)
		extendedPrice := partPrice * int64(quantity)
		discountPrice := extendedPrice * int64(100-discount)
		totalPrice += ((discountPrice / int64(100)) * int64(100+tax)) / 100

		shipDate, _ := o.lineShipDate.NextValue()
		shipDate += orderDate
		if IsPast(shipDate) {
			shippedCnt++
		}
	}
	orderStatus := ""
	if shippedCnt == lineCnt {
		orderStatus = "F"
	} else if shippedCnt > 0 {
		orderStatus = "P"
	} else {
		orderStatus = "O"
	}

	order := new(Order)
	order.RowId = idx
	order.OOrderkey = orderKey
	order.OCustkey = customerKey
	order.OOrderstatus = orderStatus
	order.OTotalprice = GetDecimal(totalPrice)
	order.OOrderdate = DateToString(orderDate)
	order.OOrderpriority, _ = o.orderPriority.NextValue()
	clerkId, _ := o.clerk.NextValue()
	order.OClerk = fmt.Sprintf("Clerk#%09d", clerkId)
	order.OShippriority = 0
	order.OComment, _ = o.comment.NextValue()

	o.orderDate.FinishRow()
	o.lineCnt.FinishRow()
	o.customerKey.FinishRow()
	o.orderPriority.FinishRow()
	o.clerk.FinishRow()
	o.comment.FinishRow()
	o.lineQty.FinishRow()
	o.lineDiscount.FinishRow()
	o.lineTax.FinishRow()
	o.linePartKey.FinishRow()
	o.lineShipDate.FinishRow()
	o.idx++
	return order
}

func MakeOrderKey(orderIdx int64) int64 {
	orderKey := orderIdx
	orderKey >>= OrderKeySparseKeep
	orderKey <<= OrderKeySparseBits
	orderKey <<= OrderKeySparseKeep
	orderKey += orderIdx & ((1 << OrderKeySparseKeep) - 1)
	return orderKey
}
