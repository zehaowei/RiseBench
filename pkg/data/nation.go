package data

import (
	"encoding/json"
	"github.com/singularity-data/tpch-bench/pkg/util"
)

type Nation struct {
	RowId      int64  `json:"-"`
	NNationkey int64  `json:"n_nationkey"`
	NName      string `json:"n_name"`
	NRegionkey int64  `json:"n_regionkey"` // foreign key to RRegionkey
	NComment   string `json:"n_comment"`
}

type NationGenerator struct {
	distManager *DistributionManager
	textPool    *TextPool
	iter        *NationGeneratorIter
}

func NewNationGenerator() *NationGenerator {
	n := &NationGenerator{
		GetDistributionManager(),
		GetTextPool(),
		nil,
	}
	dist, err := n.distManager.GetDistribution("nations")
	if err != nil {
		util.LogErr(err.Error())
		return nil
	}
	n.iter = NewNationGeneratorIter(dist, n.textPool)
	return n
}

func (n *NationGenerator) Next() []byte {
	item := n.iter.Next()
	bytes, _ := json.Marshal(item)
	return bytes
}

func (n *NationGenerator) Capacity() int64 {
	return int64(n.iter.nations.Size())
}

type NationGeneratorIter struct {
	nations       *Distribution
	commentRandom *RandomText
	idx           int
}

func NewNationGeneratorIter(nations *Distribution, pool *TextPool) *NationGeneratorIter {
	return &NationGeneratorIter{
		nations,
		NewRandomText(606179079, float64(CommentAverLen), 1, pool),
		0,
	}
}

func (n *NationGeneratorIter) Next() *Nation {
	if n.idx >= n.nations.Size() {
		return nil
	}

	nation := new(Nation)
	nation.RowId = int64(n.idx)
	nation.NNationkey = int64(n.idx)
	nation.NName = n.nations.Terms[n.idx]
	nation.NRegionkey = int64(n.nations.Weights[n.idx])
	comment, err := n.commentRandom.NextValue()
	if err != nil {
		comment = ""
	}
	nation.NComment = comment

	n.commentRandom.FinishRow()
	n.idx++

	return nation
}
