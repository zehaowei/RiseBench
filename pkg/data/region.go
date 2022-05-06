package data

import (
	"encoding/json"
	"github.com/singularity-data/tpch-bench/pkg/util"
)

type Region struct {
	RowId      int64  `json:"-"`
	RRegionkey int64  `json:"r_regionkey"`
	RName      string `json:"r_name"`
	RComment   string `json:"r_comment"`
}

const (
	CommentAverLen int = 72
)

type RegionGenerator struct {
	distManager *DistributionManager
	textPool    *TextPool
	iter        *RegionGeneratorIter
}

func NewRegionGenerator() *RegionGenerator {
	r := &RegionGenerator{
		GetDistributionManager(),
		GetTextPool(),
		nil,
	}
	dist, err := r.distManager.GetDistribution("regions")
	if err != nil {
		util.LogErr(err.Error())
		return nil
	}
	r.iter = NewRegionGeneratorIter(dist, r.textPool)
	return r
}

func (r *RegionGenerator) Next() []byte {
	item := r.iter.Next()
	bytes, _ := json.Marshal(item)
	return bytes
}

func (r *RegionGenerator) Capacity() int64 {
	return int64(r.iter.regions.Size())
}

type RegionGeneratorIter struct {
	regions       *Distribution
	commentRandom *RandomText
	idx           int
}

func NewRegionGeneratorIter(regions *Distribution, pool *TextPool) *RegionGeneratorIter {
	return &RegionGeneratorIter{
		regions,
		NewRandomText(1500869201, float64(CommentAverLen), 1, pool),
		0,
	}
}

func (r *RegionGeneratorIter) Next() *Region {
	if r.idx >= r.regions.Size() {
		return nil
	}

	region := new(Region)
	region.RowId = int64(r.idx)
	region.RRegionkey = int64(r.idx)
	region.RName = r.regions.Terms[r.idx]
	comment, err := r.commentRandom.NextValue()
	if err != nil {
		comment = ""
	}
	region.RComment = comment

	r.commentRandom.FinishRow()
	r.idx++

	return region
}
