package test

import (
	"fmt"
	"github.com/singularity-data/tpch-bench/pkg/data"
	"testing"
)

func TestDistributionManager(t *testing.T) {
	d, err := data.LoadDistributions("../assets/data/dists.dss")
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	names := []string{
		"category",
		"grammar",
		"np",
		"vp",
		"container",
		"instruct",
		"msegmnt",
		"names",
		"nations",
		"nations2",
		"o_prio",
		"regions",
		"rflag",
		"types",
		"colors",
		"articles",
		"nouns",
		"verbs",
		"adverbs",
		"auxillaries",
		"prepositions",
		"terminators",
		"Q13a",
		"Q13b",
		"adjectives",
		"p_cntr",
		"p_names",
	}
	for _, name := range names {
		dis, _ := d.GetDistribution(name)
		if dis != nil {
			fmt.Printf("distribution name:%s\n", dis.Name)
			fmt.Printf("len:%d, weight sum:%d\n", len(dis.Terms), dis.WeightSum)
		}
	}
}

func TestBasicGenerator(t *testing.T) {
	random := data.NewBoundedRandomInt(121321321, 10, 1, 100)
	for i := 0; i < 10; i++ {
		fmt.Println(random.NextValue())
	}
}

func TestTableGenerator(t *testing.T) {
	gen := data.NewTableGeneratorDefault(0.001)
	for i := 0; i < 10; i++ {
		var it data.JsonIterable
		it = gen.LineItemGen[0]
		fmt.Println(string(it.Next()))
	}
}
