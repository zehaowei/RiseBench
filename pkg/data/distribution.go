package data

import (
	"bufio"
	"github.com/singularity-data/tpch-bench/pkg/configs"
	"github.com/singularity-data/tpch-bench/pkg/util"
	"strconv"
	"strings"
	"sync"
)

type Distribution struct {
	Name      string
	Terms     []string
	Weights   []int
	inner     []string
	WeightSum int
}

func (d *Distribution) RandomValue(randomInt *RandomInt) (string, error) {
	if d.inner != nil {
		idx, err := randomInt.NextInt(0, d.WeightSum-1)
		if err != nil {
			return "", err
		}
		return d.inner[idx], nil
	}
	return "", util.Errorf("%s is not loaded or not a distribution", d.Name)
}

func (d *Distribution) Size() int {
	return len(d.Terms)
}

// DistributionManager
// singleton
type DistributionManager struct {
	distributions map[string]*Distribution
}

var onceDistributionManager sync.Once
var distributionManagerSingleton *DistributionManager

func GetDistributionManager() *DistributionManager {
	onceDistributionManager.Do(func() {
		var err error
		distributionManagerSingleton, err = LoadDistributions(configs.TpchDistributionPath)
		if err != nil {
			util.LogErr(err.Error())
		}
	})
	return distributionManagerSingleton
}

func LoadDistributions(path string) (*DistributionManager, error) {
	d := new(DistributionManager)
	d.distributions = make(map[string]*Distribution, 22)
	scanner := util.ReadFile(path)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if len(line) == 0 || line[0] == '#' {
			continue
		}
		words := strings.SplitN(line, " ", 2)
		if words[0] == "begin" || words[0] == "BEGIN" {
			scanner.Scan() // ignore the "count" line
			dis, err := parseDistribution(words[1], scanner)
			if err != nil {
				return nil, err
			}
			d.distributions[dis.Name] = dis
		} else {
			return nil, util.Errorf("distribution file parse error, expected: BEGIN, found: %s", words[0])
		}
	}
	return d, nil
}

func parseDistribution(name string, scanner *bufio.Scanner) (*Distribution, error) {
	terms := make([]string, 0)
	weights := make([]int, 0)
	weightSum := 0
	isValid := true
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		words := strings.SplitN(line, " ", 2)
		if words[0] == "end" || words[0] == "END" {
			if words[1] != name {
				return nil, util.Errorf("distribution file parse error, expected: %s, found: %s", name, words[1])
			}
			break
		}
		words = strings.Split(line, "|")
		if len(words) != 2 {
			return nil, util.Errorf("distribution file parse error, found: %s", line)
		}
		terms = append(terms, words[0])
		weight, err := strconv.Atoi(words[1])
		if err != nil {
			return nil, util.Errorf("distribution file parse error, %s", err.Error())
		}
		if weight <= 0 {
			isValid = false
		}
		weightSum += weight
		weights = append(weights, weight)
	}
	if !isValid {
		for i := 1; i < len(weights); i++ {
			weights[i] += weights[i-1]
		}
		return &Distribution{
			name,
			terms,
			weights,
			nil,
			-1,
		}, nil
	}

	idx := 0
	inner := make([]string, weightSum)
	for i, term := range terms {
		cnt := weights[i]
		for j := 0; j < cnt; j++ {
			inner[idx] = term
			idx++
		}
	}
	for i := 1; i < len(weights); i++ {
		weights[i] += weights[i-1]
	}

	return &Distribution{
		name,
		terms,
		weights,
		inner,
		weightSum,
	}, nil
}

func (d *DistributionManager) GetDistribution(name string) (*Distribution, error) {
	re, exist := d.distributions[name]
	if exist == false {
		return nil, util.Errorf("%s does not exist", name)
	}
	return re, nil
}
