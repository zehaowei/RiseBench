package data

import (
	"fmt"
	"github.com/singularity-data/tpch-bench/pkg/util"
	"math"
)

const (
	Multiplier int64 = 16807
	Mod        int64 = math.MaxInt32
)

type IntBaseGenerator struct {
	usageTimesPerRow int
	usage            int
	seed             int64
}

func NewIntBaseGenerator(seed int64, usageTimesPerRow int) *IntBaseGenerator {
	return &IntBaseGenerator{
		usageTimesPerRow,
		0,
		seed,
	}
}

func (i *IntBaseGenerator) NextInt(low int, high int) (int, error) {
	if i.usage > i.usageTimesPerRow {
		return 0, util.Errorf("IntGenerator can only be used %d times per row", i.usageTimesPerRow)
	}
	i.seed = (i.seed * Multiplier) % Mod
	i.usage++
	interval := high - low + 1
	offset := int(float64(i.seed) / float64(Mod) * float64(interval))
	return low + offset, nil
}

func (i *IntBaseGenerator) FinishRow() {
	i.advanceSeed(int64(i.usageTimesPerRow - i.usage))
	i.usage = 0
}

func (i *IntBaseGenerator) AdvanceRows(cnt int64) {
	if i.usage != 0 {
		i.FinishRow()
	}
	i.advanceSeed(int64(i.usageTimesPerRow) * cnt)
}

func (i *IntBaseGenerator) advanceSeed(cnt int64) {
	var multiplier = Multiplier
	for cnt > 0 {
		if cnt%2 != 0 {
			i.seed = (multiplier * i.seed) % Mod
		}
		cnt /= 2
		multiplier = (multiplier * multiplier) % Mod
	}
}

type RandomInt struct {
	*IntBaseGenerator
}

func NewRandomInt(seed int64, usageTimesPerRow int) *RandomInt {
	return &RandomInt{NewIntBaseGenerator(seed, usageTimesPerRow)}
}

type BoundedRandomInt struct {
	*IntBaseGenerator
	low  int
	high int
}

func NewBoundedRandomInt(seed int64, usageTimesPerRow int, low int, high int) *BoundedRandomInt {
	return &BoundedRandomInt{
		NewIntBaseGenerator(seed, usageTimesPerRow),
		low,
		high,
	}
}

func (b *BoundedRandomInt) NextValue() (int, error) {
	return b.NextInt(b.low, b.high)
}

const (
	AlphaNumericBase     string  = "0123456789abcdefghijklmnopqrstuvwxyz ABCDEFGHIJKLMNOPQRSTUVWXYZ,"
	LowLengthMultiplier  float64 = 0.4
	HighLengthMultiplier float64 = 1.6
	UsagePerRow          int     = 9
)

type RandomAlphaNumeric struct {
	*IntBaseGenerator
	alphaNumeric []byte
	minLength    int
	maxLength    int
}

func NewRandomAlphaNumeric(seed int64, averageLength int, expectRowCnt int) *RandomAlphaNumeric {
	return &RandomAlphaNumeric{
		NewIntBaseGenerator(seed, UsagePerRow*expectRowCnt),
		[]byte(AlphaNumericBase),
		int(float64(averageLength) * LowLengthMultiplier),
		int(float64(averageLength) * HighLengthMultiplier),
	}
}

func (r *RandomAlphaNumeric) NextValue() (string, error) {
	length, err := r.NextInt(r.minLength, r.maxLength)
	if err != nil {
		return "", err
	}
	buffer := make([]byte, length)
	var idx int64 = 0
	for i := 0; i < len(buffer); i++ {
		if i%5 == 0 {
			tmp, err := r.NextInt(0, math.MaxInt32)
			if err != nil {
				return "", err
			}
			idx = int64(tmp)
		}
		buffer[i] = r.alphaNumeric[idx&0x3f]
		idx >>= 6
	}
	return string(buffer[:]), nil
}

const NationsMax = 90

type RandomPhoneNumber struct {
	*IntBaseGenerator
}

func NewRandomPhoneNumber(seed int64, expectRowCnt int) *RandomPhoneNumber {
	return &RandomPhoneNumber{
		NewIntBaseGenerator(seed, 3*expectRowCnt),
	}
}

func (r *RandomPhoneNumber) NextValue(nationKey int64) (string, error) {
	part1, err := r.NextInt(100, 999)
	if err != nil {
		return "", nil
	}
	part2, err := r.NextInt(100, 999)
	if err != nil {
		return "", nil
	}
	part3, err := r.NextInt(1000, 9999)
	return fmt.Sprintf("%02d-%03d-%03d-%04d",
		10+(nationKey%NationsMax), part1, part2, part3), nil
}
