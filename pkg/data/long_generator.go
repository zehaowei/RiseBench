package data

import (
	"github.com/singularity-data/tpch-bench/pkg/util"
	"math"
)

const (
	MultiplierLong int64 = 6364136223846793005
	Increment      int64 = 1
)

type LongBaseGenerator struct {
	usageTimesPerRow int
	usage            int
	seed             int64
}

func NewLongBaseGenerator(seed int64, usageTimesPerRow int) *LongBaseGenerator {
	return &LongBaseGenerator{
		usageTimesPerRow,
		0,
		seed,
	}
}

func (l *LongBaseGenerator) NextLong(low int64, high int64) (int64, error) {
	if l.usage > l.usageTimesPerRow {
		return 0, util.Errorf("IntGenerator can only be used %d times per row", l.usageTimesPerRow)
	}
	l.seed = (l.seed * Multiplier) + Increment
	l.usage++
	interval := int64(math.Abs(float64(l.seed))) % (high - low + 1)
	return low + interval, nil
}

func (l *LongBaseGenerator) FinishRow() {
	l.advanceSeed(int64(l.usageTimesPerRow - l.usage))
	l.usage = 0
}

func (l *LongBaseGenerator) AdvanceRows(cnt int64) {
	if l.usage != 0 {
		l.FinishRow()
	}
	l.advanceSeed(int64(l.usageTimesPerRow) * cnt)
}

func (l *LongBaseGenerator) advanceSeed(cnt int64) {
	var multiplier = Multiplier
	for cnt > 0 {
		if cnt%2 != 0 {
			l.seed = (multiplier * l.seed) % Mod
		}
		cnt /= 2
		multiplier = (multiplier * multiplier) % Mod
	}
}

type RandomLong struct {
	*LongBaseGenerator
}

func NewRandomLong(seed int64, usageTimesPerRow int) *RandomLong {
	return &RandomLong{NewLongBaseGenerator(seed, usageTimesPerRow)}
}

type BoundedRandomLong struct {
	intGenerator  *IntBaseGenerator
	longGenerator *LongBaseGenerator
	low           int64
	high          int64
}

func NewBoundedRandomLong(useLong bool, seed int64, usageTimesPerRow int, low int64, high int64) *BoundedRandomLong {
	b := new(BoundedRandomLong)
	if useLong {
		b.longGenerator = NewLongBaseGenerator(seed, usageTimesPerRow)
		b.intGenerator = nil
	} else {
		b.longGenerator = nil
		b.intGenerator = NewIntBaseGenerator(seed, usageTimesPerRow)
	}
	b.low = low
	b.high = high
	return b
}

func (b *BoundedRandomLong) NextValue() (int64, error) {
	if b.longGenerator != nil {
		return b.longGenerator.NextLong(b.low, b.high)
	}
	re, err := b.intGenerator.NextInt(int(b.low), int(b.high))
	return int64(re), err
}

func (b *BoundedRandomLong) FinishRow() {
	if b.longGenerator != nil {
		b.longGenerator.FinishRow()
	} else {
		b.intGenerator.FinishRow()
	}
}

func (b *BoundedRandomLong) AdvanceRows(cnt int64) {
	if b.longGenerator != nil {
		b.longGenerator.AdvanceRows(cnt)
	} else {
		b.intGenerator.AdvanceRows(cnt)
	}
}
