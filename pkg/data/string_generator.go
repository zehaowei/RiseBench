package data

import (
	"github.com/singularity-data/tpch-bench/pkg/util"
	"math"
	"strings"
	"sync"
)

type StringBaseGenerator struct {
	randomInt *RandomInt
	dist      *Distribution
}

func NewStringBaseGenerator(seed int64, distribution *Distribution, expectedRowCnt int) *StringBaseGenerator {
	return &StringBaseGenerator{
		NewRandomInt(seed, expectedRowCnt),
		distribution,
	}
}

func (s *StringBaseGenerator) NextValue() (string, error) {
	re, err := s.dist.RandomValue(s.randomInt)
	if err != nil {
		return "", err
	}
	return re, nil
}

func (s *StringBaseGenerator) FinishRow() {
	s.randomInt.FinishRow()
}

func (s *StringBaseGenerator) AdvanceRows(cnt int64) {
	s.randomInt.advanceSeed(cnt)
}

type RandomString struct {
	*StringBaseGenerator
}

func NewRandomString(seed int64, dist *Distribution, expectedRowCnt int) *RandomString {
	return &RandomString{
		NewStringBaseGenerator(seed, dist, expectedRowCnt),
	}
}

type RandomStringSequence struct {
	cnt int
	*RandomInt
	dist *Distribution
}

func NewRandomStringSequence(seed int64, cnt int, dist *Distribution, expectedRowCnt int) *RandomStringSequence {
	return &RandomStringSequence{
		cnt,
		NewRandomInt(seed, len(dist.Terms)*expectedRowCnt),
		dist,
	}
}

func (r *RandomStringSequence) NextValue() (string, error) {
	terms := make([]string, len(r.dist.Terms))
	copy(terms, r.dist.Terms)
	if r.cnt >= len(terms) {
		return "", util.Errorf("Count should be less than [Distribution:terms] size")
	}
	for i := 0; i < r.cnt; i++ {
		j, _ := r.NextInt(i, len(terms)-1)
		tmp := terms[i]
		terms[i] = terms[j]
		terms[j] = tmp
	}
	return strings.Join(terms[0:r.cnt], " "), nil
}

type RandomText struct {
	*RandomInt
	pool      *TextPool
	minLength int
	maxLength int
}

func NewRandomText(seed int64, averageTextLength float64, expectedRowCnt int, pool *TextPool) *RandomText {
	return &RandomText{
		NewRandomInt(seed, expectedRowCnt*2),
		pool,
		int(averageTextLength * LowLengthMultiplier),
		int(averageTextLength * HighLengthMultiplier),
	}
}

func (r *RandomText) NextValue() (string, error) {
	offset, err := r.NextInt(0, r.pool.size-r.maxLength)
	if err != nil {
		return "", nil
	}
	length, err := r.NextInt(r.minLength, r.maxLength)
	if err != nil {
		return "", nil
	}
	return r.pool.GetText(offset, offset+length), nil
}

type BytesBuilder struct {
	size  int
	bytes []byte
}

func NewBytesBuilder(size int) *BytesBuilder {
	bytes := make([]byte, size)
	return &BytesBuilder{
		0,
		bytes,
	}
}

func (b *BytesBuilder) Append(s string) {
	str := []byte(s)
	for _, bt := range str {
		b.bytes[b.size] = bt
		b.size++
	}
}

func (b *BytesBuilder) Erase(cnt int) {
	if cnt > b.size {
		cnt = b.size
	}
	b.size -= cnt
}

func (b *BytesBuilder) GetSize() int {
	return b.size
}

func (b *BytesBuilder) GetBytes() []byte {
	return b.bytes
}

func (b *BytesBuilder) GetLast() byte {
	return b.bytes[b.size-1]
}

var onceTextPool sync.Once
var textPoolSingleton *TextPool

const (
	DefaultTextPoolSize int = 300 * 1024 * 1024
	MaxSentenceLength   int = 256
)

type TextPool struct {
	inner []byte
	size  int
}

func GetTextPool() *TextPool {
	onceTextPool.Do(func() {
		randomInt := NewRandomInt(933588178, math.MaxInt32)
		distManager := GetDistributionManager()
		buffer := NewBytesBuilder(DefaultTextPoolSize + MaxSentenceLength)
		for buffer.GetSize() < DefaultTextPoolSize {
			err := generateSentence(distManager, randomInt, buffer)
			if err != nil {
				util.LogErr(err.Error())
			}
		}
		buffer.Erase(buffer.GetSize() - DefaultTextPoolSize)
		textPoolSingleton = &TextPool{
			buffer.GetBytes(),
			buffer.GetSize(),
		}
	})
	return textPoolSingleton
}

func (t *TextPool) GetSize() int {
	return t.size
}

func (t *TextPool) GetText(begin int, end int) string {
	if end > t.size {
		end = t.size
	}
	return string(t.inner[begin:end])
}

func generateSentence(distManager *DistributionManager, randomInt *RandomInt, buffer *BytesBuilder) error {
	dist, err := distManager.GetDistribution("grammar")
	if err != nil {
		return err
	}
	s, err := dist.RandomValue(randomInt)
	if err != nil {
		return err
	}
	syntax := []byte(s)
	maxLen := len(syntax)
	for i := 0; i < maxLen; i += 2 {
		switch syntax[i] {
		case 'V':
			err = generateVerb(distManager, randomInt, buffer)
			if err != nil {
				return err
			}
		case 'N':
			err = generateNoun(distManager, randomInt, buffer)
			if err != nil {
				return err
			}
		case 'P':
			d, err := distManager.GetDistribution("prepositions")
			if err != nil {
				return err
			}
			prep, err := d.RandomValue(randomInt)
			if err != nil {
				return err
			}
			buffer.Append(prep)
			buffer.Append(" the ")
			err = generateNoun(distManager, randomInt, buffer)
			if err != nil {
				return err
			}
		case 'T':
			buffer.Erase(1)
			d, err := distManager.GetDistribution("terminators")
			if err != nil {
				return err
			}
			termi, err := d.RandomValue(randomInt)
			if err != nil {
				return err
			}
			buffer.Append(termi)
		default:
			return util.Errorf("Unknown word in grammar syntax: %s", syntax[i])
		}
		if buffer.GetLast() != ' ' {
			buffer.Append(" ")
		}
	}
	return nil
}

func generateVerb(distManager *DistributionManager, randomInt *RandomInt, buffer *BytesBuilder) error {
	dist, err := distManager.GetDistribution("vp")
	if err != nil {
		return err
	}
	s, err := dist.RandomValue(randomInt)
	if err != nil {
		return err
	}
	syntax := []byte(s)
	maxLen := len(syntax)
	for i := 0; i < maxLen; i += 2 {
		var source *Distribution
		switch syntax[i] {
		case 'D':
			source, err = distManager.GetDistribution("adverbs")
		case 'V':
			source, err = distManager.GetDistribution("verbs")
		case 'X':
			source, err = distManager.GetDistribution("auxillaries")
		default:
			return util.Errorf("Unknown word in vp syntax: %s", syntax[i])
		}
		if err != nil {
			return err
		}

		word, err := source.RandomValue(randomInt)
		if err != nil {
			return err
		}
		buffer.Append(word)
		buffer.Append(" ")
	}
	return nil
}

func generateNoun(distManager *DistributionManager, randomInt *RandomInt, buffer *BytesBuilder) error {
	dist, err := distManager.GetDistribution("np")
	if err != nil {
		return err
	}
	s, err := dist.RandomValue(randomInt)
	if err != nil {
		return err
	}
	syntax := []byte(s)
	maxLen := len(syntax)
	for i := 0; i < maxLen; i++ {
		var source *Distribution
		switch syntax[i] {
		case 'A':
			source, err = distManager.GetDistribution("articles")
		case 'J':
			source, err = distManager.GetDistribution("adjectives")
		case 'D':
			source, err = distManager.GetDistribution("adverbs")
		case 'N':
			source, err = distManager.GetDistribution("nouns")
		case ',':
			buffer.Erase(1)
			buffer.Append(", ")
			continue
		case ' ':
			continue
		default:
			return util.Errorf("Unknown word in np syntax: %s", syntax[i])
		}
		if err != nil {
			return err
		}

		word, err := source.RandomValue(randomInt)
		if err != nil {
			return err
		}
		buffer.Append(word)
		buffer.Append(" ")
	}
	return nil
}
