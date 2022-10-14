package keepers

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"math"
	"math/cmplx"
	rnd "math/rand"
	"sort"
	"sync"

	"github.com/smartcontractkit/libocr/offchainreporting2/types"
	ktypes "github.com/smartcontractkit/ocr2keepers/pkg/types"
)

var (
	ErrEncoding        = fmt.Errorf("error encountered while encoding")
	ErrNotEnoughInputs = fmt.Errorf("not enough inputs")
)

type cryptoRandSource struct{}

func newCryptoRandSource() cryptoRandSource {
	return cryptoRandSource{}
}

func (_ cryptoRandSource) Int63() int64 {
	var b [8]byte
	_, err := rand.Read(b[:])
	if err != nil {
		panic(err)
	}
	return int64(binary.LittleEndian.Uint64(b[:]) & (1<<63 - 1))
}

func (_ cryptoRandSource) Seed(_ int64) {}

func filterUpkeeps(upkeeps []*ktypes.UpkeepResult, filter ktypes.UpkeepState) []*ktypes.UpkeepResult {
	ret := make([]*ktypes.UpkeepResult, 0, len(upkeeps))

	for _, up := range upkeeps {
		if up.State == filter {
			ret = append(ret, up)
		}
	}

	return ret
}

func keyList(upkeeps []*ktypes.UpkeepResult) []ktypes.UpkeepKey {
	ret := make([]ktypes.UpkeepKey, len(upkeeps))

	for i, up := range upkeeps {
		ret[i] = up.Key
	}

	return ret
}

type shuffler[T any] interface {
	Shuffle([]T) []T
}

type cryptoShuffler[T any] struct{}

func (_ *cryptoShuffler[T]) Shuffle(a []T) []T {
	r := rnd.New(newCryptoRandSource())
	r.Shuffle(len(a), func(i, j int) {
		a[i], a[j] = a[j], a[i]
	})
	return a
}

type sortUpkeepKeys []ktypes.UpkeepKey

func (s sortUpkeepKeys) Less(i, j int) bool {
	return string([]byte(s[i])) < string([]byte(s[j]))
}

func (s sortUpkeepKeys) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s sortUpkeepKeys) Len() int {
	return len(s)
}

func dedupe[T any](inputs [][]T, filters ...func(T) bool) ([]T, error) {
	if len(inputs) == 0 {
		return nil, fmt.Errorf("%w: must provide at least 1", ErrNotEnoughInputs)
	}

	if len(inputs) == 1 {
		return inputs[0], nil
	}

	var max int
	for _, input := range inputs {
		max += len(input)
	}

	output := make([]T, 0, max)
	matched := make(map[string]bool)
	for _, input := range inputs {
		for _, val := range input {
			add := true
			for _, filter := range filters {
				if !filter(val) {
					add = false
					break
				}
			}

			if !add {
				continue
			}

			key := fmt.Sprintf("%v", val)
			_, ok := matched[key]
			if !ok {
				matched[key] = true
				output = append(output, val)
			}
		}
	}

	return output, nil
}

func sortedDedupedKeyList(attributed []types.AttributedObservation, filters ...func(ktypes.UpkeepKey) bool) ([]ktypes.UpkeepKey, error) {
	var err error

	kys := make([][]ktypes.UpkeepKey, len(attributed))
	for i, attr := range attributed {
		b := []byte(attr.Observation)
		if len(b) == 0 {
			continue
		}

		var ob []ktypes.UpkeepKey
		err = decode(b, &ob)
		if err != nil {
			return nil, fmt.Errorf("%w: cannot prepare sorted key list; observation not properly encoded", err)
		}

		sort.Sort(sortUpkeepKeys(ob))
		kys[i] = ob
	}
	fmt.Println("DEBUGGG: within sortedDeduped")
	for _, k := range kys {
		fmt.Printf("DEBUGGG: within sortedDeduped %s", k)
	}
	keys, err := dedupe(kys, filters...)
	fmt.Println("DEBUGGG: within sortedDeduped after filter")
	for _, k := range keys {
		fmt.Printf("DEBUGGG: within sortedDeduped after filter %s", k)
	}
	if err != nil {
		return nil, fmt.Errorf("%w: observation dedupe", err)
	}

	sort.Sort(sortUpkeepKeys(keys))

	return keys, nil
}

func sampleFromProbability(rounds, nodes int, probability float32) (sampleRatio, error) {
	var ratio sampleRatio

	if rounds <= 0 {
		return ratio, fmt.Errorf("number of rounds must be greater than 0")
	}

	if nodes <= 0 {
		return ratio, fmt.Errorf("number of nodes must be greater than 0")
	}

	if probability > 1 || probability <= 0 {
		return ratio, fmt.Errorf("probability must be less than 1 and greater than 0")
	}

	var r complex128 = complex(float64(rounds), 0)
	var n complex128 = complex(float64(nodes), 0)
	var p complex128 = complex(float64(probability), 0)

	g := -1.0 * (p - 1.0)
	x := cmplx.Pow(cmplx.Pow(g, 1.0/r), 1.0/n)
	rat := cmplx.Abs(-1.0 * (x - 1.0))
	rat = math.Round(rat/0.01) * 0.01
	ratio = sampleRatio(float32(rat))

	return ratio, nil
}

func lowest(values []int64) int64 {
	if len(values) == 0 {
		return 0
	}

	sort.Slice(values, func(i, j int) bool {
		return values[i] < values[j]
	})

	return values[0]
}

type syncedArray[T any] struct {
	data []T
	mu   sync.RWMutex
}

func newSyncedArray[T any]() *syncedArray[T] {
	return &syncedArray[T]{
		data: []T{},
	}
}

func (a *syncedArray[T]) Append(vals ...T) *syncedArray[T] {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.data = append(a.data, vals...)
	return a
}

func (a *syncedArray[T]) Values() []T {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.data
}
