package simulators

import (
	"context"
	"fmt"
	"math/big"
	"math/rand"
	"strings"
	"time"

	"github.com/smartcontractkit/ocr2keepers/pkg/types"
)

type SimulatedUpkeep struct {
	ID         *big.Int
	EligibleAt []*big.Int
	Performs   map[string]types.PerformLog // performs at block number
}

func (ct *SimulatedContract) GetActiveUpkeepKeys(ctx context.Context, key types.BlockKey) ([]types.UpkeepKey, error) {
	ct.logger.Printf("getting keys at block %s", ct.lastBlock)

	r := rand.Intn(ct.avgLatency)
	timer := time.NewTimer(time.Duration(r*2) * time.Millisecond)

	select {
	case <-ctx.Done():
		timer.Stop()
		return nil, fmt.Errorf("context cancelled")
	case <-timer.C:
	}

	block := ct.lastBlock.String()
	keys := []types.UpkeepKey{}

	// TODO: filter out cancelled upkeeps
	for key := range ct.upkeeps {
		k := types.UpkeepKey([]byte(fmt.Sprintf("%s|%s", block, key)))
		keys = append(keys, k)
	}

	return keys, nil
}

func (ct *SimulatedContract) CheckUpkeep(ctx context.Context, key types.UpkeepKey) (bool, types.UpkeepResult, error) {
	rl := rand.Intn(ct.avgLatency)
	timer := time.NewTimer(time.Duration(rl*2) * time.Millisecond)

	ct.telemetry.CheckKey([]byte(key))

	select {
	case <-ctx.Done():
		timer.Stop()
		return false, types.UpkeepResult{}, fmt.Errorf("context cancelled")
	case <-timer.C:
	}

	parts := strings.Split(string(key), "|")
	if len(parts) != 2 {
		panic("upkeep key does not contain block and id")
	}

	block, ok := new(big.Int).SetString(parts[0], 10)
	if !ok {
		return false, types.UpkeepResult{}, fmt.Errorf("block in key not parsable as big int")
	}

	up, ok := ct.upkeeps[parts[1]]
	if !ok {
		return false, types.UpkeepResult{}, fmt.Errorf("upkeep not registered")
	}

	var bl [32]byte
	r := types.UpkeepResult{
		Key:     key,
		State:   types.NotEligible,
		GasUsed: big.NewInt(0),
		/*
			FailureReason    uint8
		*/
		PerformData:      []byte{}, // TODO: add perform data
		FastGasWei:       big.NewInt(0),
		LinkNative:       big.NewInt(0),
		CheckBlockNumber: uint32(block.Int64() - 1), // minus 1 because the real contract does this
		CheckBlockHash:   bl,
	}

	// start at the highest blocks eligible. the first eligible will be a block
	// lower than the current
	for j := len(up.EligibleAt) - 1; j >= 0; j-- {
		e := up.EligibleAt[j]

		if block.Cmp(e) >= 0 {
			r.State = types.Eligible

			// check that upkeep has not been recently performed between two
			// points of eligibility
			// is there a log between eligible and block
			var i int64
			diff := new(big.Int).Sub(block, e).Int64()
			for i = 0; i <= diff; i++ {
				c := new(big.Int).Add(e, big.NewInt(i))
				_, ok := up.Performs[c.String()]
				if ok {
					r.State = types.NotEligible
					return false, r, nil
				}
			}

			return true, r, nil
		}
	}

	return false, r, nil
}

func (ct *SimulatedContract) IdentifierFromKey(key types.UpkeepKey) (types.UpkeepIdentifier, error) {
	parts := strings.Split(string(key), "|")
	if len(parts) != 2 {
		panic("upkeep key does not contain block and id")
	}

	return types.UpkeepIdentifier(parts[1]), nil
}
