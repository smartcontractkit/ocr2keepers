package chain

import (
	"context"
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/smartcontractkit/ocr2keepers/pkg/chain/gethwrappers/keeper_registry_wrapper2_0"
	"github.com/smartcontractkit/ocr2keepers/pkg/types"
)

const (
	ActiveUpkeepIDBatchSize int64  = 10000
	separator               string = "|"
)

var (
	ErrRegistryCallFailure   = fmt.Errorf("registry chain call failure")
	ErrBlockKeyNotParsable   = fmt.Errorf("block identifier not parsable")
	ErrUpkeepKeyNotParsable  = fmt.Errorf("upkeep key not parsable")
	ErrInitializationFailure = fmt.Errorf("failed to initialize registry")
	ErrContextCancelled      = fmt.Errorf("context was cancelled")

	keeperRegistryABI = mustGetABI(keeper_registry_wrapper2_0.KeeperRegistryABI)
)

type outStruct struct {
	ur  []types.UpkeepResult
	err error
}

// evmRegistryv2_0 implements types.Registry interface
type evmRegistryv2_0 struct {
	registry *keeper_registry_wrapper2_0.KeeperRegistryCaller
	client   EVMClient
}

// NewEVMRegistryV2_0 is the constructor of evmRegistryv2_0
func NewEVMRegistryV2_0(address common.Address, client EVMClient) (*evmRegistryv2_0, error) {
	registry, err := keeper_registry_wrapper2_0.NewKeeperRegistryCaller(address, client)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to create caller for address and backend", ErrInitializationFailure)
	}

	return &evmRegistryv2_0{
		registry: registry,
		client:   client,
	}, nil
}

func (r *evmRegistryv2_0) GetActiveUpkeepKeys(ctx context.Context, block types.BlockKey) ([]types.UpkeepKey, error) {
	opts, err := r.buildCallOpts(ctx, block)
	if err != nil {
		return nil, err
	}

	state, err := r.registry.GetState(opts)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get contract state at block number %d", opts.BlockNumber.Int64())
	}

	keys := make([]types.UpkeepKey, 0)
	for int64(len(keys)) < state.State.NumUpkeeps.Int64() {
		startIndex := int64(len(keys))
		maxCount := state.State.NumUpkeeps.Int64() - int64(len(keys))

		if maxCount > ActiveUpkeepIDBatchSize {
			maxCount = ActiveUpkeepIDBatchSize
		}

		nextRawKeys, err := r.registry.GetActiveUpkeepIDs(opts, big.NewInt(startIndex), big.NewInt(maxCount))
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get active upkeep IDs from index %d to %d (both inclusive)", startIndex, startIndex+maxCount-1)
		}

		nextKeys := make([]types.UpkeepKey, len(nextRawKeys))
		for i, next := range nextRawKeys {
			nextKeys[i] = BlockAndIdToKey(opts.BlockNumber, next)
		}

		if len(nextKeys) == 0 {
			break
		}

		buffer := make([]types.UpkeepKey, len(keys), len(keys)+len(nextKeys))
		copy(keys, buffer)

		keys = append(buffer, nextKeys...)
	}

	return keys, nil
}

func (r *evmRegistryv2_0) check(ctx context.Context, keys []types.UpkeepKey, ch chan outStruct) {
	var (
		checkReqs    = make([]rpc.BatchElem, len(keys))
		checkResults = make([]*string, len(keys))
	)

	for i, key := range keys {
		block, upkeepId, err := BlockAndIdFromKey(key)
		if err != nil {
			ch <- outStruct{
				err: err,
			}
			return
		}

		opts, err := r.buildCallOpts(ctx, block)
		if err != nil {
			ch <- outStruct{
				err: err,
			}
			return
		}

		payload, err := keeperRegistryABI.Pack("checkUpkeep", upkeepId)
		if err != nil {
			ch <- outStruct{
				err: err,
			}
			return
		}

		var result string
		checkReqs[i] = rpc.BatchElem{
			Method: "eth_call",
			Args: []interface{}{
				map[string]interface{}{
					"to":   r.registry,
					"data": hexutil.Bytes(payload),
				},
				hexutil.EncodeBig(opts.BlockNumber),
			},
			Result: &result,
		}

		checkResults[i] = &result
	}

	if err := r.client.BatchCallContext(ctx, checkReqs); err != nil {
		ch <- outStruct{
			err: err,
		}
		return
	}

	var (
		err            error
		performReqs    = make([]rpc.BatchElem, len(keys))
		performResults = make([]*string, len(keys))
		results        = make([]types.UpkeepResult, len(keys))
	)

	for i, req := range checkReqs {
		if req.Error != nil {
			if strings.Contains(req.Error.Error(), "reverted") {
				// subscription was canceled
				// NOTE: would we want to publish the fact that it is inactive?
				continue
			}
			// some other error
			multierr.AppendInto(&err, req.Error)
		} else {
			results[i], err = unmarshalCheckUpkeepResult(keys[i], *checkResults[i])
			if err != nil {
				ch <- outStruct{
					err: err,
				}
				return
			}

			block, upkeepId, err := BlockAndIdFromKey(keys[i])
			if err != nil {
				ch <- outStruct{
					err: err,
				}
				return
			}

			opts, err := r.buildCallOpts(ctx, block)
			if err != nil {
				ch <- outStruct{
					err: err,
				}
				return
			}

			// Since checkUpkeep is true, simulate the perform upkeep to ensure it doesn't revert
			payload, err := keeperRegistryABI.Pack("simulatePerformUpkeep", upkeepId, results[i].PerformData)
			if err != nil {
				ch <- outStruct{
					err: err,
				}
				return
			}

			var result string
			performReqs[i] = rpc.BatchElem{
				Method: "eth_call",
				Args: []interface{}{
					map[string]interface{}{
						"to":   r.registry,
						"data": hexutil.Bytes(payload),
					},
					hexutil.EncodeBig(opts.BlockNumber),
				},
				Result: &result,
			}

			performResults[i] = &result
		}
	}

	if err != nil {
		ch <- outStruct{
			err: err,
		}
		return
	}

	if err = r.client.BatchCallContext(ctx, performReqs); err != nil {
		ch <- outStruct{
			err: fmt.Errorf("%w: simulate perform upkeep returned result: %s", ErrRegistryCallFailure, err),
		}
		return
	}

	out := outStruct{
		ur: make([]types.UpkeepResult, len(keys)),
	}

	for i, req := range performReqs {
		if req.Error != nil {
			if strings.Contains(req.Error.Error(), "reverted") {
				// subscription was canceled
				// NOTE: would we want to publish the fact that it is inactive?
				continue
			}
			// some other error
			multierr.AppendInto(&err, req.Error)
		} else {
			simulatePerformSuccess, err := unmarshalPerformUpkeepSimulationResult(*performResults[i])
			if err != nil {
				ch <- outStruct{
					err: err,
				}
				return
			}

			if !simulatePerformSuccess {
				results[i].State = types.NotEligible
			}

			out.ur[i] = results[i]
		}
	}

	if err != nil {
		ch <- outStruct{
			err: err,
		}
		return
	}

	ch <- out
}

func (r *evmRegistryv2_0) CheckUpkeep(ctx context.Context, keys ...types.UpkeepKey) (types.UpkeepResults, error) {
	chResult := make(chan outStruct, 1)
	go r.check(ctx, keys, chResult)

	select {
	case rs := <-chResult:
		return rs.ur, rs.err
	case <-ctx.Done():
		// safety on context done to provide an error on context cancellation
		// contract calls through the geth wrappers are a bit of a black box
		// so this safety net ensures contexts are fully respected and contract
		// call functions have a more graceful closure outside the scope of
		// CheckUpkeep needing to return immediately.
		return nil, fmt.Errorf("%w: failed to check upkeep on registry", ErrContextCancelled)
	}
}

func (r *evmRegistryv2_0) IdentifierFromKey(key types.UpkeepKey) (types.UpkeepIdentifier, error) {
	_, id, err := BlockAndIdFromKey(key)
	if err != nil {
		return nil, err
	}

	return id.Bytes(), nil
}

func (r *evmRegistryv2_0) buildCallOpts(ctx context.Context, block types.BlockKey) (*bind.CallOpts, error) {
	b := new(big.Int)
	_, ok := b.SetString(string(block), 10)

	if !ok {
		return nil, fmt.Errorf("%w: requires big int", ErrBlockKeyNotParsable)
	}

	if b == nil || b.Int64() == 0 {
		// fetch the current block number so batched GetActiveUpkeepKeys calls can be performed on the same block
		header, err := r.client.HeaderByNumber(ctx, nil)
		if err != nil {
			return nil, fmt.Errorf("%w: %s: EVM failed to fetch block header", err, ErrRegistryCallFailure)
		}

		b = header.Number
	}

	return &bind.CallOpts{
		Context:     ctx,
		BlockNumber: b,
	}, nil
}

func BlockAndIdFromKey(key types.UpkeepKey) (types.BlockKey, *big.Int, error) {
	parts := strings.Split(string(key), separator)
	if len(parts) != 2 {
		return "", nil, fmt.Errorf("%w: missing data in upkeep key", ErrUpkeepKeyNotParsable)
	}

	id := new(big.Int)
	_, ok := id.SetString(parts[1], 10)
	if !ok {
		return "", nil, fmt.Errorf("%w: must be big int", ErrUpkeepKeyNotParsable)
	}

	return types.BlockKey(parts[0]), id, nil
}

func BlockAndIdToKey(block *big.Int, id *big.Int) types.UpkeepKey {
	return types.UpkeepKey(fmt.Sprintf("%s%s%s", block, separator, id))
}
