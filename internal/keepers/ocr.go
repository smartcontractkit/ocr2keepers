package keepers

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/smartcontractkit/libocr/offchainreporting2/types"
	ktypes "github.com/smartcontractkit/ocr2keepers/pkg/types"
)

// Query implements the types.ReportingPlugin interface in OCR2. The query produced from this
// method is intended to be empty.
func (k *keepers) Query(_ context.Context, _ types.ReportTimestamp) (types.Query, error) {
	return types.Query{}, nil
}

// Observation implements the types.ReportingPlugin interface in OCR2. This method samples a set
// of upkeeps available in and UpkeepService and produces an observation containing upkeeps that
// need to be executed.
func (k *keepers) Observation(ctx context.Context, rt types.ReportTimestamp, _ types.Query) (types.Observation, error) {
	t := time.Now()

	k.logger.Printf("sampling upkeeps for epoch %d and round %d", rt.Epoch, rt.Round)
	results, err := k.service.SampleUpkeeps(ctx, k.filter.Filter())
	if err != nil {
		return nil, fmt.Errorf("%w: failed to sample upkeeps for observation", err)
	}

	keys := keyList(filterUpkeeps(results, ktypes.Eligible))

	k.logger.Printf("%d eligible keys included in observation for epoch %d and round %d", len(keys), rt.Epoch, rt.Round)

	b, err := encode(keys)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to encode upkeep keys for observation", err)
	}

	k.logger.Printf("observation for epoch %d and round %d took %dms", rt.Epoch, rt.Round, time.Since(t)/time.Millisecond)

	return types.Observation(b), nil
}

// Report implements the types.ReportingPlugin inteface in OC2. This method chooses a single upkeep
// from the provided observations by the earliest block number, checks the upkeep, and builds a
// report. Multiple upkeeps in a single report is supported by how the data is abi encoded, but
// no gas estimations exist yet.
func (k *keepers) Report(ctx context.Context, rt types.ReportTimestamp, _ types.Query, attributed []types.AttributedObservation) (bool, types.Report, error) {
	var err error

	// pass the filter to the dedupe function
	// ensure no locked keys come through
	keys, err := sortedDedupedKeyList(attributed, k.filter.Filter())
	if err != nil {
		return false, nil, fmt.Errorf("%w: failed to sort/dedupe attributed observations", err)
	}

	// select, verify, and build report
	toPerform := make([]ktypes.UpkeepResult, 0, 1)
	for _, key := range keys {

		upkeep, err := k.service.CheckUpkeep(ctx, key)
		if err != nil {
			return false, nil, fmt.Errorf("%w: failed to check upkeep from attributed observation", err)
		}

		if upkeep.State == ktypes.Eligible {
			// only build a report from a single upkeep for now
			k.logger.Printf("reporting %s to be performed for epoch %d and round %d", upkeep.Key, rt.Epoch, rt.Round)
			toPerform = append(toPerform, upkeep)
			break
		}
	}

	// if nothing to report, return false with no error
	if len(toPerform) == 0 {
		return false, nil, nil
	}

	b, err := k.encoder.EncodeReport(toPerform)
	if err != nil {
		return false, nil, fmt.Errorf("%w: failed to encode OCR report", err)
	}

	return true, types.Report(b), err
}

// ShouldAcceptFinalizedReport implements the types.ReportingPlugin interface
// from OCR2. The implementation checks the length of the report and the number
// of keys in the report. Finally it applies a lockout to all keys in the report
func (k *keepers) ShouldAcceptFinalizedReport(ctx context.Context, rt types.ReportTimestamp, r types.Report) (bool, error) {
	if len(r) == 0 {
		k.logger.Printf("finalized report is empty; not accepting epoch %d and round %d", rt.Epoch, rt.Round)
		return false, nil
	}

	results, err := k.encoder.DecodeReport(r)
	if err != nil {
		return false, err
	}

	if len(results) == 0 {
		k.logger.Printf("no upkeeps in report; not accepting epoch %d and round %d", rt.Epoch, rt.Round)
		return false, nil
	}

	for _, r := range results {
		// indicate to the filter that the key has been accepted for transmit
		err = k.filter.Accept(r.Key)
		if err != nil {
			if errors.Is(err, ErrKeyAlreadySet) {
				k.logger.Printf("%s: key already set; epoch %d and round %d", err, rt.Epoch, rt.Round)
				return false, nil
			}
			return false, fmt.Errorf("%w: failed to accept key from epoch %d and round %d", err, rt.Epoch, rt.Round)
		}
		k.logger.Printf("accepting key %s for epoch %d and round %d", r.Key, rt.Epoch, rt.Round)
	}

	return true, nil
}

// ShouldTransmitAcceptedReport implements the types.ReportingPlugin interface
// from OCR2. The implementation essentially draws straws on which node should
// be the transmitter.
func (k *keepers) ShouldTransmitAcceptedReport(_ context.Context, rt types.ReportTimestamp, r types.Report) (bool, error) {
	results, err := k.encoder.DecodeReport(r)
	if err != nil {
		return false, fmt.Errorf("failed to get ids from report: %w", err)
	}

	if len(results) == 0 {
		return false, fmt.Errorf("no ids in report in epoch %d for round %d", rt.Epoch, rt.Round)
	}

	for _, id := range results {
		transmitConfirmed := k.filter.IsTransmissionConfirmed(id.Key)
		// multiple keys can be in a single report. if one has a confirmed transmission
		// (while others may not have), don't try to transmit again
		// TODO: reevaluate this assumption
		if transmitConfirmed {
			k.logger.Printf("not transmitting report because upkeep '%s' was already transmitted before epoch %d and round %d", string(id.Key), rt.Epoch, rt.Round)
			return false, nil
		}

		k.logger.Printf("upkeep '%s' transmit not confirmed for epoch %d and round %d", id.Key, rt.Epoch, rt.Round)
	}

	return true, nil
}

// Close implements the types.ReportingPlugin interface in OCR2.
func (k *keepers) Close() error {
	return nil
}
