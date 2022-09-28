package keepers

import (
	"fmt"
	"log"
	"time"

	"github.com/smartcontractkit/libocr/offchainreporting2/types"
	ktypes "github.com/smartcontractkit/ocr2keepers/pkg/types"
)

type ReportingFactoryConfig struct {
	CacheExpiration       time.Duration
	CacheEvictionInterval time.Duration
	MaxServiceWorkers     int
	ServiceQueueLength    int
}

// NewReportingPluginFactory returns an OCR ReportingPluginFactory. When the plugin
// starts, a separate service is started as a separate go-routine automatically. There
// is no start or stop function for this service so stopping this service relies on
// releasing references to the plugin such that the Go garbage collector cleans up
// hanging routines automatically.
func NewReportingPluginFactory(registry ktypes.Registry, encoder ktypes.ReportEncoder, logger *log.Logger, config ReportingFactoryConfig) types.ReportingPluginFactory {
	return &keepersReportingFactory{registry: registry, encoder: encoder, logger: logger, config: config}
}

type keepersReportingFactory struct {
	registry ktypes.Registry
	encoder  ktypes.ReportEncoder
	logger   *log.Logger
	config   ReportingFactoryConfig
}

var _ types.ReportingPluginFactory = (*keepersReportingFactory)(nil)

// NewReportingPlugin implements the libocr/offchainreporting2/types ReportingPluginFactory interface
func (d *keepersReportingFactory) NewReportingPlugin(c types.ReportingPluginConfig) (types.ReportingPlugin, types.ReportingPluginInfo, error) {
	/*
		var offChainCfg offChainConfig
		err := decode(c.OffchainConfig, &offChainCfg)
		if err != nil {
			return nil, types.ReportingPluginInfo{}, fmt.Errorf("%w: failed to decode off chain config", err)
		}
	*/

	info := types.ReportingPluginInfo{
		Name: fmt.Sprintf("Oracle %d: Keepers Plugin Instance w/ Digest '%s'", c.OracleID, c.ConfigDigest),
		Limits: types.ReportingPluginLimits{
			// queries should be empty anyway with the current implementation
			MaxQueryLength: 0,
			// an upkeep key is composed of a block number and upkeep id (~8 bytes)
			// an observation is multiple upkeeps to be performed
			// 100 upkeeps to be performed would be a very high upper limit
			// 100 * 10 = 1_000 bytes
			MaxObservationLength: 1_000,
			// a report is composed of 1 or more abi encoded perform calls
			// with performData of arbitrary length
			MaxReportLength: 10_000, // TODO (config): pick sane limit based on expected performData size. maybe set this to block size limit or 2/3 block size limit?
		},
		// TODO: unique reports may need to be a configuration param from
		// offChainConfig or onChainConfig
		// unique reports ensures that each round produces only a single report
		UniqueReports: true,
	}

	// TODO (config): sample ratio is calculated with number of rounds, number
	// of nodes, and target probability for all upkeeps to be checked. each
	// chain will have a different average number of rounds per block. this
	// number needs to either come from a config, or be calculated on actual
	// performance of the nodes in real time. that is, start at 1 and increment
	// after some blocks pass until a stable number is reached.
	sample, err := sampleFromProbability(1, c.N-c.F, 0.99999)
	if err != nil {
		return nil, info, fmt.Errorf("%w: failed to create plugin", err)
	}

	service := newSimpleUpkeepService(
		sample,
		d.registry,
		d.logger,
		d.config.CacheExpiration,
		d.config.CacheEvictionInterval,
		d.config.MaxServiceWorkers,
		d.config.ServiceQueueLength)

	return &keepers{service: service, encoder: d.encoder, logger: d.logger}, info, nil
}
