package chain

import (
	"context"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/smartcontractkit/ocr2keepers/gethwrappers/keeper_registry_v1_2"
	"github.com/smartcontractkit/ocr2keepers/internal/keepers"
	"github.com/smartcontractkit/ocr2keepers/internal/mocks"
	"github.com/smartcontractkit/ocr2keepers/pkg/types"
)

func TestGetActiveUpkeepKeys(t *testing.T) {
	mockClient := new(mocks.Client)
	ctx := context.Background()
	kabi, _ := keeper_registry_v1_2.KeeperRegistryMetaData.GetAbi()
	rec := mocks.NewContractMockReceiver(t, mockClient, *kabi)

	block := big.NewInt(4)
	mockClient.On("HeaderByNumber", ctx, mock.Anything).Return(&ethtypes.Header{Number: block}, nil).Once()

	state := MockGetState
	state.State.NumUpkeeps = big.NewInt(4)
	ids := []*big.Int{big.NewInt(1), big.NewInt(2), big.NewInt(3), big.NewInt(4)}

	rec.MockResponse("getState", state)
	rec.MockResponse("getActiveUpkeepIDs", ids)

	reg, err := NewEVMRegistryV1_2(common.Address{}, mockClient)
	if err != nil {
		t.FailNow()
	}

	keys, err := reg.GetActiveUpkeepKeys(ctx, types.BlockKey("0"))
	if err != nil {
		t.Logf("error: %s", err)
		t.FailNow()
	}

	assert.Len(t, keys, 4)
	mockClient.Mock.AssertExpectations(t)
}

func TestCheckUpkeep(t *testing.T) {
	kabi, _ := keeper_registry_v1_2.KeeperRegistryMetaData.GetAbi()

	t.Run("Perform", func(t *testing.T) {
		mockClient := new(mocks.Client)
		ctx := context.Background()
		rec := mocks.NewContractMockReceiver(t, mockClient, *kabi)

		responseArgs := []interface{}{[]byte{}, big.NewInt(0), big.NewInt(0), big.NewInt(0), big.NewInt(0)}
		rec.MockResponse("checkUpkeep", responseArgs...)

		reg, err := NewEVMRegistryV1_2(common.Address{}, mockClient)
		if err != nil {
			t.FailNow()
		}

		ok, upkeep, err := reg.CheckUpkeep(ctx, types.Address([]byte("7865")), types.UpkeepKey([]byte("1|1234")))
		assert.NoError(t, err)
		assert.Equal(t, true, ok)
		assert.Equal(t, keepers.Perform, upkeep.State)
	})

	t.Run("Skip", func(t *testing.T) {
		mockClient := new(mocks.Client)
		ctx := context.Background()
		rec := mocks.NewContractMockReceiver(t, mockClient, *kabi)

		rec.MockRevertResponse("checkUpkeep", " UpkeepNotNeeded")

		reg, err := NewEVMRegistryV1_2(common.Address{}, mockClient)
		if err != nil {
			t.FailNow()
		}

		ok, upkeep, err := reg.CheckUpkeep(ctx, types.Address([]byte("7865")), types.UpkeepKey([]byte("1|1234")))
		assert.NoError(t, err)
		assert.Equal(t, false, ok)
		assert.Equal(t, keepers.Skip, upkeep.State)
	})

}

var MockRegistryState = keeper_registry_v1_2.State{
	Nonce:               uint32(0),
	OwnerLinkBalance:    big.NewInt(1000000000000000000),
	ExpectedLinkBalance: big.NewInt(1000000000000000000),
	NumUpkeeps:          big.NewInt(0),
}

var MockRegistryConfig = keeper_registry_v1_2.Config{
	PaymentPremiumPPB:    100,
	FlatFeeMicroLink:     uint32(0),
	BlockCountPerTurn:    big.NewInt(20),
	CheckGasLimit:        2_000_000,
	StalenessSeconds:     big.NewInt(3600),
	GasCeilingMultiplier: uint16(2),
	MinUpkeepSpend:       big.NewInt(0),
	MaxPerformGas:        uint32(5000000),
	FallbackGasPrice:     big.NewInt(1000000),
	FallbackLinkPrice:    big.NewInt(1000000),
	Transcoder:           common.Address{},
	Registrar:            common.Address{},
}

var MockGetState = keeper_registry_v1_2.GetState{
	State:   MockRegistryState,
	Config:  MockRegistryConfig,
	Keepers: []common.Address{},
}