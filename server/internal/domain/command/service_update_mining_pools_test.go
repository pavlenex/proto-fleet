package command

import (
	"context"
	"errors"
	"testing"

	pb "github.com/block/proto-fleet/server/generated/grpc/minercommand/v1"
	"github.com/block/proto-fleet/server/internal/domain/fleeterror"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type stubSV2TranslatorRouter struct {
	organizationID int64
	upstreamURL    string
	username       string
	err            error
}

func (s *stubSV2TranslatorRouter) Route(
	_ context.Context,
	organizationID int64,
	upstreamURL string,
	username string,
) (string, string, string, error) {
	s.organizationID = organizationID
	s.upstreamURL = upstreamURL
	s.username = username
	if s.err != nil {
		return "", "", "", s.err
	}
	return "stratum+tcp://192.168.1.10:34255", username, "x", nil
}

func TestCreateUpdateMiningPoolsPayload_RawPoolsPreserveExistingSuffixes(t *testing.T) {
	service := &Service{}

	payload, err := service.createUpdateMiningPoolsPayload(
		context.Background(),
		&pb.PoolSlotConfig{
			PoolSource: &pb.PoolSlotConfig_RawPool{
				RawPool: &pb.RawPoolInfo{
					Url:      "stratum+tcp://pool1.example.com:3333",
					Username: "wallet.existing-worker",
				},
			},
		},
		&pb.PoolSlotConfig{
			PoolSource: &pb.PoolSlotConfig_RawPool{
				RawPool: &pb.RawPoolInfo{
					Url:      "stratum+tcp://pool2.example.com:3333",
					Username: "wallet.backup-worker",
				},
			},
		},
		nil,
	)
	require.NoError(t, err)

	assert.False(t, payload.DefaultPool.AppendMinerName)
	assert.Equal(t, "wallet.existing-worker", payload.DefaultPool.Username)
	require.NotNil(t, payload.Backup1Pool)
	assert.False(t, payload.Backup1Pool.AppendMinerName)
	assert.Equal(t, "wallet.backup-worker", payload.Backup1Pool.Username)
}

func TestCreateUpdateMiningPoolsPayload_RawPoolsAppendMinerNameWhenUsernameHasNoSuffix(t *testing.T) {
	service := &Service{}

	payload, err := service.createUpdateMiningPoolsPayload(
		context.Background(),
		&pb.PoolSlotConfig{
			PoolSource: &pb.PoolSlotConfig_RawPool{
				RawPool: &pb.RawPoolInfo{
					Url:      "stratum+tcp://pool1.example.com:3333",
					Username: "wallet",
				},
			},
		},
		&pb.PoolSlotConfig{
			PoolSource: &pb.PoolSlotConfig_RawPool{
				RawPool: &pb.RawPoolInfo{
					Url:      "stratum+tcp://pool2.example.com:3333",
					Username: "wallet-backup",
				},
			},
		},
		nil,
	)
	require.NoError(t, err)

	assert.True(t, payload.DefaultPool.AppendMinerName)
	assert.Equal(t, "wallet", payload.DefaultPool.Username)
	require.NotNil(t, payload.Backup1Pool)
	assert.True(t, payload.Backup1Pool.AppendMinerName)
	assert.Equal(t, "wallet-backup", payload.Backup1Pool.Username)
}

func TestCreateUpdateMiningPoolsPayload_RoutesRawSV2PoolThroughTranslator(t *testing.T) {
	router := &stubSV2TranslatorRouter{}
	service := &Service{sv2Translator: router}
	upstreamURL := "stratum2+tcp://v2.example.com:3336/9awtMD5KQgvRUh2yFbjVeT7b6hjipWcAsQHd6wEhgtDT9soosna"

	payload, err := service.createUpdateMiningPoolsPayload(
		sessionCtxWithOrg(7),
		&pb.PoolSlotConfig{
			PoolSource: &pb.PoolSlotConfig_RawPool{
				RawPool: &pb.RawPoolInfo{
					Url:      upstreamURL,
					Username: "wallet",
				},
			},
		},
		nil,
		nil,
	)

	require.NoError(t, err)
	assert.Equal(t, int64(7), router.organizationID)
	assert.Equal(t, upstreamURL, router.upstreamURL)
	assert.Equal(t, "wallet", router.username)
	assert.Equal(t, "stratum+tcp://192.168.1.10:34255", payload.DefaultPool.URL)
	assert.Equal(t, "wallet", payload.DefaultPool.Username)
	assert.Equal(t, "x", payload.DefaultPool.Password)
	assert.True(t, payload.DefaultPool.AppendMinerName)
}

func TestCreateUpdateMiningPoolsPayload_ReportsTranslatorStartupFailure(t *testing.T) {
	service := &Service{
		sv2Translator: &stubSV2TranslatorRouter{err: errors.New("listener unavailable")},
	}

	_, err := service.createUpdateMiningPoolsPayload(
		sessionCtxWithOrg(7),
		&pb.PoolSlotConfig{
			PoolSource: &pb.PoolSlotConfig_RawPool{
				RawPool: &pb.RawPoolInfo{
					Url:      "stratum2+tcp://v2.example.com:3336/9awtMD5KQgvRUh2yFbjVeT7b6hjipWcAsQHd6wEhgtDT9soosna",
					Username: "wallet",
				},
			},
		},
		nil,
		nil,
	)

	require.Error(t, err)
	assert.True(t, fleeterror.IsFailedPreconditionError(err))
	assert.Contains(t, err.Error(), "listener unavailable")
}
