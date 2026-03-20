package event

import (
	"context"
	"time"

	"chainmaker.org/chainmaker/pb-go/v2/common"

	"github.com/redis/go-redis/v9"
)

type RedisInterface interface {
	Close() error
	Ping(ctx context.Context) error
	XAddEventAndSetBlockHeightInPipeline(ctx context.Context,
		event *common.ContractEventInfo, sid, topic string,
		contractType, partition int) error
	PublishEventToStream(ctx context.Context, event *common.ContractEventInfo,
		sid, topic string, contractType, partition int) error
	PublishDataToStream(ctx context.Context, data interface{}, streamId string) error
	SubscribeDataFromStream(ctx context.Context, streamId,
		groupName, consumerName string, handler func([]byte) error,
		wantTrimOldMsg bool, ackCountThreshold int64, block time.Duration) error
	CreateConsumerGroup(ctx context.Context, streamId, groupName, consumerName string) error
	GetConsumerPending(ctx context.Context, streamId, groupName, consumerName string) (int64, error)
	GetLastId(ctx context.Context, streamId, groupName, consumerName string) (string, error)
	SubscribeByStreamId(ctx context.Context, streamId, groupName, consumerName string,
		handler func(data []byte, messageId string) error, wantTrimOldMsg bool, ackCountThreshold int64,
		block time.Duration, valueKey string) error
	SubscribeToStream(ctx context.Context, sid, contractName,
		groupName, consumerName string, handler func(*common.ContractEventInfo),
		wantTrimOldMsg bool, ackCountThreshold int64, block time.Duration) error
	SubscribeToStreamWithHandlerError(ctx context.Context, sid, contractName,
		groupName, consumerName string, handler func(*common.ContractEventInfo) error,
		wantTrimOldMsg bool, ackCountThreshold int64, block time.Duration) error
	SubscribeToStreamv100tdh(ctx context.Context, chainID, subChainID, contractName,
		eventName, groupName, consumerName string, handler func(*common.ContractEventInfo),
		wantTrimOldMsg bool, ackCountThreshold int64, block time.Duration) error
	SetLatestBlockHeight(ctx context.Context, chainID string, height int64) error
	GetLatestBlockHeight(ctx context.Context, chainID string) (int64, error)
	GetMaxPending(ctx context.Context, streamId string) (int64, error)
	TrimOldMsg(ctx context.Context, streamId string, ackCountThreshold int64) (int64, error)
	GetLastAckMsgId(ctx context.Context, streamId, lastDeliveredID string,
		pending int64) (string, error)
	GetMinLastAckIdInAllGroup(ctx context.Context, streamId string,
		streamLen int64) (string, int64, error)
	GetStickyConns(ctx context.Context, redisMode string) (map[string]*redis.Conn, error)
	GetClusterTopology(ctx context.Context) ([]redis.ClusterSlot, error)
}
