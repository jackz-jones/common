// Package event provides a Redis client to publish and subscribe to events.
package event

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"chainmaker.org/chainmaker/pb-go/v2/common"
	"github.com/joaojeronimo/go-crc16"
	"github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/logx"
)

// RedisEventKey is the key used to store the event data in Redis.
const RedisEventKey = "event"
const BlockHeightStr = "block_height"
const RedisAlreadyExists = "Consumer Group name already exists"
const RedisDataKey = "data"

// RedisClient is a simple struct to hold the Redis client.
type RedisClient struct {
	RedisClient RedisNode
}

// RedisNode interface represents a redis node.
type RedisNode interface {
	io.Closer
	redis.Cmdable
	redis.BitMapCmdable
	Watch(ctx context.Context, fn func(*redis.Tx) error, keys ...string) error
}

// NewRedisClient creates a new RedisClient with a connected Redis client.
//
//	@Description:
//	@param redisAddr
//	@param username
//	@param password
//	@return *RedisClient
//	@return error
func NewRedisClient(confType, redisAddr, username, password, masterName string) (*RedisClient, error) {
	ctx := context.Background()
	switch confType {
	case RedisTypeSentinel:
		// 哨兵集群模式
		addrs := strings.Split(redisAddr, ",")
		rdb := redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:            masterName,
			SentinelAddrs:         addrs,
			Username:              username, // 用户名
			Password:              password, // 密码，没有则留空
			ContextTimeoutEnabled: true,
		})
		err := rdb.Ping(context.Background()).Err()
		if err != nil {
			return nil, err
		}
		return &RedisClient{
			RedisClient: rdb,
		}, nil
	case RedisTypeCluster:
		// 集群模式
		addrs := strings.Split(redisAddr, ",")
		rdb := redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:                 addrs,
			Username:              username, // 用户名
			Password:              password, // 密码，没有则留空
			ContextTimeoutEnabled: true,
		})
		err := rdb.Ping(context.Background()).Err()
		if err != nil {
			return nil, err
		}
		return &RedisClient{
			RedisClient: rdb,
		}, nil
	case RedisTypeNode:
		// 单点模式
		rdb := redis.NewClient(&redis.Options{
			Addr:                  redisAddr, // Redis地址
			Username:              username,  // 用户名
			Password:              password,  // 密码，没有则留空
			DB:                    0,         // 使用默认DB
			ContextTimeoutEnabled: true,
		})

		_, err := rdb.Ping(ctx).Result()
		if err != nil {
			return nil, err
		}

		return &RedisClient{
			RedisClient: rdb,
		}, nil
	}
	return nil, fmt.Errorf("unsupported Redis type: %s", confType)

}

// Close closes the Redis connection.
func (p *RedisClient) Close() error {
	return p.RedisClient.Close()
}

// Ping checks the connectivity to the Redis server.
func (p *RedisClient) Ping(ctx context.Context) error {
	return p.RedisClient.Ping(ctx).Err()
}

// XAddEventAndSetBlockHeightInPipeline 提交事件和块高更新操作到 pipeline 批处理
func (p *RedisClient) XAddEventAndSetBlockHeightInPipeline(ctx context.Context,
	event *common.ContractEventInfo, sid, topic string,
	contractType, partition int) error {

	// 获取实际的 streamId 和要写入的 msg
	streamId, msg, err := GetStreamIdAndMSg(contractType, partition, sid, topic, event)
	if err != nil {
		return fmt.Errorf("failed to GetStreamIdAndMSg, %v", err)
	}

	// 打开 pipeline
	pipeline := p.RedisClient.Pipeline()

	// 提交事件写入操作
	_, err = pipeline.XAdd(ctx, &redis.XAddArgs{
		Stream: streamId,
		Values: map[string]interface{}{RedisEventKey: msg},
	}).Result()
	if err != nil {
		return fmt.Errorf("failed to pipeline.XAdd event, %v", err)
	}

	// 如果有 topic，那么更新高度的 key 需要加上 topic
	heightKey := strings.Join([]string{sid, event.ContractName}, "#")
	if topic != "" {
		heightKey = strings.Join([]string{heightKey, topic}, "#")
	}

	// 提交块高更新操作
	err = pipeline.Set(ctx, strings.Join([]string{BlockHeightStr, heightKey}, "#"),
		strconv.FormatUint(event.BlockHeight, 10), 0).Err()
	if err != nil {
		return fmt.Errorf("failed to pipeline.Set block height, %v", err)
	}

	// 提交 pipeline，此时 redis client 会将收集的多个指令一起发送到 redis server 端去执行
	_, err = pipeline.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to exec pipeline, %v", err)
	}

	logx.Debugf("success to xadd event and set block height in pipeline, streamId: %s", streamId)
	return nil
}

// PublishTradeGuardEventToStream publishes an TradeGuard event to a Redis channel.
//
//	@Description:
//	@receiver p
//	@param ctx
//	@param event
//	@param chainType 链类型
//	@param chainConfName 链配置名称
//	@param contractConfName 合约配置名称
//	@param evenName 事件名称
//	@return error
func (p *RedisClient) PublishTradeGuardEventToStream(ctx context.Context, event interface{}, chainType,
	chainConfName, contractType, contractConfName, evenName string) error {
	message, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %v", err)
	}

	// 封装成数字海关统一的事件结构
	tradeGuardEvent := TradeGuardEvent{
		EventName:    evenName,
		ChainType:    chainType,
		ChainName:    chainConfName,
		ContractType: contractType,
		ContractName: contractConfName,
		EventData:    message,
	}

	tradeGuardEventMessage, err := json.Marshal(tradeGuardEvent)
	if err != nil {
		return fmt.Errorf("failed to marshal tradeGuardEvent: %v", err)
	}

	// 添加事件到 redis
	_, err = p.RedisClient.XAdd(ctx, &redis.XAddArgs{
		Stream: strings.Join([]string{chainType, chainConfName, contractType, contractConfName}, "#"),
		Values: map[string]interface{}{RedisEventKey: tradeGuardEventMessage},
	}).Result()
	if err != nil {
		return fmt.Errorf("failed to publish event stream to redis: %v", err)
	}
	return nil
}

// PublishEventToStream publishes an event to a Redis channel.
//
//	@Description:
//	@receiver p
//	@param ctx
//	@param event
//	@param sid
//	@return error
func (p *RedisClient) PublishEventToStream(ctx context.Context, event *common.ContractEventInfo,
	sid, topic string, contractType, partition int) error {

	// 获取实际的 streamId 和要写入的 msg
	streamId, msg, err := GetStreamIdAndMSg(contractType, partition, sid, topic, event)
	if err != nil {
		return fmt.Errorf("failed to GetStreamIdAndMSg, %v", err)
	}

	// 写入 redis
	_, err = p.RedisClient.XAdd(ctx, &redis.XAddArgs{
		Stream: streamId,
		Values: map[string]interface{}{RedisEventKey: msg},
	}).Result()
	if err != nil {
		return fmt.Errorf("failed to publish event stream to redis, %v", err)
	}

	logx.Debugf("success to publish event stream to redis, streamId: %s", streamId)
	return nil
}

// PublishDataToStream 推送数据到 stream 中，通用方法.
//
//	@Description:
//	@receiver p
//	@param ctx
//	@param data 要推送的数据
//	@param streamId 推送的目标 stream
//	@return error
func (p *RedisClient) PublishDataToStream(ctx context.Context, data interface{}, streamId string) error {

	// 序列化数据
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal data in PublishDataToStream, %v", err)
	}

	// 写入 redis
	_, err = p.RedisClient.XAdd(ctx, &redis.XAddArgs{
		Stream: streamId,
		Values: map[string]interface{}{RedisDataKey: dataBytes},
	}).Result()
	if err != nil {
		return fmt.Errorf("failed to publish data to redis stream, %v", err)
	}

	logx.Debugf("success to publish data to redis stream, streamId: %s", streamId)
	return nil
}

// SubscribeDataFromStream 从 stream 中订阅数据，通用方法.
//
//	@Description:
//	@receiver p
//	@param ctx
//	@param streamId
//	@param groupName
//	@param consumerName
//	@param handler 业务处理函数，接收数据 []byte 类型，json序列化过，业务自行根据
//	@param wantTrimOldMsg
//	@param ackCountThreshold
//	@param block
//	@return error
func (p *RedisClient) SubscribeDataFromStream(ctx context.Context, streamId,
	groupName, consumerName string, handler func([]byte) error,
	wantTrimOldMsg bool, ackCountThreshold int64, block time.Duration) error {

	// 转换 handler
	handler2 := func(data []byte, messageId string) error {
		return handler(data)
	}

	return p.SubscribeByStreamId(ctx, streamId, groupName, consumerName, handler2, wantTrimOldMsg,
		ackCountThreshold, block, RedisDataKey)
}

// CreateConsumerGroup  creates a new consumer group in Redis.
//
//	@Description:
//	@receiver p
//	@param ctx
//	@param streamId
//	@param groupName
//	@return error
func (p *RedisClient) CreateConsumerGroup(ctx context.Context, streamId, groupName, consumerName string) error {

	// 创建消费者组
	_, err := p.RedisClient.XGroupCreateMkStream(ctx, streamId, groupName, "0").Result()
	if err != nil && !strings.Contains(err.Error(), RedisAlreadyExists) {
		return err
	}

	// 创建消费者
	_, err = p.RedisClient.XGroupCreateConsumer(ctx, streamId, groupName, consumerName).Result()
	if err != nil {
		return err
	}
	return nil
}

// GetConsumerPending 获取消费者 pending 消息数量
func (p *RedisClient) GetConsumerPending(ctx context.Context, streamId, groupName, consumerName string) (int64, error) {
	consumers, err := p.RedisClient.XInfoConsumers(ctx, streamId, groupName).Result()
	if err != nil {
		return 0, fmt.Errorf("failed to XInfoConsumers, %v", err)
	}

	for _, consumer := range consumers {
		if consumer.Name == consumerName {
			return consumer.Pending, nil
		}
	}

	return 0, nil
}

// GetLastId 获取 read stream 的起始 id
func (p *RedisClient) GetLastId(ctx context.Context, streamId, groupName, consumerName string) (string, error) {

	// 初始时 consumer 还没有记录在 group 中，需要和 group 一起提前创建好，否则这里获取不到
	pending, err := p.GetConsumerPending(ctx, streamId, groupName, consumerName)
	if err != nil {
		return "", fmt.Errorf("failed to GetConsumerPending, %v", err)
	}

	// 如果当前消费者没有 pending 消息，lastId 设置 >
	if pending == 0 {
		return ">", nil
	}

	// 否则设置 0
	return "0", nil
}

// SubscribeByStreamId subscribes to a Redis stream by streamId.
//
//	@Description:
//	@receiver p
//	@param ctx
//	@param streamId
//	@param groupName
//	@param consumerName
//	@param handler
//	@param wantTrimOldMsg
//	@param ackCountThreshold
//	@param block
//	@return error
func (p *RedisClient) SubscribeByStreamId(ctx context.Context, streamId, groupName, consumerName string,
	handler func(data []byte, messageId string) error, wantTrimOldMsg bool, ackCountThreshold int64,
	block time.Duration, valueKey string) error {

	// 限制 ackCountThreshold 最小 10000 条
	if ackCountThreshold < minAckCountThreshold {
		ackCountThreshold = minAckCountThreshold
	}

	if err := p.CreateConsumerGroup(ctx, streamId, groupName, consumerName); err != nil {
		return fmt.Errorf("failed to CreateConsumerGroup for stream %s group %s consumer %s, %v",
			streamId, groupName, consumerName, err)
	}

	// 获取当前消费者组的 lastId
	lastId, err := p.GetLastId(ctx, streamId, groupName, consumerName)
	if err != nil {
		return fmt.Errorf("failed to GetLastId for stream %s group %s consumer %s, %v",
			streamId, groupName, consumerName, err)
	}

	for {

		// 如果 ctx.Done() 则退出，否则继续执行消费逻辑，方便业务层退出控制
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		xReadGroupArgs := &redis.XReadGroupArgs{
			Group:    groupName,
			Consumer: consumerName,
			Streams:  []string{streamId, lastId},
			Count:    1,
			Block:    block,
			NoAck:    false,
		}
		messages, err := p.RedisClient.XReadGroup(ctx, xReadGroupArgs).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				logx.Debugf("[redis-stream]no msg for stream %s group %s consumer %s", streamId, groupName, consumerName)
				continue
			}
			return fmt.Errorf("failed to XReadGroup for stream %s group %s consumer %s, %v",
				streamId, groupName, consumerName, err)
		}

		for _, message := range messages {
			for _, xMessage := range message.Messages {

				// 获取消息内容，此时是 json marshal 字节格式
				data, ok := xMessage.Values[valueKey]
				if !ok {
					logx.Errorf("[redis-stream %s][lastId %s][%#v]not redis value for %s", streamId, lastId, xMessage, valueKey)

					// 对于不是我们的消息，当前消费组直接 ack掉，防止阻塞
					_, err = p.RedisClient.XAck(ctx, streamId, groupName, xMessage.ID).Result()
					if err != nil {
						return fmt.Errorf("failed to ack message %s for stream %s group %s consumer %s, %v",
							xMessage.ID, streamId, groupName, consumerName, err)
					}
					continue
				}

				// 转成 string
				dataBytes, ok := data.(string)
				if !ok {
					logx.Errorf("[redis-stream %s][lastId %s][currentId %s]data can not convert to string：%#v",
						streamId, lastId, xMessage.ID, data)
					continue
				}

				// 处理消息
				if err = handler([]byte(dataBytes), xMessage.ID); err != nil {
					return fmt.Errorf("failed to handler message %s for stream %s group %s consumer %s, %v",
						xMessage.ID, streamId, groupName, consumerName, err)
				}

				// ack 消息
				_, err = p.RedisClient.XAck(ctx, streamId, groupName, xMessage.ID).Result()
				if err != nil {
					return fmt.Errorf("failed to ack message %s for stream %s group %s consumer %s, %v",
						xMessage.ID, streamId, groupName, consumerName, err)
				}
			}

			// 获取当前消费者组的 lastId
			lastId, err = p.GetLastId(ctx, streamId, groupName, consumerName)
			if err != nil {
				return fmt.Errorf("failed to GetLastId for stream %s group %s consumer %s, %v",
					streamId, groupName, consumerName, err)
			}
		}

		// 修剪 ack 过的历史消息
		if wantTrimOldMsg {
			_, err = p.TrimOldMsg(ctx, streamId, ackCountThreshold)
			if err != nil {
				return fmt.Errorf("failed to TrimOldMsg for stream %s group %s consumer %s,ackCountThreshold[%d], %v",
					streamId, groupName, consumerName, ackCountThreshold, err)
			}
		}
	}
}

// SubscribeTradeGuardEventFromStream subscribes to a Redis stream and processes events using the provided
// handler function.
//
//	@Description:
//	@receiver p
//	@param ctx
//	@param chainType 链类型
//	@param chainConfName 链配置名称
//	@param contractConfName 合约配置名称
//	@param groupName
//	@param consumerName
//	@param handler
//	@return error
func (p *RedisClient) SubscribeTradeGuardEventFromStream(ctx context.Context, chainType, chainConfName,
	contractType, contractConfName, groupName, consumerName string, handler func(event TradeGuardEvent) error,
	wantTrimOldMsg bool, ackCountThreshold int64, block time.Duration) error {
	streamId := strings.Join([]string{chainType, chainConfName, contractType, contractConfName}, "#")

	// 转换 handler
	handlerWithError := func(data []byte, messageId string) error {

		// 转成业务结构
		var tradeGuardEvent TradeGuardEvent
		if err := json.Unmarshal(data, &tradeGuardEvent); err != nil {
			return fmt.Errorf("failed to unmarshal business event data for msg %s for stream %s group %s consumer %s,"+
				" %v", messageId, streamId, groupName, consumerName, err)
		}

		return handler(tradeGuardEvent)
	}
	return p.SubscribeByStreamId(ctx, streamId, groupName, consumerName, handlerWithError, wantTrimOldMsg,
		ackCountThreshold, block, RedisEventKey)
}

// SubscribeFromStream subscribes to a Redis stream and processes events using the provided handler function.
//
//	@Description:
//	@receiver p
//	@param ctx
//	@param sid
//	@param contractName
//	@param groupName
//	@param consumerName
//	@param handler
//	@param wantTrimOldMsg
//	@param ackCountThreshold
//	@param block 消费阻塞时间，比如设置 time.Second * 10，表示如果当前没有消息可读就会暂停 10s，之后再读，如果当前有消息就会持续读。
//	@return error
func (p *RedisClient) SubscribeFromStream(ctx context.Context, sid, contractName,
	groupName, consumerName string, handler func(*common.ContractEventInfo),
	wantTrimOldMsg bool, ackCountThreshold int64, block time.Duration) error {
	streamId := strings.Join([]string{sid, contractName}, "#")

	// 转换 handler
	handlerWithError := func(data []byte, messageId string) error {

		// 业务解析为 event 结构
		var event common.ContractEventInfo
		if err := json.Unmarshal(data, &event); err != nil {
			return fmt.Errorf("failed to unmarshal event data for msg %s for stream %s group %s consumer %s, %v",
				messageId, streamId, groupName, consumerName, err)
		}

		handler(&event)
		return nil
	}
	return p.SubscribeByStreamId(ctx, streamId, groupName, consumerName, handlerWithError, wantTrimOldMsg,
		ackCountThreshold, block, RedisEventKey)
}

// SubscribeTopicFromStream subscribes to a Redis stream and processes events using the provided handler function.
//
//	@Description:
//	@receiver p
//	@param ctx
//	@param sid
//	@param contractName
//	@param groupName
//	@param consumerName
//	@param topic
//	@param handler
//	@param wantTrimOldMsg
//	@param ackCountThreshold
//	@param block
//	@return error
func (p *RedisClient) SubscribeTopicFromStream(ctx context.Context, sid, contractName,
	groupName, consumerName, topic string, handler func(*common.ContractEventInfo),
	wantTrimOldMsg bool, ackCountThreshold int64, block time.Duration) error {
	streamId := strings.Join([]string{sid, contractName, topic}, "#")

	// 转换 handler
	handlerWithError := func(data []byte, messageId string) error {

		// 业务解析为 event 结构
		var event common.ContractEventInfo
		if err := json.Unmarshal(data, &event); err != nil {
			return fmt.Errorf("failed to unmarshal event data for msg %s for stream %s group %s consumer %s, %v",
				messageId, streamId, groupName, consumerName, err)
		}

		handler(&event)
		return nil
	}
	return p.SubscribeByStreamId(ctx, streamId, groupName, consumerName, handlerWithError, wantTrimOldMsg,
		ackCountThreshold, block, RedisEventKey)

}

// SubscribeTopicFromStreamWithHandlerError subscribes to a Redis stream and processes events using
// the provided handler function, and catch handler error.
//
//	@Description:
//	@receiver p
//	@param ctx
//	@param sid
//	@param contractName
//	@param groupName
//	@param consumerName
//	@param topic
//	@param handler
//	@param wantTrimOldMsg
//	@param ackCountThreshold
//	@param block
//	@return error
func (p *RedisClient) SubscribeTopicFromStreamWithHandlerError(ctx context.Context, sid, contractName,
	groupName, consumerName, topic string, handler func(*common.ContractEventInfo) error,
	wantTrimOldMsg bool, ackCountThreshold int64, block time.Duration) error {
	streamId := strings.Join([]string{sid, contractName, topic}, "#")

	// 转换 handler
	handlerWithError := func(data []byte, messageId string) error {

		// 业务解析为 event 结构
		var event common.ContractEventInfo
		if err := json.Unmarshal(data, &event); err != nil {
			return fmt.Errorf("failed to unmarshal event data for msg %s for stream %s group %s consumer %s, %v",
				messageId, streamId, groupName, consumerName, err)
		}

		return handler(&event)
	}
	return p.SubscribeByStreamId(ctx, streamId, groupName, consumerName, handlerWithError, wantTrimOldMsg,
		ackCountThreshold, block, RedisEventKey)

}

// SubscribeFromStreamWithHandlerError subscribes to a Redis stream and processes events using
// the provided handler function, and catch handler error.
//
//	@Description:
//	@receiver p
//	@param ctx
//	@param sid
//	@param contractName
//	@param groupName
//	@param consumerName
//	@param handler
//	@param wantTrimOldMsg
//	@param ackCountThreshold
//	@param block 消费阻塞时间，比如设置 time.Second * 10，表示如果当前没有消息可读就会暂停 10s，之后再读，如果当前有消息就会持续读。
//	@return error
func (p *RedisClient) SubscribeFromStreamWithHandlerError(ctx context.Context, sid, contractName,
	groupName, consumerName string, handler func(*common.ContractEventInfo) error,
	wantTrimOldMsg bool, ackCountThreshold int64, block time.Duration) error {
	streamId := strings.Join([]string{sid, contractName}, "#")

	// 转换 handler
	handler2 := func(data []byte, messageId string) error {

		// 业务解析为 event 结构
		var event common.ContractEventInfo
		if err := json.Unmarshal(data, &event); err != nil {
			return fmt.Errorf("failed to unmarshal event data for msg %s for stream %s group %s consumer %s, %v",
				messageId, streamId, groupName, consumerName, err)
		}

		return handler(&event)
	}
	return p.SubscribeByStreamId(ctx, streamId, groupName, consumerName, handler2, wantTrimOldMsg,
		ackCountThreshold, block, RedisEventKey)
}

// SetLatestBlockHeight sets the latest block height
//
//	@Description:
//	@receiver p
//	@param ctx
//	@param chainID
//	@param height
//	@return error
func (p *RedisClient) SetLatestBlockHeight(ctx context.Context, chainID string, height int64) error {
	err := p.RedisClient.Set(ctx, strings.Join([]string{BlockHeightStr, chainID}, "#"),
		strconv.FormatInt(height, 10), 0).Err()
	if err != nil {
		return fmt.Errorf("failed to set redis kv, %v", err)
	}
	return nil
}

// GetLatestBlockHeight gets the latest block height
//
//	@Description:
//	@receiver p
//	@param ctx
//	@param chainID
//	@return int64
//	@return error
func (p *RedisClient) GetLatestBlockHeight(ctx context.Context, chainID string) (int64, error) {
	val, err := p.RedisClient.Get(ctx, strings.Join([]string{BlockHeightStr, chainID}, "#")).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return 0, nil
		}
		return -1, fmt.Errorf("failed to get redis kv, %v", err)
	}
	height, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return -1, fmt.Errorf("failed to atoi string[%s], %v", val, err)
	}
	return height, nil
}

// GetMaxPending gets the max pending message count from a stream
//
//	@Description:
//	@receiver p
//	@param ctx
//	@param streamId
//	@return error
func (p *RedisClient) GetMaxPending(ctx context.Context, streamId string) (int64, error) {
	groups, err := p.RedisClient.XInfoGroups(ctx, streamId).Result()
	if err != nil {
		return 0, fmt.Errorf("failed to XInfoGroups, %v", err)
	}

	// 获取当前 stream 中多消费端的最大未消费消息数据
	var maxPending int64
	for _, group := range groups {
		temp := group.Pending + group.Lag
		if temp > maxPending {
			maxPending = temp
		}
	}

	return maxPending, nil
}

// TrimOldMsg trims old messages from a stream
func (p *RedisClient) TrimOldMsg(ctx context.Context, streamId string, ackCountThreshold int64) (int64, error) {
	start := time.Now().UnixMicro()

	// 获取当前 stream 的长度
	streamLen, err := p.RedisClient.XLen(ctx, streamId).Result()
	if err != nil {
		return 0, fmt.Errorf("failed to XLen, %v", err)
	}

	// 获取当前 stream 下所有消费组的最新 ack 消息中最小的 id
	id, ackCount, err := p.GetMinLastAckIdInAllGroup(ctx, streamId, streamLen)
	if err != nil {
		return 0, fmt.Errorf("failed to GetMinLastAckIdInAllGroup, %v", err)
	}

	logx.Infof("[redis-stream][TrimOldMsg]minId: %s, ackCount: %d, ackCountThreshold: %d, streamLen: %d",
		id, ackCount, ackCountThreshold, streamLen)

	// 如果 ack 过的消息数小于指定数量，则无需修剪，可以减少频繁的修剪操作
	if id == "" || ackCount <= ackCountThreshold {
		return 0, nil
	}

	// 删除 id 之前的所有消息，保留 >= id 的消息
	count, err := p.RedisClient.XTrimMinID(ctx, streamId, id).Result()
	if err != nil {
		return 0, fmt.Errorf("failed to XTrimMinID,streamId[%s], %v", streamId, err)
	}

	// 返回修剪的消息条数
	end := time.Now().UnixMicro()
	logx.Infof("[redis-stream][TrimOldMsg]trim old msg count: %d,cost %d us", count, end-start)
	return count, nil
}

// GetLastAckMsgId 获取指定 lastDeliveredID 之前 pending 条消息的最小消息id
func (p *RedisClient) GetLastAckMsgId(ctx context.Context, streamId, lastDeliveredID string,
	pending int64) (string, error) {

	// 如果 pending 为 0，说明 lastDeliveredID 就是当前消费组最新的 ack 消息 id
	if pending <= 0 {
		return lastDeliveredID, nil
	}

	// 从新到旧排序查询，找到 endId 之前的 pending+1 条消息，因为 XRevRangeN 是 [start,end] 双闭区间查询，
	// 所以查到的第一个肯定是 lastDeliveredID，而它对应的消息是未 ack 的，不应该被修剪
	msgs, err := p.RedisClient.XRevRangeN(ctx, streamId, lastDeliveredID, "0", pending+1).Result()
	if err != nil {
		return "", fmt.Errorf("failed to XRevRangeN, %v", err)
	}

	// 只有查到的消息数大于 pending 数，说明才能找到最新 ack 消息 id，这时才能去修剪，否则不修剪
	if len(msgs) <= int(pending) {
		return "", nil
	}

	// 取最后一条消息的 id 作为最新 ack 消息的 id
	return msgs[len(msgs)-1].ID, nil
}

// GetMinLastAckIdInAllGroup 获取指定 streamId 下所有消费组的最新 ack 消息中最小 id
func (p *RedisClient) GetMinLastAckIdInAllGroup(ctx context.Context, streamId string,
	streamLen int64) (string, int64, error) {

	// 获取当前 stream 中所有消费组
	groups, err := p.RedisClient.XInfoGroups(ctx, streamId).Result()
	if err != nil {
		return "", 0, fmt.Errorf("failed to XInfoGroups, %v", err)
	}

	// 获取当前 stream 中多消费端的最新 ack 消息中最小 id，以及对应消费组 ack 过的消息数量
	minEndMsgId := ""
	ids := make(map[string]int64)
	for _, group := range groups {

		// 计算当前消费组 ack 过的消息数量
		ackCount := streamLen - group.Lag - group.Pending

		// 获取当前消费组 lastDeliveredID 之前的最新 ack 消息 id
		id, err := p.GetLastAckMsgId(ctx, streamId, group.LastDeliveredID, group.Pending)
		if err != nil {
			return "", 0, fmt.Errorf("failed to GetLastAckMsgId, %v", err)
		}

		// 收集 ids
		ids[id] = ackCount
		logx.Infof("[redis-stream][GetMinLastAckIdInAllGroup]group: %s, streamLen: %d, lag: %d, pending: %d, "+
			"ackCount: %d,id: %s", group.Name, streamLen, group.Lag, group.Pending, ackCount, id)

		// 初始化 minEndMsgId
		minEndMsgId = id
	}

	if len(ids) == 0 {
		return "", 0, nil
	}

	// 比较最小 id
	for id := range ids {
		if minEndMsgId > id {
			minEndMsgId = id
		}
	}

	return minEndMsgId, ids[minEndMsgId], nil
}

// GetStickyConns 返回粘性连接conn
func (p *RedisClient) GetStickyConns(ctx context.Context, redisMode string) (map[string]*redis.Conn, error) {
	redisNode := p.RedisClient
	switch redisMode {
	case RedisTypeNode, RedisTypeSentinel:
		client, ok := redisNode.(*redis.Client)
		if !ok || client == nil {
			return nil, fmt.Errorf("failed to get redis client")
		}
		return map[string]*redis.Conn{client.Options().Addr: client.Conn()}, nil
	case RedisTypeCluster:
		clusterClient, ok := redisNode.(*redis.ClusterClient)
		if !ok || clusterClient == nil {
			return nil, fmt.Errorf("failed to get redis client")
		}

		var mu sync.Mutex
		conns := make(map[string]*redis.Conn)
		err := clusterClient.ForEachMaster(ctx, func(ctx context.Context, client *redis.Client) error {
			mu.Lock()
			conns[client.Options().Addr] = client.Conn()
			mu.Unlock()
			return nil
		})

		if err != nil {
			return nil, fmt.Errorf("failed to get cluster masters, %v", err)
		}
		return conns, nil
	default:
		return nil, fmt.Errorf("unsupported redis mode")
	}
}

// GetClusterTopology 获取cluster 拓扑信息
func (p *RedisClient) GetClusterTopology(ctx context.Context) ([]redis.ClusterSlot, error) {
	clusterClient, ok := p.RedisClient.(*redis.ClusterClient)
	if !ok {
		return nil, fmt.Errorf("failed to get redis client")
	}
	return clusterClient.ClusterSlots(ctx).Result()
}

func XAddEventAndSetBlockHeightInPipelineByConnect(ctx context.Context, conn *redis.Conn,
	event *common.ContractEventInfo, sid, topic, heightKey string,
	contractType, partition int) error {

	// 获取实际的 streamId 和要写入的 msg
	streamId, msg, err := GetStreamIdAndMSg(contractType, partition, sid, topic, event)
	if err != nil {
		return fmt.Errorf("failed to GetStreamIdAndMSg, %v", err)
	}

	// 打开 pipeline
	// 真正的执行发生在 pipeline.Exec(ctx) 无法提前检查捕获错误
	pipeline := conn.Pipeline()

	// 提交事件写入操作
	pipeline.XAdd(ctx, &redis.XAddArgs{
		Stream: streamId,
		Values: map[string]interface{}{RedisEventKey: msg},
	})

	// 提交块高更新操作
	pipeline.Set(ctx, heightKey, strconv.FormatUint(event.BlockHeight, 10), 0)

	// 提交 pipeline，此时 redis client 会将收集的多个指令一起发送到 redis server 端去执行
	_, err = pipeline.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to exec pipeline, %v", err)
	}

	logx.Debugf("success to xadd event and set block height in pipeline, "+
		"streamId: %s, height: %d", streamId, event.BlockHeight)
	return nil
}

// XAddEventAndSetBlockHeightForClusterMultiPartition 集群多分区写入事件和设置块高
func XAddEventAndSetBlockHeightForClusterMultiPartition(ctx context.Context,
	event *common.ContractEventInfo, sid, topic string,
	contractType, partition int, clusterTopology []redis.ClusterSlot,
	conns map[string]*redis.Conn, heightKeys map[string]string) error {
	// 获取实际的 streamId 和要写入的 msg
	streamId, msg, err := GetStreamIdAndMSg(contractType, partition, sid, topic, event)
	if err != nil {
		return fmt.Errorf("failed to GetStreamIdAndMSg, %v", err)
	}

	streamSlot := GetSlot(streamId)
	// 找到负责该 Slot 的 Master 节点地址
	var targetAddr string
	for _, slotInfo := range clusterTopology {
		// 检查 Slot 是否在当前分片的范围内
		if int64(slotInfo.Start) <= streamSlot && streamSlot <= int64(slotInfo.End) {
			if len(slotInfo.Nodes) > 0 {
				targetAddr = slotInfo.Nodes[0].Addr
			}
			break
		}
	}

	targetConn, ok := conns[targetAddr]
	if !ok {
		return fmt.Errorf("failed to get targetConn")
	}

	targetHeightKey, ok := heightKeys[targetAddr]
	if !ok {
		// 分片中未设置对应的高度key 仍然写入事件 通过返回错误触发重试
		_, err = targetConn.XAdd(ctx, &redis.XAddArgs{
			Stream: streamId,
			Values: map[string]interface{}{RedisEventKey: msg},
		}).Result()
		if err != nil {
			return fmt.Errorf("failed to xadd event when targetHeightKey not found, %v", err)
		}
		return fmt.Errorf("failed to get targetHeightKey")
	}

	// 打开 pipeline
	// 真正的执行发生在 pipeline.Exec(ctx) 无法提前检查捕获错误
	pipeline := targetConn.Pipeline()

	// 提交事件写入操作
	pipeline.XAdd(ctx, &redis.XAddArgs{
		Stream: streamId,
		Values: map[string]interface{}{RedisEventKey: msg},
	})

	// 提交块高更新操作
	pipeline.Set(ctx, targetHeightKey, strconv.FormatUint(event.BlockHeight, 10), 0)

	// 提交 pipeline，此时 redis conn 会将收集的多个指令一起发送到 redis server 端去执行
	_, err = pipeline.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to exec pipeline, %v", err)
	}

	//// 其余主节点同步块高
	//for addr, conn := range conns {
	//	if addr == targetAddr {
	//		continue
	//	}
	//
	//	heightKey, ok := heightKeys[addr]
	//	if !ok {
	//		return fmt.Errorf("failed to get height key for other node")
	//	}
	//
	//	err = conn.Set(ctx, heightKey,
	//		strconv.FormatUint(event.BlockHeight, 10), 0).Err()
	//	if err != nil {
	//		return fmt.Errorf("failed to Set block height in other node, %v", err)
	//	}
	//}

	return nil
}

func GetSlot(key string) int64 {
	start := strings.IndexByte(key, '{')
	if start > -1 {
		end := strings.IndexByte(key[start+1:], '}')
		if end > -1 {
			end += start + 1
			if end > start+1 {
				key = key[start+1 : end]
			}
		}
	}
	return int64(crc16.Crc16([]byte(key)) % 16384)
}

// GetStreamIdAndMSg 解析获取 streamId 和写入 redis 的 msg 内容
func GetStreamIdAndMSg(contractType, partition int, sid, topic string, event *common.ContractEventInfo) (
	streamId string, msg []byte, err error) {
	//var getStreamIdAndMSg getStreamIdAndMSg
	switch contractType {

	// 使用定业务的解析方式
	//case 2, 3, 4, 5, 13:
	//	getStreamIdAndMSg = business.GetStreamKeyAndMsg

	// 否则都按默认的 streamId 和消息返回
	default:

		// 调用默认的 DefaultGetStreamIdAndMSg 方法解析
		streamId, msg, err = DefaultGetStreamIdAndMSg(sid, topic, event)
		if err != nil {
			return "", nil, err
		}

		return streamId, msg, nil
	}

	// 按特定业务生成 streamId 和 msg
	//streamId, msg, err = getStreamIdAndMSg(event, partition)
	//if err != nil {
	//	return "", nil, err
	//}

	//return streamId, msg, nil
}

// GetStreamIdAndMSg 各业务模块标准的获取 streamId 和 msg 的接口
//
//	@Description:
//	@param event 链上事件
//	@param partition 分区数，传入非0需要实现分区写入和消费，0则不需要
//	@return streamId 写入 redis 的 key
//	@return msg 写入 redis 的 msg 内容
//	@return err 返回错误
type getStreamIdAndMSg func(event *common.ContractEventInfo, partition int) (streamId string, msg []byte, err error)

// DefaultGetStreamIdAndMSg 默认的获取 streamId 和 msg 的接口
func DefaultGetStreamIdAndMSg(sid, topic string, event *common.ContractEventInfo) (
	streamId string, msg []byte, err error) {

	// json 序列化 event 结构
	msg, err = json.Marshal(event)
	if err != nil {
		return "", nil, fmt.Errorf("failed to marshal event, %v", err)
	}

	streamId = strings.Join([]string{sid, event.ContractName}, "#")
	if topic != "" {
		streamId = strings.Join([]string{streamId, topic}, "#")
	}
	return
}
