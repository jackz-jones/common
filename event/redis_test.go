// Package event defines the event module
package event

import (
	"context"
	"fmt"
	"testing"
	"time"

	"chainmaker.org/chainmaker/pb-go/v2/common"
	"github.com/stretchr/testify/assert"
)

func TestRedisClient(t *testing.T) {
	ctx := context.Background()
	redisAddr := "192.168.1.135:26379"
	groupName := "testGroup"
	consumerName := "testConsumer"
	chainID := "testChain"
	subChainID := "testSubChain"
	//chainInfoId := 1

	client, err := NewRedisClient(RedisTypeSentinel, redisAddr, "", "", "mymaster")
	assert.NoError(t, err)

	event := &common.ContractEventInfo{
		ChainId: chainID,
	}
	err = client.PublishEventToStream(ctx, event, subChainID, "", 0, 0)
	assert.NoError(t, err)

	err = client.CreateConsumerGroup(ctx, chainID, groupName, consumerName)
	assert.NoError(t, err)

	err = client.CreateConsumerGroup(ctx, chainID, groupName, consumerName)
	assert.NoError(t, err)

	handler := func(event *common.ContractEventInfo) {
		fmt.Println(event.ChainId)
		assert.Equal(t, chainID, event.ChainId)
	}

	go func() {
		err1 := client.SubscribeFromStream(ctx, chainID, subChainID,
			groupName, consumerName, handler, false, 10, 0)
		assert.NoError(t, err1)
	}()

	time.Sleep(1 * time.Second)
}

func TestMinId(t *testing.T) {
	id1 := "1679123456002-0"
	id2 := "1679123456001-0"
	id3 := "1679123456001-1"
	id4 := ""
	assert.Equal(t, true, id1 > id2, "id1>id2")
	assert.Equal(t, true, id1 > id3, "id1>id3")
	assert.Equal(t, true, id1 > id4, "id1>id4")
	assert.Equal(t, true, id2 < id3, "id2<id3")
	assert.Equal(t, true, id2 > id4, "id2>id4")
	assert.Equal(t, true, id3 > id4, "id3>id4")
}
