package event

const (
	// RedisTypeCluster redis集群模式
	RedisTypeCluster = "cluster"
	// RedisTypeNode redis单节点模式
	RedisTypeNode = "node"
	// RedisTypeSentinel redis哨兵集群
	RedisTypeSentinel = "sentinel"
)

const (

	// minAckCountThreshold 最小ack计数阈值为 100
	minAckCountThreshold = 100
)
