package event

// CrossChainEvent 跨链协议统一事件结构
type CrossChainEvent struct {
	EventName      string `json:"eventName"`      // 事件名称
	ChainInfoId    uint   `json:"chainInfoId"`    // 链信息ID
	ContractInfoId uint   `json:"contractInfoId"` // 合约信息ID
	EventData      []byte `json:"eventData"`      // 事件数据
}
