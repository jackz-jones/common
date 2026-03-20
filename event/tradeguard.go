package event

// TradeGuardEvent
type TradeGuardEvent struct {
	EventName    string `json:"eventName"`    // 事件名称
	ChainType    string `json:"chainType"`    // 链类型
	ChainName    string `json:"chainName"`    // 链名称
	ContractType string `json:"contractType"` // 合约类型
	ContractName string `json:"contractName"` // 合约名称
	EventData    []byte `json:"eventData"`    // 事件数据
}
