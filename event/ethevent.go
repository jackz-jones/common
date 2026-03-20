package event

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

// EthEventHandler 以太坊合约事件处理器
type EthEventHandler struct {

	// 合约abi
	ContractAbi abi.ABI
}

func NewEthEventHandler(abiStr string) (*EthEventHandler, error) {
	conABI, err := abi.JSON(strings.NewReader(abiStr))
	if err != nil {
		return nil, fmt.Errorf("failed to abi.JSON in NewEthEventHandler: %v", err)
	}

	return &EthEventHandler{
		ContractAbi: conABI,
	}, nil
}

// EventName 解析事件名称
func (m *EthEventHandler) EventName(vLog types.Log) (string, error) {
	eventID := vLog.Topics[0].String()
	for _, eventInfo := range m.ContractAbi.Events {

		// 检查是否是目标事件
		if eventInfo.ID.String() != eventID {
			continue
		}

		// 返回目标事件类型
		return eventInfo.RawName, nil
	}

	// 未知事件
	return "", fmt.Errorf("unknown eth event id: %s", eventID)
}

// UnpackIntoInterface 解析以太坊事件参数到指定 struct 结构
func (m *EthEventHandler) UnpackIntoInterface(vLog types.Log, result interface{}) error {
	temp, err := m.UnpackIntoMap(vLog)
	if err == nil {
		return fmt.Errorf("failed to UnpackIntoMap: %v", err)
	}

	tempBytes, err := json.Marshal(temp)
	if err != nil {
		return fmt.Errorf("failed to json.Marshal: %v", err)
	}

	err = json.Unmarshal(tempBytes, &result)
	if err == nil {
		return fmt.Errorf("failed to json.Unmarshal: %v", err)
	}

	return nil
}

// UnpackIntoMap 解析以太坊事件参数到 map 结构
func (m *EthEventHandler) UnpackIntoMap(vLog types.Log) (map[string]interface{}, error) {
	eventID := vLog.Topics[0].String()
	for _, eventInfo := range m.ContractAbi.Events {

		// 检查是否是目标事件
		if eventInfo.ID.String() != eventID {
			continue
		}

		// 找到目标事件类型
		result := make(map[string]interface{})

		// 解包事件数据
		err := m.ContractAbi.UnpackIntoMap(result, eventInfo.RawName, vLog.Data)
		if err != nil {
			return nil, fmt.Errorf("failed to contractAbi.UnpackIntoMap: %v", err)
		}

		return result, nil
	}

	return nil, fmt.Errorf("unknown event id: %s", eventID)
}

// EnterpriseNotifiedEvent 企业身份通知事件
type EnterpriseNotifiedEvent struct {
	EnterpriseInfo struct {
		ID                string         `json:"id"`                // 链上唯一 id，企业信息文件 id
		OriginHash        [32]byte       `json:"originHash"`        // 企业信息文件原文 hash 值
		CreatedAt         uint64         `json:"createdAt"`         // 创建时间戳，单位 s
		EnterpriseAddress common.Address `json:"enterpriseAddress"` // 企业链上地址
		Did               string         `json:"did"`               // 企业did
		Sender            common.Address `json:"sender"`            // 创建地址
	} `json:"enterpriseInfo"` // 企业信息
	NeedCrossChain bool `json:"needCrossChain"` // 是否需要跨链
}

// FileNotifiedEvent  提单文件类通知事件
type FileNotifiedEvent struct {
	FileInfo struct {
		ID         string         `json:"id"`         // 链上唯一 id，电子提单草案文件 id
		OriginHash [32]byte       `json:"originHash"` // 原文 hash 值
		MsgType    int            `json:"msgType"`    // 文件消息通知类型
		CreatedAt  uint64         `json:"createdAt"`  // 创建时间戳，单位 s
		Sender     common.Address `json:"sender"`     // 创建地址
	} `json:"fileInfo"` // 文件信息
	NeedCrossChain bool `json:"needCrossChain"` // 是否需要跨链
}

// CallbackEvent 回调消息事件，告诉发起端跨链成功与否
type CallbackEvent struct {
	CallBackInfo struct {
		ID         string         `json:"id"`         // 链上唯一 id，文件 id
		OriginHash [32]byte       `json:"originHash"` // 文件原文 hash 值
		Msg        string         `json:"msg"`        // 如果失败，错误信息是啥
		Code       int            `json:"code"`       // 200000 表示成功，其他表示失败
		Sender     common.Address `json:"sender"`     // 创建地址
		MsgType    int            `json:"msgType"`    // 文件消息通知类型
	} `json:"callbackInfo"` // 回调信息
}

// MintEvent NFT mint 事件
type MintEvent struct {
	NFTInfo struct {
		ID         string         `json:"id"`         // 正式提单源文件id
		NftOwner   common.Address `json:"nftOwner"`   // 出口企业或进口企业地址
		Holder     common.Address `json:"holder"`     // 承运人地址
		Sender     common.Address `json:"sender"`     // 目标链交易签名者地址
		TokenId    [32]byte       `json:"tokenId"`    // 跨链资产id，hash(EBLOriginHash+hash(data))，合约里计算返回
		OriginHash [32]byte       `json:"originHash"` // 正式提单 hash 值
		State      int            `json:"state"`      // 状态，0-解锁状态，1-锁住状态
		Data       string         `json:"data"`       // 附加数据
		CreatedAt  uint64         `json:"createdAt"`  // 创建时间戳，单位 s
	} `json:"nftInfo"` // NFT信息
}

// TransferEvent NFT transfer 事件
type TransferEvent struct {
	MintEvent
	From common.Address `json:"from"`
	To   common.Address `json:"to"`
}

// CrossChainTransferEvent 跨链转移事件
type CrossChainTransferEvent struct {
	TransferEvent
}

// CrossChainMintEvent 跨链 mint 事件
type CrossChainMintEvent struct {
	MintEvent
}

// UpdateCrossChainStatusEvent 更新跨链状态事件
type UpdateCrossChainStatusEvent struct {
	MintEvent
	CrossState int `json:"crossState"` // 跨链状态，0-跨链成功，1-跨链失败
}
