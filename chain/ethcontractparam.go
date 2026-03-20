package chain

import (
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"time"

	"github.com/ethereum/go-ethereum/common"
	nftConst "github.com/jackz-jones/nft-contract-go/const"
	nftTypes "github.com/jackz-jones/nft-contract-go/types"
	notificationConst "github.com/jackz-jones/notification-contract-go/const"
	notificationTypes "github.com/jackz-jones/notification-contract-go/types"
	notificationUtil "github.com/jackz-jones/notification-contract-go/util"
)

// CreateArgs 创建调用以太坊合约的参数，需要根据具体合约方法去构造
func CreateArgs(method string, kvs map[string][]byte) ([]interface{}, error) {

	switch method {

	// notification 合约方法
	case notificationConst.MethodNotifyEnterpriseInfo:
		return createEnterpriseInfoArgs(kvs)
	case notificationConst.MethodNotifyFileInfo:
		return createFileInfoArgs(kvs)
	case notificationConst.MethodCallback:
		return createCallbackArgs(kvs)

	// nft 合约方法
	case nftConst.MethodMint:
		return createMintArgs(kvs)
	case nftConst.MethodTransfer:
		return createTransferArgs(kvs)
	case nftConst.MethodCrossChainTransfer:
		return createCrossChainTransferArgs(kvs)
	case nftConst.MethodCrossChainMint:
		return createCrossChainMintArgs(kvs)
	case nftConst.MethodUpdateCrossChainStatus:
		return createUpdateCrossChainStatusArgs(kvs)
	default:
		return nil, fmt.Errorf("unknown method: %s", method)
	}
}

// createEnterpriseInfoArgs 创建调用以太坊合约企业身份信息参数
func createEnterpriseInfoArgs(kvs map[string][]byte) ([]interface{}, error) {
	var (
		ei             notificationTypes.EnterpriseInfo
		needCrossChain bool
	)

	// 解析 EnterpriseInfo 参数
	for k, v := range kvs {
		switch k {
		case notificationConst.ParamEnterpriseInfo:
			if err := json.Unmarshal(v, &ei); err != nil {
				return nil, fmt.Errorf("failed to unmarshal EnterpriseInfo: %v", err)
			}

		case notificationConst.ParamNeedCrossChain:
			needCrossChain = notificationUtil.BytesToBool(v)
		}
	}

	return []interface{}{ei.ID, common.HexToHash(ei.OriginHash), big.NewInt(time.Now().Unix()),
		common.HexToAddress(ei.Address), ei.Did, needCrossChain}, nil
}

// createFileInfoArgs 创建调用以太坊合约文件信息参数
func createFileInfoArgs(kvs map[string][]byte) ([]interface{}, error) {
	var (
		fi             notificationTypes.FileInfo
		needCrossChain bool
	)

	// 解析 FileInfo 参数
	for k, v := range kvs {
		switch k {
		case notificationConst.ParamFileInfo:
			if err := json.Unmarshal(v, &fi); err != nil {
				return nil, fmt.Errorf("failed to unmarshal FileInfo: %v", err)
			}

		case notificationConst.ParamNeedCrossChain:
			needCrossChain = notificationUtil.BytesToBool(v)
		}
	}

	return []interface{}{fi.ID, common.HexToHash(fi.OriginHash),
		big.NewInt(time.Now().Unix()), uint64(fi.MsgType), needCrossChain}, nil
}

// createCallbackArgs 创建调用以太坊合约回调参数
func createCallbackArgs(kvs map[string][]byte) ([]interface{}, error) {
	var (
		callBackInfo notificationTypes.CallBackInfo
	)

	// 解析 CallBackInfo 参数
	for k, v := range kvs {
		switch k {
		case notificationConst.ParamCallbackInfo:
			if err := json.Unmarshal(v, &callBackInfo); err != nil {
				return nil, fmt.Errorf("failed to unmarshal CallBackInfo: %v", err)
			}
		}
	}

	return []interface{}{callBackInfo.ID, common.HexToHash(callBackInfo.OriginHash),
		big.NewInt(int64(callBackInfo.Code)), callBackInfo.Msg}, nil
}

func createMintArgs(kvs map[string][]byte) ([]interface{}, error) {
	var (
		nftInfo nftTypes.NFTInfo
	)

	// 解析 NFTInfo 参数
	for k, v := range kvs {
		switch k {
		case nftConst.ParamNFTInfo:
			if err := json.Unmarshal(v, &nftInfo); err != nil {
				return nil, fmt.Errorf("failed to unmarshal NFTInfo: %v", err)
			}
		}
	}

	return []interface{}{nftInfo.ID, common.HexToAddress(nftInfo.Owner),
		common.HexToAddress(nftInfo.Holder), common.HexToHash(nftInfo.OriginHash),
		nftInfo.Data, big.NewInt(time.Now().Unix())}, nil
}

func createTransferArgs(kvs map[string][]byte) ([]interface{}, error) {
	var tokenId, from, to string

	// 解析 Transfer 参数
	for k, v := range kvs {
		switch k {
		case nftConst.ParamTokenId:
			tokenId = string(v)
		case nftConst.ParamFrom:
			from = string(v)
		case nftConst.ParamTo:
			to = string(v)
		}
	}

	return []interface{}{common.HexToHash(tokenId), common.HexToAddress(from), common.HexToAddress(to)}, nil
}

func createCrossChainTransferArgs(kvs map[string][]byte) ([]interface{}, error) {
	return createTransferArgs(kvs)
}

func createCrossChainMintArgs(kvs map[string][]byte) ([]interface{}, error) {
	return createMintArgs(kvs)
}

func createUpdateCrossChainStatusArgs(kvs map[string][]byte) ([]interface{}, error) {
	var (
		tokenId string
		state   int64
		err     error
	)

	// 解析 Transfer 参数
	for k, v := range kvs {
		switch k {
		case nftConst.ParamTokenId:
			tokenId = string(v)
		case nftConst.ParamState:
			state, err = strconv.ParseInt(string(v), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("failed to strconv.ParseInt state %s: %v", string(v), err)
			}
		}
	}

	return []interface{}{common.HexToHash(tokenId), big.NewInt(state)}, nil
}
