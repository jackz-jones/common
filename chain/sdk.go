package chain

import (
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"

	"chainmaker.org/chainmaker/common/v2/crypto"
	"chainmaker.org/chainmaker/common/v2/crypto/x509"
	"chainmaker.org/chainmaker/common/v2/log"
	chainmakersdk "chainmaker.org/chainmaker/sdk-go/v2"
	"go.uber.org/zap"
)

// NodeConf 节点配置
type NodeConf struct {
	Url         string
	EnableTls   bool
	TlsHostName string
	CaCert      string

	// TLS chain hostname(如果是网关，tlsHostName是网关，当前为链的配置nginx的寻址地址)
	ChainTlsHostName string
}

// createSDKClient 创建 sdk client
//
//	@Description:
//	@param nodeConfs
//	@param chainId
//	@param chainMode
//	@param userKey
//	@param userCert
//	@param hashMethod
//	@return *chainmaker_sdk_go.ChainClient
//	@return error
func createSDKClient(nodeConfs []NodeConf, chainId, chainMode, signKey, signCert,
	hashMethod, orgId, userTlsCert, userTlsKey, userEncCert, userEncKey,
	logPath, proxyUrl, logLevel string, maxAge int) (sdkClient *chainmakersdk.ChainClient, err error) {
	rpcConf := chainmakersdk.NewRPCClientConfig(
		chainmakersdk.WithRPCClientGetTxTimeout(60),
		chainmakersdk.WithRPCClientSendTxTimeout(60),
		chainmakersdk.WithRPCClientMaxReceiveMessageSize(100),
		chainmakersdk.WithRPCClientMaxSendMessageSize(100),
	)

	cryptoConf := chainmakersdk.NewCryptoConfig(
		chainmakersdk.WithHashAlgo(hashMethod),
	)

	sdkClientOpts := []chainmakersdk.ChainClientOption{
		chainmakersdk.WithChainClientChainId(chainId),
		chainmakersdk.WithCryptoConfig(cryptoConf),
		chainmakersdk.WithHashType(hashMethod),

		// 这两个参数主要是用来轮训 getTxById 的，如果设置就容易走 getTxById 轮训交易结果流程，缺少私钥就会报错
		//chainmakersdk.WithRetryInterval(500),
		//chainmakersdk.WithRetryLimit(20),
		chainmakersdk.WithEnableNormalKey(false),
		chainmakersdk.WithRPCClientConfig(rpcConf),

		// sdk 日志设置
		chainmakersdk.WithChainClientLogger(getDefaultLogger(logPath, logLevel, maxAge)),

		// 配置代理，如果传了正确地址才有，没传则不走代理
		chainmakersdk.WithProxy(proxyUrl),
	}

	// 添加多个 node 配置
	for _, n := range nodeConfs {

		// 解码每个节点的 ca cert
		caStr, err2 := base64.StdEncoding.DecodeString(n.CaCert)
		if err2 != nil {
			return nil, fmt.Errorf("failed to base64 decode caCert,err: %v", err2)
		}

		nodeConf := chainmakersdk.NewNodeConfig(
			chainmakersdk.WithNodeAddr(n.Url),
			chainmakersdk.WithNodeConnCnt(10),
			chainmakersdk.WithNodeUseTLS(n.EnableTls),
			chainmakersdk.WithNodeTLSHostName(n.TlsHostName),

			// 如果是开启 tls，必须传 CaCert
			chainmakersdk.WithNodeCACerts([]string{string(caStr)}),

			// 如果是网关，tlsHostName是网关，当前为链的配置nginx的寻址地址
			chainmakersdk.WithNodeChainTLSHostName(n.ChainTlsHostName),
		)

		sdkClientOpts = append(sdkClientOpts, chainmakersdk.AddChainClientNodeConfig(nodeConf))
	}

	key, err := base64.StdEncoding.DecodeString(signKey)
	if err != nil {
		return nil, fmt.Errorf("failed to base64 decode signKey,err: %v", err)
	}

	// 解析 tls 证书和私钥
	userKey, err1 := base64.StdEncoding.DecodeString(userTlsKey)
	if err1 != nil {
		return nil, fmt.Errorf("failed to base64 decode userTlsKey,err: %v", err1)
	}

	userCert, err1 := base64.StdEncoding.DecodeString(userTlsCert)
	if err1 != nil {
		return nil, fmt.Errorf("failed to base64 decode userTlsCert,err: %v", err1)
	}

	// 解析国密双证书和私钥
	userEncKeyRaw, err1 := base64.StdEncoding.DecodeString(userEncKey)
	if err1 != nil {
		return nil, fmt.Errorf("failed to base64 decode userEncKey,err: %v", err1)
	}

	userEncCertRaw, err1 := base64.StdEncoding.DecodeString(userEncCert)
	if err1 != nil {
		return nil, fmt.Errorf("failed to base64 decode userEncCert,err: %v", err1)
	}

	sdkClientOpts = append(sdkClientOpts,

		// 这里是用于签名的
		chainmakersdk.WithUserSignKeyBytes(key),

		// tls，用于验证节点端证书有效性的，并不是签名用的
		chainmakersdk.WithUserKeyBytes(userKey),
		chainmakersdk.WithUserCrtBytes(userCert),

		// 国密双证书和私钥
		chainmakersdk.WithUserEncKeyBytes(userEncKeyRaw),
		chainmakersdk.WithUserEncCrtBytes(userEncCertRaw),
	)

	switch chainMode {
	case KEY_MODE_PK:

		sdkClientOpts = append(sdkClientOpts,
			chainmakersdk.WithAuthType("public"),
		)
	case KEY_MODE_CERT:

		sCert, err1 := base64.StdEncoding.DecodeString(signCert)
		if err1 != nil {
			return nil, fmt.Errorf("failed to base64 decode signCert,err: %v", err1)
		}

		sdkClientOpts = append(sdkClientOpts,
			chainmakersdk.WithAuthType("permissionedwithcert"),
			chainmakersdk.WithChainClientOrgId(orgId),

			// 这里是用于签名的证书
			chainmakersdk.WithUserSignCrtBytes(sCert),
		)
	}

	if signKey != "" {
		sdkClient, err = chainmakersdk.NewChainClient(sdkClientOpts...)
		if err != nil {
			return nil, fmt.Errorf("failed to new sdk client,err: %v", err)
		}
	} else {
		sdkClient, err = chainmakersdk.NewChainClientWithoutKey(sdkClientOpts...)
		if err != nil {
			return nil, fmt.Errorf("failed to new sdk client without sign key,err: %v", err)
		}
	}

	return sdkClient, nil
}

// CreateSDKClient 创建 sdk client
//
//	@Description:
//	@param nodeConfs
//	@param chainId
//	@param chainMode
//	@param userKey
//	@param userCert
//	@param hashMethod
//	@return *chainmaker_sdk_go.ChainClient
//	@return error
func CreateSDKClient(nodeConfs []NodeConf, chainId, chainMode, signKey, signCert,
	hashMethod, orgId, userTlsCert, userTlsKey, userEncCert, userEncKey,
	logPath, proxyUrl, logLevel string, maxAge int) (*chainmakersdk.ChainClient, error) {
	return createSDKClient(nodeConfs, chainId, chainMode, signKey, signCert, hashMethod, orgId,
		userTlsCert, userTlsKey, userEncCert, userEncKey, logPath, proxyUrl, logLevel, maxAge)
}

// CreateSDKClientWithoutKey  创建 sdk client 不带签名 key
//
//	@Description:
//	@param nodeConfs
//	@param chainId
//	@param chainMode
//	@param hashMethod
//	@return *chainmaker_sdk_go.ChainClient
//	@return error
func CreateSDKClientWithoutKey(nodeConfs []NodeConf, chainId,
	chainMode, hashMethod, orgId, userTlsCert, userTlsKey, userEncCert, userEncKey,
	logPath, proxyUrl, logLevel string, maxAge int) (*chainmakersdk.ChainClient, error) {
	return createSDKClient(nodeConfs, chainId, chainMode, "", "", hashMethod,
		orgId, userTlsCert, userTlsKey, userEncCert, userEncKey, logPath, proxyUrl, logLevel, maxAge)
}

// GetOrgId  获取证书中的 org id
//
//	@Description:
//	@param cert
//	@return string
//	@return error
func GetOrgId(cert []byte) (string, error) {
	pemBlock, _ := pem.Decode(cert)
	if pemBlock == nil {
		return "", errors.New("invalid pem format cert")
	}

	c, err := x509.ParseCertificate(pemBlock.Bytes)
	if err != nil {
		return "", fmt.Errorf("failed to x509 parse cert,err: %v", err)
	}

	orgId := ""
	if len(c.Issuer.Organization) > 0 {
		orgId = c.Issuer.Organization[0]
	}

	return orgId, nil
}

func getDefaultLogger(logPath, logLevel string, maxAge int) *zap.SugaredLogger {

	// sdk 日志默认路径
	if logPath == "" {
		logPath = "./logs/sdk.log"
	}

	// sdk 日志默认等级 info
	var level log.LOG_LEVEL
	switch logLevel {
	case "debug":
		level = log.LEVEL_DEBUG
	case "info":
		level = log.LEVEL_INFO
	case "warn":
		level = log.LEVEL_WARN
	case "error":
		level = log.LEVEL_ERROR
	default:
		level = log.LEVEL_INFO
	}

	// sdk 日志默认保留 30 天
	if maxAge <= 0 {
		maxAge = 30
	}

	config := log.LogConfig{
		Module:       "[SDK]",
		LogPath:      logPath,
		LogLevel:     level,
		MaxAge:       maxAge,
		JsonFormat:   false,
		ShowLine:     true,
		LogInConsole: false,
	}

	logger, _ := log.InitSugarLogger(&config)
	return logger
}

// ConvertHashType 将字符串转换成 HashType
func ConvertHashType(hashType string) (crypto.HashType, error) {
	switch hashType {
	case "SM3":
		return crypto.HASH_TYPE_SM3, nil
	case "SHA256":
		return crypto.HASH_TYPE_SHA256, nil
	case "SHA3_256":
		return crypto.HASH_TYPE_SHA3_256, nil
	default:
		return 0, errors.New("unknown hash type")
	}
}
