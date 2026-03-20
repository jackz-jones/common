// Package chain defines the constants for chain module
package chain

// key 模式
const (

	// 证书模式
	KEY_MODE_CERT = "KEY_MODE_CERT"

	// 公钥模式
	KEY_MODE_PK = "KEY_MODE_PK"
)

// 链上支持的签名 hash 算法
const (
	HASH_TYPE_SM3      = "SM3"
	HASH_TYPE_SHA256   = "SHA256"
	HASH_TYPE_SHA3_256 = "SHA3_256"
)
