// Package grpc provides ...
package grpc

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// CreateGRPCClient  创建一个 gRPC 客户端
//
//	@Description:
//	@param caCertFile
//	@param clientCertFile
//	@param clientKeyFile
//	@param serverDNS
//	@param serverEndpoint
//	@return *zrpc.Client
//	@return error
func CreateGRPCClient(caCertFile, clientCertFile, clientKeyFile, serverDNS,
	serverEndpoint string) (*zrpc.Client, error) {
	opts := make([]zrpc.ClientOption, 0)

	// 配置 tls
	if caCertFile != "" && clientCertFile != "" && clientKeyFile != "" {

		// 加载 client 端证书
		cert, err := tls.LoadX509KeyPair(clientCertFile, clientKeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load client tls cert: %v", err)
		}

		certPool := x509.NewCertPool()
		ca, err := os.ReadFile(caCertFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read ca pem: %v", err)
		}

		certPool.AppendCertsFromPEM(ca)
		creds := credentials.NewTLS(&tls.Config{ // nolint:gosec
			Certificates: []tls.Certificate{cert},

			// 证书配置中的 DNS，指定配置的任一一个即可
			ServerName: serverDNS,
			RootCAs:    certPool,
		})

		opts = append(opts, zrpc.WithDialOption(grpc.WithTransportCredentials(creds)))
	}

	// grpc 通信最大发送和接收数据大小设置为 20mb
	callOpts := zrpc.WithDialOption(grpc.WithDefaultCallOptions(
		grpc.MaxCallRecvMsgSize(20*1024*1024),
		grpc.MaxCallSendMsgSize(20*1024*1024),
	))
	//
	//// 添加 keepalive 参数
	//kaOpts := zrpc.WithDialOption(grpc.WithKeepaliveParams(keepalive.ClientParameters{
	//	Time:                10 * time.Second, // 客户端 ping 服务器的频率
	//	Timeout:             time.Second,      // ping 请求超时时间
	//	PermitWithoutStream: true,             // 允许在没有活动流的情况下发送 ping
	//}))
	//
	//// 添加断线重连参数
	//backoffOpts := zrpc.WithDialOption(grpc.WithConnectParams(grpc.ConnectParams{
	//	Backoff:           backoff.DefaultConfig,
	//	MinConnectTimeout: 20 * time.Second,
	//	// https://github.com/grpc/grpc/blob/master/doc/connection-backoff.md#proposed-backoff-algorithm
	//})) // 设置最大的重连延迟时间

	opts = append(opts, callOpts)

	tlsClient, err := zrpc.NewClient(
		zrpc.RpcClientConf{
			Endpoints: []string{serverEndpoint},
		},
		opts...,
	)
	if err != nil {
		return nil, err
	}

	return &tlsClient, nil
}

func CreateGRPCClientWithTimeout(caCertFile, clientCertFile, clientKeyFile, serverDNS,
	serverEndpoint string, timeout int64) (*zrpc.Client, error) {
	opts := make([]zrpc.ClientOption, 0)

	// 配置 tls
	if caCertFile != "" && clientCertFile != "" && clientKeyFile != "" {

		// 加载 client 端证书
		cert, err := tls.LoadX509KeyPair(clientCertFile, clientKeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load client tls cert: %v", err)
		}

		certPool := x509.NewCertPool()
		ca, err := os.ReadFile(caCertFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read ca pem: %v", err)
		}

		certPool.AppendCertsFromPEM(ca)
		creds := credentials.NewTLS(&tls.Config{ // nolint:gosec
			Certificates: []tls.Certificate{cert},

			// 证书配置中的 DNS，指定配置的任一一个即可
			ServerName: serverDNS,
			RootCAs:    certPool,
		})

		opts = append(opts, zrpc.WithDialOption(grpc.WithTransportCredentials(creds)))
	}

	// grpc 通信最大发送和接收数据大小设置为 20mb
	callOpts := zrpc.WithDialOption(grpc.WithDefaultCallOptions(
		grpc.MaxCallRecvMsgSize(20*1024*1024),
		grpc.MaxCallSendMsgSize(20*1024*1024),
	))
	//
	//// 添加 keepalive 参数
	//kaOpts := zrpc.WithDialOption(grpc.WithKeepaliveParams(keepalive.ClientParameters{
	//	Time:                10 * time.Second, // 客户端 ping 服务器的频率
	//	Timeout:             time.Second,      // ping 请求超时时间
	//	PermitWithoutStream: true,             // 允许在没有活动流的情况下发送 ping
	//}))
	//
	//// 添加断线重连参数
	//backoffOpts := zrpc.WithDialOption(grpc.WithConnectParams(grpc.ConnectParams{
	//	Backoff:           backoff.DefaultConfig,
	//	MinConnectTimeout: 20 * time.Second,
	//	// https://github.com/grpc/grpc/blob/master/doc/connection-backoff.md#proposed-backoff-algorithm
	//})) // 设置最大的重连延迟时间

	opts = append(opts, callOpts)

	tlsClient, err := zrpc.NewClient(
		zrpc.RpcClientConf{
			Endpoints: []string{serverEndpoint},
			Timeout:   timeout,
		},
		opts...,
	)
	if err != nil {
		return nil, err
	}

	return &tlsClient, nil
}
