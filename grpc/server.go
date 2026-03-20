// Package grpc provides ...
package grpc

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

// initServerOptions 初始化 grpc 服务设置
//
//	@Description:
//	@param caCertFile
//	@param serverCertFile
//	@param serverKeyFile
//	@param maxRecvMsgSize
//	@param maxSendMsgSize
//	@return []grpc.ServerOption
//	@return error
func initServerOptions(caCertFile, serverCertFile, serverKeyFile string, maxRecvMsgSize,
	maxSendMsgSize int) ([]grpc.ServerOption, error) {
	opts := []grpc.ServerOption{

		// 服务端的强制策略，将会关闭违反该策略的客户端连接
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{

			// client 在发送 ping 检查之前的最短等待时间，也就是 client 那边的 Time 参数要设置的比这个大，否则会因为违反策略而被强制关闭连接。默认 5m
			MinTime: 5 * time.Second,

			// true 表示在没有实际数据流传输时也允许 client 来 ping，false 表示没有数据流传输时不允许 client 来 ping，否则会因为违反策略而被强制关闭连接。默认 false
			PermitWithoutStream: true,
		}),

		// 心跳相关设置
		grpc.KeepaliveParams(keepalive.ServerParameters{

			// 设置最大空闲时间，即空闲一段时间之后会被自动关闭掉
			MaxConnectionIdle: 15 * time.Second,

			// NOTE: 这个参数谨慎设置，主要是为了防止客户端连接爆炸问题，但是限制了服务端长连接返回，比如持续健康检查时，服务端需要不断返回
			// 设置连接保持的最长时长，系统内部会按照该值的 +/- 10% 动态调整，以此降低连接风暴
			//MaxConnectionAge: 60 * time.Second,

			// 是 MaxConnectionAge 之后的附加时间段，在此之后连接将被强制关闭，也就是优雅关闭延长的时间。
			MaxConnectionAgeGrace: 5 * time.Second,

			// 设置 ping 时间间隔，即如果这段时间中还没收到 client 的通信，server 会主动去 ping 客户端检查是否还在线，类似 client 那边的连接参数。最小 1s，默认是 2h
			//Time: 10 * time.Second,

			// 设置 ping 的超时时间，默认 20s
			//Timeout: 5 * time.Second,
		}),

		// 拦截器, 对于每个 inbound 都进行 token 检验
		//grpc.UnaryInterceptor(ensureValidToken),

		// 接收消息大小设置为 20 Mb
		grpc.MaxRecvMsgSize(maxRecvMsgSize),

		// 发送消息大小设置为 20 Mb
		grpc.MaxSendMsgSize(maxSendMsgSize),
	}

	// 配置 tls
	if caCertFile != "" && serverCertFile != "" && serverKeyFile != "" {

		// 加载 server tls 证书
		cert, err := tls.LoadX509KeyPair(serverCertFile, serverKeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load key pair: %v ", err)
		}

		certPool := x509.NewCertPool()
		ca, err := os.ReadFile(caCertFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read ca cert file: %v ", err)
		}

		// 只能解析 pem 类型的根证书，所以需要的是 ca.pem
		certPool.AppendCertsFromPEM(ca)
		creds := credentials.NewTLS(&tls.Config{ // nolint:gosec
			Certificates: []tls.Certificate{cert},        //服务端证书链，可以有多个
			ClientAuth:   tls.RequireAndVerifyClientCert, //要求必须验证客户端证书
			ClientCAs:    certPool,                       //设置根证书的集合，校验方式使用 ClientAuth 中设定的模式

		})

		// 开启服务端 tls 认证
		opts = append(opts, grpc.Creds(creds))
	}

	return opts, nil
}

// CreateGRPCServer  创建一个 gRPC 服务端
//
//	@Description:
//	@param c
//	@param register
//	@param caCertFile
//	@param serverCertFile
//	@param serverKeyFile
//	@param maxRecvMsgSize
//	@param maxSendMsgSize
//	@return *zrpc.RpcServer
//	@return error
func CreateGRPCServer(c zrpc.RpcServerConf, register func(*grpc.Server), caCertFile, serverCertFile,
	serverKeyFile string, maxRecvMsgSize, maxSendMsgSize int) (*zrpc.RpcServer, error) {
	s := zrpc.MustNewServer(c, register)
	opts, err := initServerOptions(caCertFile, serverCertFile, serverKeyFile, maxRecvMsgSize, maxSendMsgSize)
	if err != nil {
		return nil, err
	}

	s.AddOptions(opts...)
	return s, nil
}
