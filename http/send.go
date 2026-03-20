package http

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// SendHttpRequest 发送http请求
func SendHttpRequest(url, method string, params interface{}) ([]byte, error) {

	// 序列化请求参数
	paBytes, err := json.Marshal(params)
	if err != nil {
		return nil, fmt.Errorf("json marshal req params failed: %v", err)
	}

	// 创建请求
	req, err := http.NewRequest(method, url, bytes.NewBuffer(paBytes))
	if err != nil {
		return nil, fmt.Errorf("new http req failed: %v", err)
	}

	// 设置请求头
	req.Header.Set("Content-Type", "application/json")

	// 发送请求
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("send req failed: %v", err)
	}

	// 读取响应
	defer res.Body.Close()
	bBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("read response body failed: %v", err)
	}

	return bBytes, nil
}
