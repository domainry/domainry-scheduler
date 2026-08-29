package http

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	schedulersdk "github.com/domainry/domainry-scheduler-sdk"
	"github.com/domainry/domainry-scheduler-sdk/modulehost"
)

const defaultMaxResponseBytes int64 = 1 << 20

type Client interface {
	Do(*http.Request) (*http.Response, error)
}

type Executor struct {
	connections modulehost.HTTPConnectionProvider
	client      Client
}

func New(connections modulehost.HTTPConnectionProvider, client Client) *Executor {
	if client == nil {
		client = http.DefaultClient
	}
	return &Executor{connections: connections, client: client}
}

type ExecutionError struct {
	StatusCode int
	Retryable  bool
	Message    string
}

func (e *ExecutionError) Error() string { return e.Message }

func (e *Executor) Dispatch(ctx context.Context, trigger schedulersdk.Trigger) (schedulersdk.DownstreamReceipt, error) {
	connection, err := e.connections.ResolveHTTPConnection(ctx, trigger.Target.ConnectionKey)
	if err != nil {
		return schedulersdk.DownstreamReceipt{}, fmt.Errorf("resolve Scheduler HTTP connection: %w", err)
	}
	operation, found := connection.Operations[trigger.Target.Operation]
	if !found {
		return schedulersdk.DownstreamReceipt{}, fmt.Errorf("Scheduler HTTP operation %q is not published", trigger.Target.Operation)
	}
	endpoint, err := endpointURL(connection.BaseURL, operation.Path)
	if err != nil {
		return schedulersdk.DownstreamReceipt{}, err
	}
	method := strings.ToUpper(strings.TrimSpace(operation.Method))
	if method == "" {
		method = http.MethodPost
	}
	if method != http.MethodPost && method != http.MethodPut && method != http.MethodPatch {
		return schedulersdk.DownstreamReceipt{}, fmt.Errorf("Scheduler HTTP method %q is not allowed", method)
	}
	requestCtx := ctx
	cancel := func() {}
	if operation.Timeout > 0 {
		requestCtx, cancel = context.WithTimeout(ctx, operation.Timeout)
	}
	defer cancel()
	body := []byte(trigger.Target.Payload)
	request, err := http.NewRequestWithContext(requestCtx, method, endpoint, bytes.NewReader(body))
	if err != nil {
		return schedulersdk.DownstreamReceipt{}, fmt.Errorf("create Scheduler HTTP request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", trigger.IdempotencyKey)
	request.Header.Set("X-Domainry-Trigger-ID", trigger.RunID)
	request.Header.Set("X-Domainry-Schedule-Window", trigger.WindowKey)
	for key, value := range operation.Headers {
		if allowedHeader(key) {
			request.Header.Set(key, value)
		}
	}
	if strings.EqualFold(connection.Auth, "hmac_v1") {
		timestamp := strconv.FormatInt(time.Now().UTC().Unix(), 10)
		request.Header.Set("X-Client-ID", connection.ClientID)
		request.Header.Set("X-Timestamp", timestamp)
		request.Header.Set("X-Signature", signature(connection.Secret, connection.ClientID, timestamp, trigger.WindowKey, body))
	}
	response, err := e.client.Do(request)
	if err != nil {
		return schedulersdk.DownstreamReceipt{}, &ExecutionError{Retryable: true, Message: fmt.Sprintf("Scheduler HTTP request failed: %v", err)}
	}
	defer response.Body.Close()
	limit := operation.MaxResponseBytes
	if limit <= 0 || limit > defaultMaxResponseBytes {
		limit = defaultMaxResponseBytes
	}
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, limit+1))
	if err != nil {
		return schedulersdk.DownstreamReceipt{}, &ExecutionError{StatusCode: response.StatusCode, Retryable: true, Message: fmt.Sprintf("read Scheduler HTTP response: %v", err)}
	}
	if int64(len(responseBody)) > limit {
		return schedulersdk.DownstreamReceipt{}, &ExecutionError{StatusCode: response.StatusCode, Message: "Scheduler HTTP response exceeds configured limit"}
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return schedulersdk.DownstreamReceipt{}, &ExecutionError{StatusCode: response.StatusCode, Retryable: retryableStatus(response.StatusCode), Message: fmt.Sprintf("Scheduler HTTP request returned status %d", response.StatusCode)}
	}
	receipt, err := decodeReceipt(trigger, responseBody)
	if err != nil {
		return schedulersdk.DownstreamReceipt{}, &ExecutionError{StatusCode: response.StatusCode, Message: err.Error()}
	}
	return receipt, nil
}

func endpointURL(base, path string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(base))
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return "", fmt.Errorf("Scheduler HTTP connection base URL is invalid")
	}
	relative, err := url.Parse(strings.TrimSpace(path))
	if err != nil || relative.IsAbs() || relative.Host != "" {
		return "", fmt.Errorf("Scheduler HTTP operation path must be relative")
	}
	return parsed.ResolveReference(relative).String(), nil
}

func allowedHeader(key string) bool {
	switch strings.ToLower(strings.TrimSpace(key)) {
	case "host", "authorization", "idempotency-key", "x-client-id", "x-timestamp", "x-signature", "x-domainry-trigger-id", "x-domainry-schedule-window", "content-length":
		return false
	default:
		return true
	}
}

func signature(secret []byte, clientID, timestamp, window string, body []byte) string {
	bodyHash := sha256.Sum256(body)
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(clientID + "\n" + timestamp + "\n" + window + "\n" + hex.EncodeToString(bodyHash[:])))
	return hex.EncodeToString(mac.Sum(nil))
}

func retryableStatus(status int) bool {
	return status == http.StatusTooManyRequests || status == http.StatusBadGateway || status == http.StatusServiceUnavailable || status == http.StatusGatewayTimeout
}

func decodeReceipt(trigger schedulersdk.Trigger, body []byte) (schedulersdk.DownstreamReceipt, error) {
	decoded := struct {
		ReceiptID string `json:"receipt_id"`
		ID        string `json:"id"`
		Status    string `json:"status"`
		Success   *bool  `json:"success"`
		ErrCode   *int64 `json:"errCode"`
		ErrMsg    string `json:"errMsg"`
	}{}
	_ = json.Unmarshal(body, &decoded)
	if decoded.Success != nil && !*decoded.Success {
		return schedulersdk.DownstreamReceipt{}, fmt.Errorf("Scheduler HTTP downstream reported failure")
	}
	if decoded.ErrCode != nil && *decoded.ErrCode != 0 {
		message := strings.TrimSpace(decoded.ErrMsg)
		if message == "" {
			message = fmt.Sprintf("error code %d", *decoded.ErrCode)
		}
		return schedulersdk.DownstreamReceipt{}, fmt.Errorf("Scheduler HTTP downstream reported %s", message)
	}
	id := strings.TrimSpace(decoded.ReceiptID)
	if id == "" {
		id = strings.TrimSpace(decoded.ID)
	}
	if id == "" {
		digest := sha256.Sum256(body)
		id = "http-" + hex.EncodeToString(digest[:12])
	}
	status := strings.TrimSpace(decoded.Status)
	if status == "" {
		status = "accepted"
	}
	return schedulersdk.DownstreamReceipt{ID: id, Owner: "http", Status: status}, nil
}
