package services_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"jaiscloud/internal/adapter/aws/services"
	"jaiscloud/internal/model"
)

var codec = &services.SQSCodec{}

func jsonRequest(t *testing.T, action string, body map[string]any) *http.Request {
	t.Helper()
	b, _ := json.Marshal(body)
	r := httptest.NewRequest("POST", "/", bytes.NewReader(b))
	r.Header.Set("X-Amz-Target", "AmazonSQS."+action)
	r.Header.Set("Content-Type", "application/x-amz-json-1.0")
	return r
}

func queryRequest(t *testing.T, body string) *http.Request {
	t.Helper()
	r := httptest.NewRequest("POST", "/", strings.NewReader(body))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	return r
}

// ─── Decode tests ─────────────────────────────────────────────────────────────

func TestDecode_JSON_CreateQueue(t *testing.T) {
	r := jsonRequest(t, "CreateQueue", map[string]any{"QueueName": "test-q"})
	nr, err := codec.Decode(r, []byte(`{"QueueName":"test-q"}`))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if nr.Action != "CreateQueue" {
		t.Fatalf("expected action CreateQueue, got %s", nr.Action)
	}
	if nr.Params["QueueName"] != "test-q" {
		t.Fatalf("expected QueueName=test-q, got %v", nr.Params["QueueName"])
	}
	if nr.GetMeta("sqs_protocol") != "json" {
		t.Fatal("expected json protocol")
	}
}

func TestDecode_Query_CreateQueue(t *testing.T) {
	r := queryRequest(t, "Action=CreateQueue&QueueName=test-q&Version=2012-11-05")
	nr, err := codec.Decode(r, []byte("Action=CreateQueue&QueueName=test-q&Version=2012-11-05"))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if nr.Action != "CreateQueue" {
		t.Fatalf("expected CreateQueue, got %s", nr.Action)
	}
	if nr.Params["QueueName"] != "test-q" {
		t.Fatalf("expected QueueName=test-q, got %v", nr.Params["QueueName"])
	}
	if nr.GetMeta("sqs_protocol") != "query" {
		t.Fatal("expected query protocol")
	}
}

func TestDecode_Query_SendMessageBatch_NumberedParams(t *testing.T) {
	body := "Action=SendMessageBatch&QueueUrl=http://localhost:4566/000/q" +
		"&SendMessageBatchRequestEntry.1.Id=m1&SendMessageBatchRequestEntry.1.MessageBody=hello" +
		"&SendMessageBatchRequestEntry.2.Id=m2&SendMessageBatchRequestEntry.2.MessageBody=world"

	r := queryRequest(t, body)
	nr, err := codec.Decode(r, []byte(body))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	entries, ok := nr.Params["Entries"].([]map[string]any)
	if !ok || len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %T %v", nr.Params["Entries"], nr.Params["Entries"])
	}
	if entries[0]["Id"] != "m1" || entries[0]["MessageBody"] != "hello" {
		t.Fatalf("entry[0] mismatch: %v", entries[0])
	}
	if entries[1]["Id"] != "m2" || entries[1]["MessageBody"] != "world" {
		t.Fatalf("entry[1] mismatch: %v", entries[1])
	}
}

func TestDecode_Query_GetQueueAttributes(t *testing.T) {
	body := "Action=GetQueueAttributes&QueueUrl=http://localhost:4566/000/q&AttributeName.1=All"
	r := queryRequest(t, body)
	nr, err := codec.Decode(r, []byte(body))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	names, ok := nr.Params["AttributeNames"].([]string)
	if !ok || len(names) != 1 || names[0] != "All" {
		t.Fatalf("expected AttributeNames=[All], got %v", nr.Params["AttributeNames"])
	}
}

// ─── Encode tests ─────────────────────────────────────────────────────────────

func TestEncode_JSON_CreateQueue(t *testing.T) {
	nr := &model.NormalizedRequest{Action: "CreateQueue"}
	nr.SetMeta("sqs_protocol", "json")
	resp := &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"QueueUrl": "http://localhost:4566/000/q"}}

	status, headers, body := codec.Encode(nr, resp)
	if status != 200 {
		t.Fatalf("expected 200, got %d", status)
	}
	if !strings.Contains(headers.Get("Content-Type"), "json") {
		t.Fatalf("expected JSON content type, got %s", headers.Get("Content-Type"))
	}
	var out map[string]any
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("body not valid JSON: %v", err)
	}
	if out["QueueUrl"] != "http://localhost:4566/000/q" {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestEncode_XML_CreateQueue(t *testing.T) {
	nr := &model.NormalizedRequest{Action: "CreateQueue"}
	nr.SetMeta("sqs_protocol", "query")
	resp := &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"QueueUrl": "http://localhost:4566/000/q"}}

	status, headers, body := codec.Encode(nr, resp)
	if status != 200 {
		t.Fatalf("expected 200, got %d", status)
	}
	if !strings.Contains(headers.Get("Content-Type"), "xml") {
		t.Fatalf("expected XML content type, got %s", headers.Get("Content-Type"))
	}
	if !strings.Contains(string(body), "CreateQueueResponse") {
		t.Fatalf("expected CreateQueueResponse wrapper: %s", body)
	}
	if !strings.Contains(string(body), "http://localhost:4566/000/q") {
		t.Fatalf("URL not in response: %s", body)
	}
}

// ─── EncodeError tests ────────────────────────────────────────────────────────

func TestEncodeError_JSON(t *testing.T) {
	nr := &model.NormalizedRequest{Action: "CreateQueue"}
	nr.SetMeta("sqs_protocol", "json")
	perr := model.NewProviderError("NotFound", "queue does not exist", 400)

	status, headers, body := codec.EncodeError(nr, perr)
	if status != 400 {
		t.Fatalf("expected 400, got %d", status)
	}
	if !strings.Contains(headers.Get("Content-Type"), "json") {
		t.Fatal("expected JSON content type")
	}
	var out map[string]any
	json.Unmarshal(body, &out)
	if !strings.Contains(out["__type"].(string), "NonExistentQueue") {
		t.Fatalf("expected NonExistentQueue error code, got %v", out["__type"])
	}
}

func TestEncodeError_XML(t *testing.T) {
	nr := &model.NormalizedRequest{Action: "CreateQueue"}
	nr.SetMeta("sqs_protocol", "query")
	perr := model.NewProviderError("BatchTooLarge", "too many entries", 400)

	status, _, body := codec.EncodeError(nr, perr)
	if status != 400 {
		t.Fatalf("expected 400, got %d", status)
	}
	if !strings.Contains(string(body), "TooManyEntriesInBatchRequest") {
		t.Fatalf("expected TooManyEntriesInBatchRequest, got %s", body)
	}
}

func TestEncodeError_NilNR(t *testing.T) {
	perr := model.NewProviderError("NotFound", "queue does not exist", 400)
	status, _, body := codec.EncodeError(nil, perr)
	if status != 400 {
		t.Fatalf("expected 400, got %d", status)
	}
	if !strings.Contains(string(body), "NonExistentQueue") {
		t.Fatalf("expected NonExistentQueue, got %s", body)
	}
}
