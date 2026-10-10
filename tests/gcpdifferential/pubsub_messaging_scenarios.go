//go:build gcp_differential

package gcpdifferential

import (
	"fmt"
	"net/http"
	"time"
)

// Pub/Sub messaging-semantics oracle (PSM1). The control-plane CRUD set above
// exercises topic/subscription lifecycle, publish and one drain; these scenarios
// record the contract-bearing data-plane paths that the fidelity matrix marked
// `ga` without a real-GCP transcript: acknowledge, modify-ack-deadline (extend
// then nack/redeliver), the empty-pull response shape, and ordered (keyed)
// delivery. Each resource is created here (captured) and torn down by Cleanup,
// so a recording leaves nothing behind and every golden is scoped to this run.
//
// The block is appended at the very end of Scenarios so the existing golden
// indices (and the control-plane list goldens, which must not see these extra
// resources) are unchanged.
//
// Out of scope (recorded as debt, not faked): real-GCP dead-letter republish is
// timing/delivery-count dependent and the emulator omits the
// CloudPubSubDeadLetterSource* attributes, so it is deferred to its own fix
// (see the plan doc). Exactly-once ack-ID versioning is EOD1 (W1.2).
func pubsubMessagingScenarios(project string, n ResourceNames) []Scenario {
	base := "/v1/projects/" + project
	msgTopic := base + "/topics/" + n.MsgTopic
	msgSub := base + "/subscriptions/" + n.MsgSub
	orderTopic := base + "/topics/" + n.OrderTopic
	orderSub := base + "/subscriptions/" + n.OrderSub
	eodTopic := base + "/topics/" + n.EODTopic
	eodSub := base + "/subscriptions/" + n.EODSub

	// waitPull polls a subscription until at least one message is delivered,
	// then records that response. The ackId is captured for a later
	// ack/modack; the emulator's ack id is delivery-scoped, so a redelivery
	// overwrites the captured value just as real GCP mints a new one.
	waitPull := func(op, sub string) Scenario {
		return Scenario{
			Op: op, Service: "pubsub", Method: http.MethodPost,
			Path: sub + ":pull", Body: `{"maxMessages":1}`,
			Wait: &WaitSpec{Field: "receivedMessages", MinLen: 1, Interval: time.Second, Timeout: 60 * time.Second},
			Save: map[string]string{"ackId": "receivedMessages.0.ackId"},
		}
	}

	return []Scenario{
		// ── Resources (dedicated, so the control-plane CRUD/lists above are
		//    unaffected and the message state is isolated) ────────────────────
		{Op: "msg_topic_create", Service: "pubsub", Method: http.MethodPut, Path: msgTopic,
			Body: `{}`},
		{Op: "msg_sub_create", Service: "pubsub", Method: http.MethodPut, Path: msgSub,
			Body: fmt.Sprintf(`{"topic":%q,"ackDeadlineSeconds":60}`, "projects/"+project+"/topics/"+n.MsgTopic)},

		// ── Acknowledge: publish → pull (ackId) → ack → pull is empty ────────
		// Proves the ack id is accepted and the message is removed. The empty
		// pull also records the response shape (real GCP omits receivedMessages;
		// the emulator returns an empty array — an accepted additive field).
		{Op: "msg_ack_publish", Service: "pubsub", Method: http.MethodPost, Path: msgTopic + ":publish",
			Body: `{"messages":[{"data":"bXNnLWFjaw=="}]}`},
		waitPull("msg_ack_pull", msgSub),
		{Op: "msg_ack_acknowledge", Service: "pubsub", Method: http.MethodPost, Path: msgSub + ":acknowledge",
			Body: `{"ackIds":["${ackId}"]}`},
		{Op: "msg_ack_pull_empty", Service: "pubsub", Method: http.MethodPost, Path: msgSub + ":pull",
			Body: `{"maxMessages":1,"returnImmediately":true}`},

		// ── ModifyAckDeadline: extend keeps it invisible past the original
		//    deadline; nack (0) makes it immediately redeliverable ────────────
		{Op: "msg_modack_publish", Service: "pubsub", Method: http.MethodPost, Path: msgTopic + ":publish",
			Body: `{"messages":[{"data":"bXNnLW1vZGFjaw=="}]}`},
		waitPull("msg_modack_pull", msgSub),
		{Op: "msg_modack_extend", Service: "pubsub", Method: http.MethodPost, Path: msgSub + ":modifyAckDeadline",
			Body: `{"ackIds":["${ackId}"],"ackDeadlineSeconds":600}`},
		{Op: "msg_modack_invisible", Service: "pubsub", Method: http.MethodPost, Path: msgSub + ":pull",
			Body: `{"maxMessages":1,"returnImmediately":true}`},
		{Op: "msg_modack_nack", Service: "pubsub", Method: http.MethodPost, Path: msgSub + ":modifyAckDeadline",
			Body: `{"ackIds":["${ackId}"],"ackDeadlineSeconds":0}`},
		waitPull("msg_modack_redelivered", msgSub),
		{Op: "msg_modack_acknowledge", Service: "pubsub", Method: http.MethodPost, Path: msgSub + ":acknowledge",
			Body: `{"ackIds":["${ackId}"]}`},

		// ── Ordered (keyed) delivery ─────────────────────────────────────────
		// Three messages share an ordering key. Real GCP returns all three in
		// one pull (in publish order) once the subscription has
		// enableMessageOrdering; the emulator's ordering-key gate withholds all
		// but the earliest until it is acked, so the single pull records that
		// divergence. The ack uses the first captured ack id, present on both
		// sides, so the ack response itself is comparable.
		{Op: "order_topic_create", Service: "pubsub", Method: http.MethodPut, Path: orderTopic,
			Body: `{}`},
		{Op: "order_sub_create", Service: "pubsub", Method: http.MethodPut, Path: orderSub,
			Body: fmt.Sprintf(`{"topic":%q,"ackDeadlineSeconds":10,"enableMessageOrdering":true}`, "projects/"+project+"/topics/"+n.OrderTopic)},
		{Op: "order_publish_0", Service: "pubsub", Method: http.MethodPost, Path: orderTopic + ":publish",
			Body: `{"messages":[{"data":"a2V5LTA=","orderingKey":"key"}]}`},
		{Op: "order_publish_1", Service: "pubsub", Method: http.MethodPost, Path: orderTopic + ":publish",
			Body: `{"messages":[{"data":"a2V5LTE=","orderingKey":"key"}]}`},
		{Op: "order_publish_2", Service: "pubsub", Method: http.MethodPost, Path: orderTopic + ":publish",
			Body: `{"messages":[{"data":"a2V5LTI=","orderingKey":"key"}]}`},
		{Op: "order_pull", Service: "pubsub", Method: http.MethodPost, Path: orderSub + ":pull",
			Body: `{"maxMessages":10}`,
			Save: map[string]string{"ackId": "receivedMessages.0.ackId"}},
		{Op: "order_acknowledge", Service: "pubsub", Method: http.MethodPost, Path: orderSub + ":acknowledge",
			Body: `{"ackIds":["${ackId}"]}`},

		// ── Exactly-once acknowledgement-ID versioning (EOD1) ────────────────
		// Real GCP versions the ackId per delivery: on an exactly-once
		// subscription a superseded (nacked-then-redelivered) ackId is
		// INVALID_ARGUMENT carrying a google.rpc.ErrorInfo detail, while the
		// latest id is accepted. The emulator embeds the delivery attempt in the
		// ackId and matches. Two variables are needed because the redelivery
		// mints a new id; the metadata key is that id, folded by the normalizer.
		{Op: "eod_topic_create", Service: "pubsub", Method: http.MethodPut, Path: eodTopic,
			Body: `{}`},
		{Op: "eod_sub_create", Service: "pubsub", Method: http.MethodPut, Path: eodSub,
			Body: fmt.Sprintf(`{"topic":%q,"ackDeadlineSeconds":10,"enableExactlyOnceDelivery":true}`, "projects/"+project+"/topics/"+n.EODTopic)},
		{Op: "eod_publish", Service: "pubsub", Method: http.MethodPost, Path: eodTopic + ":publish",
			Body: `{"messages":[{"data":"Z29k"}]}`},
		{Op: "eod_pull", Service: "pubsub", Method: http.MethodPost, Path: eodSub + ":pull",
			Body: `{"maxMessages":1}`,
			Wait: &WaitSpec{Field: "receivedMessages", MinLen: 1, Interval: time.Second, Timeout: 60 * time.Second},
			Save: map[string]string{"eodAck1": "receivedMessages.0.ackId"}},
		{Op: "eod_nack", Service: "pubsub", Method: http.MethodPost, Path: eodSub + ":modifyAckDeadline",
			Body: `{"ackIds":["${eodAck1}"],"ackDeadlineSeconds":0}`},
		{Op: "eod_repull", Service: "pubsub", Method: http.MethodPost, Path: eodSub + ":pull",
			Body: `{"maxMessages":1}`,
			Wait: &WaitSpec{Field: "receivedMessages", MinLen: 1, Interval: time.Second, Timeout: 60 * time.Second},
			Save: map[string]string{"eodAck2": "receivedMessages.0.ackId"}},
		{Op: "eod_ack_superseded", Service: "pubsub", Method: http.MethodPost, Path: eodSub + ":acknowledge",
			Body: `{"ackIds":["${eodAck1}"]}`},
		{Op: "eod_ack_latest", Service: "pubsub", Method: http.MethodPost, Path: eodSub + ":acknowledge",
			Body: `{"ackIds":["${eodAck2}"]}`},
	}
}
