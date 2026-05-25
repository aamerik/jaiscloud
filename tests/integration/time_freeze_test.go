package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestClock_FixedMode_TimeDoesNotAdvance verifies that in fixed mode clock.Now()
// returns the same timestamp on every call regardless of real wall time passing.
func TestClock_FixedMode_TimeDoesNotAdvance(t *testing.T) {
	resetState(t)
	ctx := context.Background()

	T0 := time.Date(2024, 3, 15, 10, 0, 0, 0, time.UTC)
	FreezeAt(t, T0)

	// GET should reflect the frozen mode and time.
	resp, err := http.Get(jaiscloudEndpoint + "/_jaiscloud/clock")
	require.NoError(t, err)
	var clockResp struct {
		Mode string    `json:"mode"`
		Time time.Time `json:"time"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&clockResp))
	resp.Body.Close()
	assert.Equal(t, "fixed", clockResp.Mode)
	assert.True(t, T0.Equal(clockResp.Time), "expected frozen time %v, got %v", T0, clockResp.Time)

	// Create two resources with real wall time passing between them.
	sqsClient := newSQSClient(t)
	_, err = sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("probe-fixed-1")})
	require.NoError(t, err)

	time.Sleep(250 * time.Millisecond)

	_, err = sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("probe-fixed-2")})
	require.NoError(t, err)

	// Server clock must still return T0 — real time must not have advanced it.
	t1 := clockNow(t)
	time.Sleep(250 * time.Millisecond)
	t2 := clockNow(t)
	assert.Equal(t, T0, t1, "clock.Now() should be frozen at T0")
	assert.Equal(t, T0, t2, "clock.Now() should still be frozen after real time passes")
}

// TestClock_OffsetMode_TimeAdvancesFromBase verifies that in offset mode clock.Now()
// advances at real-wall-clock speed starting from the provided base time.
func TestClock_OffsetMode_TimeAdvancesFromBase(t *testing.T) {
	resetState(t)

	base := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	body, _ := json.Marshal(map[string]any{"mode": "offset", "time": base})
	resp, err := http.Post(jaiscloudEndpoint+"/_jaiscloud/clock", "application/json", bytes.NewReader(body))
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusNoContent, resp.StatusCode)
	t.Cleanup(func() { resetClock(t) })

	c1 := clockNow(t)
	assert.True(t, c1.Equal(base) || c1.After(base), "clock should be at or after base")

	time.Sleep(300 * time.Millisecond)

	c2 := clockNow(t)
	assert.True(t, c2.After(c1), "clock should have advanced with real time")

	elapsed := c2.Sub(c1)
	assert.True(t, elapsed >= 200*time.Millisecond, "elapsed should be >= 200ms, got %v", elapsed)
	assert.True(t, elapsed < 2*time.Second, "elapsed should be < 2s (not frozen), got %v", elapsed)
}

// TestClock_AdminEndpointContract verifies mode transitions, validation,
// and that POST /_jaiscloud/reset restores wall clock.
func TestClock_AdminEndpointContract(t *testing.T) {
	resetState(t)

	mustPost := func(body map[string]any) int {
		b, _ := json.Marshal(body)
		resp, err := http.Post(jaiscloudEndpoint+"/_jaiscloud/clock", "application/json", bytes.NewReader(b))
		require.NoError(t, err)
		resp.Body.Close()
		return resp.StatusCode
	}

	// Initial state after reset is real.
	assert.Equal(t, "real", clockModeFromServer(t))

	// Transition to fixed.
	T0 := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	assert.Equal(t, http.StatusNoContent, mustPost(map[string]any{"mode": "fixed", "time": T0}))
	assert.Equal(t, "fixed", clockModeFromServer(t))
	assert.True(t, T0.Equal(clockNow(t)), "expected frozen time %v, got %v", T0, clockNow(t))

	// Transition back to real.
	assert.Equal(t, http.StatusNoContent, mustPost(map[string]any{"mode": "real"}))
	assert.Equal(t, "real", clockModeFromServer(t))

	// Validation: fixed with no time → 400.
	assert.Equal(t, http.StatusBadRequest, mustPost(map[string]any{"mode": "fixed"}))

	// Validation: fixed with zero time → 400.
	assert.Equal(t, http.StatusBadRequest, mustPost(map[string]any{"mode": "fixed", "time": time.Time{}}))

	// Validation: unknown mode → 400.
	assert.Equal(t, http.StatusBadRequest, mustPost(map[string]any{"mode": "bogus"}))

	// POST /_jaiscloud/reset must restore wall clock.
	mustPost(map[string]any{"mode": "fixed", "time": T0})
	resetState(t)
	assert.Equal(t, "real", clockModeFromServer(t))
}

// TestClock_NoLeakBetweenTests verifies that a frozen clock from sub-test A
// does not leak into sub-test B after FreezeAt's t.Cleanup fires.
func TestClock_NoLeakBetweenTests(t *testing.T) {
	t.Run("A_freeze", func(t *testing.T) {
		resetState(t)
		FreezeAt(t, time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC))
		assert.Equal(t, "fixed", clockModeFromServer(t))
		// t.Cleanup registered by FreezeAt resets clock to "real" when this sub-test ends.
	})

	// B runs after A's cleanup has fired — clock must be back to "real".
	t.Run("B_no_leak", func(t *testing.T) {
		resetState(t)
		assert.Equal(t, "real", clockModeFromServer(t),
			"clock should be wall time — A's freeze must not leak into B")
		now := clockNow(t)
		assert.True(t, now.Year() >= 2024, "clock should be near real wall time, not 2020")
	})
}

// TestClock_STSTokenExpiry verifies AssumeRole credentials carry an Expiration
// timestamp based on the frozen clock, not wall time.
func TestClock_STSTokenExpiry(t *testing.T) {
	resetState(t)
	ctx := context.Background()

	T0 := time.Date(2020, 6, 15, 12, 0, 0, 0, time.UTC)
	FreezeAt(t, T0)

	iamClient := newIAMClient(t)
	roleName := "time-freeze-role"
	_, err := iamClient.CreateRole(ctx, &iam.CreateRoleInput{
		RoleName:                 aws.String(roleName),
		AssumeRolePolicyDocument: aws.String(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}`),
	})
	require.NoError(t, err)

	stsClient := newSTSClient(t)
	roleARN := "arn:aws:iam::000000000000:role/" + roleName
	result, err := stsClient.AssumeRole(ctx, &sts.AssumeRoleInput{
		RoleArn:         aws.String(roleARN),
		RoleSessionName: aws.String("test-session"),
		DurationSeconds: aws.Int32(3600),
	})
	require.NoError(t, err)
	require.NotNil(t, result.Credentials)
	require.NotNil(t, result.Credentials.Expiration)

	expected := T0.Add(3600 * time.Second)
	actual := *result.Credentials.Expiration
	diff := actual.Sub(expected)
	if diff < 0 {
		diff = -diff
	}
	assert.True(t, diff < 2*time.Second,
		"credentials Expiration should be T0+3600s (%v), got %v (diff %v)", expected, actual, diff)
}
