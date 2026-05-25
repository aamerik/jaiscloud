package notification

import (
	"jaiscloud/internal/clock"
)

func buildSNSEnvelope(topicARN, msg, subject, msgID, subARN string, attrs map[string]any) map[string]any {
	env := map[string]any{
		"Type":             "Notification",
		"MessageId":        msgID,
		"TopicArn":         topicARN,
		"Subject":          subject,
		"Message":          msg,
		"Timestamp":        clock.Now().Format("2006-01-02T15:04:05.000Z"),
		"SignatureVersion": "1",
		"Signature":        "EXAMPLE_SIGNATURE_NOT_VERIFIED_BY_EMULATOR",
		"SigningCertURL":   "http://localhost:4566/_jaiscloud/sns/SimpleNotificationService.pem",
		"UnsubscribeURL":   "http://localhost:4566/?Action=Unsubscribe&SubscriptionArn=" + subARN,
	}
	if len(attrs) > 0 {
		env["MessageAttributes"] = attrs
	}
	return env
}
