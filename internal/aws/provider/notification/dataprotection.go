package notification

import (
	"context"
	"encoding/json"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

const rtDataProtection = "sns_data_protection"

func (p *SNSProvider) PutDataProtectionPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "ResourceArn")
	policy := strParam(nr.Params, "DataProtectionPolicy")
	raw, _ := json.Marshal(map[string]string{"policy": policy})
	entry := store.ResourceEntry{Type: rtDataProtection, ID: arn, Data: raw}
	if err := p.resources.Create(ctx, nr.AccountID, nr.Region, entry); err != nil {
		if err == store.ErrAlreadyExists {
			_ = p.resources.Update(ctx, nr.AccountID, nr.Region, entry)
		}
	}
	return provider.OK(map[string]any{}), nil
}

func (p *SNSProvider) GetDataProtectionPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "ResourceArn")
	policy := ""
	if e, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtDataProtection, arn); err == nil {
		var m map[string]string
		if json.Unmarshal(e.Data, &m) == nil {
			policy = m["policy"]
		}
	}
	return provider.OK(map[string]any{"DataProtectionPolicy": policy}), nil
}
