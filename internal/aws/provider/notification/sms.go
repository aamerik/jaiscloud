package notification

import (
	"context"
	"encoding/json"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

const (
	rtSMSAttrs  = "sns_sms_attrs"
	rtSMSOptOut = "sns_sms_optout"
	smsAttrKey  = "account"
)

func (p *SNSProvider) SetSMSAttributes(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	attrs := map[string]string{}
	if m, ok := nr.Params["attributes"].(map[string]any); ok {
		for k, v := range m {
			if s, ok := v.(string); ok {
				attrs[k] = s
			}
		}
	}
	raw, _ := json.Marshal(attrs)
	entry := store.ResourceEntry{Type: rtSMSAttrs, ID: smsAttrKey, Data: raw}
	if err := p.resources.Create(ctx, entry); err != nil {
		if err == store.ErrAlreadyExists {
			_ = p.resources.Update(ctx, entry)
		}
	}
	return provider.OK(map[string]any{}), nil
}

func (p *SNSProvider) GetSMSAttributes(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	attrs := map[string]string{}
	if e, err := p.resources.Get(ctx, rtSMSAttrs, smsAttrKey); err == nil {
		json.Unmarshal(e.Data, &attrs)
	}
	return provider.OK(map[string]any{"attributes": attrs}), nil
}

func (p *SNSProvider) OptInPhoneNumber(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	phone := strParam(nr.Params, "phoneNumber")
	_ = p.resources.Delete(ctx, rtSMSOptOut, phone)
	return provider.OK(map[string]any{}), nil
}

func (p *SNSProvider) CheckIfPhoneNumberIsOptedOut(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	phone := strParam(nr.Params, "phoneNumber")
	_, err := p.resources.Get(ctx, rtSMSOptOut, phone)
	isOptedOut := err == nil
	return provider.OK(map[string]any{"isOptedOut": isOptedOut}), nil
}

func (p *SNSProvider) ListPhoneNumbersOptedOut(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, _ := p.resources.List(ctx, rtSMSOptOut, "")
	phones := make([]string, 0, len(entries))
	for _, e := range entries {
		phones = append(phones, e.ID)
	}
	return provider.OK(map[string]any{"phoneNumbers": phones}), nil
}

func (p *SNSProvider) ListOriginationNumbers(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return provider.OK(map[string]any{"phoneNumbers": []any{}}), nil
}
