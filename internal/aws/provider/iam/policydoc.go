package iam

import (
	"encoding/json"
	"fmt"
)

type policyDoc struct {
	Version   string       `json:"Version"`
	Statement []policyStmt `json:"Statement"`
}

type policyStmt struct {
	Sid          string `json:"Sid,omitempty"`
	Effect       string `json:"Effect"`
	Action       any    `json:"Action,omitempty"`
	NotAction    any    `json:"NotAction,omitempty"`
	Resource     any    `json:"Resource,omitempty"`
	NotResource  any    `json:"NotResource,omitempty"`
	Principal    any    `json:"Principal,omitempty"`
	NotPrincipal any    `json:"NotPrincipal,omitempty"`
	Condition    any    `json:"Condition,omitempty"`
}

// ValidatePolicyDocument returns a MalformedPolicyDocument error or nil.
func ValidatePolicyDocument(doc string) error {
	if doc == "" {
		return nil // allow empty (some callers pass empty)
	}
	var pd policyDoc
	if err := json.Unmarshal([]byte(doc), &pd); err != nil {
		return fmt.Errorf("MalformedPolicyDocument: %s", err.Error())
	}
	for i, s := range pd.Statement {
		if s.Effect != "Allow" && s.Effect != "Deny" {
			return fmt.Errorf("MalformedPolicyDocument: Statement[%d] Effect must be Allow or Deny, got %q", i, s.Effect)
		}
		hasAction := s.Action != nil
		hasNotAction := s.NotAction != nil
		if !hasAction && !hasNotAction {
			return fmt.Errorf("MalformedPolicyDocument: Statement[%d] must have Action or NotAction", i)
		}
		if hasAction && hasNotAction {
			return fmt.Errorf("MalformedPolicyDocument: Statement[%d] cannot have both Action and NotAction", i)
		}
	}
	return nil
}
