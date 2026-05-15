package stack

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

// ParseTemplate parses a CFN template from JSON or YAML bytes.
// YAML short-form intrinsics (!Ref, !GetAtt, !Sub, etc.) are expanded
// to their long-form map equivalents before decoding.
func ParseTemplate(body []byte) (map[string]any, error) {
	trimmed := bytes.TrimLeft(body, " \t\n\r")
	if len(trimmed) > 0 && trimmed[0] == '{' {
		var out map[string]any
		return out, json.Unmarshal(body, &out)
	}
	var root yaml.Node
	if err := yaml.Unmarshal(body, &root); err != nil {
		return nil, fmt.Errorf("cfn: yaml parse: %w", err)
	}
	if root.Kind == yaml.DocumentNode && len(root.Content) > 0 {
		rewriteCFNTags(root.Content[0])
	}
	var out map[string]any
	if err := root.Decode(&out); err != nil {
		return nil, fmt.Errorf("cfn: yaml decode: %w", err)
	}
	return out, nil
}

func rewriteCFNTags(n *yaml.Node) {
	if n == nil {
		return
	}
	for _, c := range n.Content {
		rewriteCFNTags(c)
	}
	tag := strings.TrimPrefix(n.Tag, "!")
	switch tag {
	case "Ref":
		toMapNode(n, "Ref", stringNode(n.Value))
	case "GetAtt":
		parts := strings.SplitN(n.Value, ".", 2)
		if len(parts) == 2 {
			seq := &yaml.Node{Kind: yaml.SequenceNode, Tag: "!!seq",
				Content: []*yaml.Node{stringNode(parts[0]), stringNode(parts[1])}}
			toMapNode(n, "Fn::GetAtt", seq)
		}
	case "Sub":
		toMapNode(n, "Fn::Sub", stringNode(n.Value))
	case "Base64":
		toMapNode(n, "Fn::Base64", stringNode(n.Value))
	case "ImportValue":
		toMapNode(n, "Fn::ImportValue", stringNode(n.Value))
	case "Join", "Select", "Split", "FindInMap", "If",
		"Equals", "Not", "And", "Or", "Cidr":
		// These wrap a sequence; n itself should be a sequence node.
		child := copyNode(n)
		toMapNode(n, "Fn::"+tag, child)
	}
}

func toMapNode(n *yaml.Node, key string, val *yaml.Node) {
	n.Kind = yaml.MappingNode
	n.Tag = "!!map"
	n.Value = ""
	n.Content = []*yaml.Node{stringNode(key), val}
}

func stringNode(v string) *yaml.Node {
	return &yaml.Node{Kind: yaml.ScalarNode, Value: v, Tag: "!!str"}
}

func copyNode(n *yaml.Node) *yaml.Node {
	if n == nil {
		return nil
	}
	out := &yaml.Node{
		Kind:        n.Kind,
		Style:       n.Style,
		Tag:         n.Tag,
		Value:       n.Value,
		Anchor:      n.Anchor,
		Alias:       n.Alias,
		HeadComment: n.HeadComment,
		LineComment:  n.LineComment,
		FootComment: n.FootComment,
		Line:        n.Line,
		Column:      n.Column,
	}
	out.Content = make([]*yaml.Node, len(n.Content))
	for i, c := range n.Content {
		out.Content[i] = copyNode(c)
	}
	return out
}
