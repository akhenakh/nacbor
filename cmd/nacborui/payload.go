package main

import (
	"encoding/json"
	"regexp"
	"strings"

	"github.com/akhenakh/nacbor/internal/cborcodec"

	"charm.land/lipgloss/v2"
)

var (
	jsonKeyRe  = regexp.MustCompile(`"([^"\\]*(\\.[^"\\]*)*)"\s*:`)
	jsonStrRe  = regexp.MustCompile(`:\s*"([^"\\]*(\\.[^"\\]*)*)"`)
	jsonNumRe  = regexp.MustCompile(`:\s*([-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?)`)
	jsonBoolRe = regexp.MustCompile(`:\s*(true|false|null)`)
)

func colorizeJSON(b []byte) string {
	s := string(b)
	s = jsonKeyRe.ReplaceAllStringFunc(s, func(m string) string {
		parts := strings.SplitN(m, ":", 2)
		return lipgloss.NewStyle().Foreground(lipgloss.Color("36")).Render(parts[0]) + ":" + parts[1]
	})
	s = jsonStrRe.ReplaceAllStringFunc(s, func(m string) string {
		idx := strings.Index(m, `"`)
		if idx == -1 {
			return m
		}
		return m[:idx] + lipgloss.NewStyle().Foreground(lipgloss.Color("42")).Render(m[idx:])
	})
	s = jsonNumRe.ReplaceAllStringFunc(s, func(m string) string {
		idx := strings.IndexAny(m, "0123456789-")
		if idx == -1 {
			return m
		}
		return m[:idx] + lipgloss.NewStyle().Foreground(lipgloss.Color("214")).Render(m[idx:])
	})
	s = jsonBoolRe.ReplaceAllStringFunc(s, func(m string) string {
		idx := strings.IndexAny(m, "tfn")
		if idx == -1 {
			return m
		}
		return m[:idx] + lipgloss.NewStyle().Foreground(lipgloss.Color("205")).Render(m[idx:])
	})
	return s
}

// formatPayload attempts to decode the given bytes as CBOR, then JSON, and
// finally falls back to raw bytes. It returns the detected payload type label
// and a rendered string suitable for display.
func formatPayload(data []byte) (string, string) {
	if len(data) == 0 {
		return "Empty", ""
	}

	if v, err := cborcodec.Decode(data); err == nil {
		if pj, err := json.MarshalIndent(v, "", "  "); err == nil {
			return "CBOR", colorizeJSON(pj)
		}
	}

	if json.Valid(data) {
		var v interface{}
		if err := json.Unmarshal(data, &v); err == nil {
			if pj, err := json.MarshalIndent(v, "", "  "); err == nil {
				return "JSON", colorizeJSON(pj)
			}
		}
	}

	return "Raw", string(data)
}
