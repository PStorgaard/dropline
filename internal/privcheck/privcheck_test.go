package privcheck

import "testing"

func TestRawICMPInvariant(t *testing.T) {
	s := RawICMP()
	if s.OK && s.Reason != "" {
		t.Errorf("OK=true must imply empty Reason, got %q", s.Reason)
	}
	if !s.OK && s.Reason == "" {
		t.Error("OK=false must imply non-empty Reason")
	}
}
