package agg

import "testing"

// FormatBurstSummary tiers on Max with a side reading of Ten99 +
// HundredUp for the app-impacting count. The tiers exist so the live
// TUI and the saved text report present the same plain-English line
// to the user; this test pins each tier's wording.
func TestFormatBurstSummaryTiers(t *testing.T) {
	cases := []struct {
		name                 string
		ten99, hundredUp, max int64
		want                 string
	}{
		{"no loss runs", 0, 0, 0, "none"},
		{"single drop", 0, 0, 1, "worst 1 packets (isolated losses)"},
		{"max two", 0, 0, 2, "worst 2 packets (isolated losses)"},
		{"short burst", 0, 0, 7, "worst 7 packets, no app-impacting runs"},
		{"app impacting", 4, 0, 33, "worst 33 packets · 4 runs ≥10 (likely affects VoIP/video)"},
		{"long burst", 4, 1, 137, "worst 137 packets · 5 runs ≥10 (likely affects VoIP/video)"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := FormatBurstSummary(tc.ten99, tc.hundredUp, tc.max)
			if got != tc.want {
				t.Errorf("FormatBurstSummary() = %q\n  want %q", got, tc.want)
			}
		})
	}
}

func TestFormatRate(t *testing.T) {
	cases := []struct {
		in   int64
		want string
	}{
		{0, "0 bps"},
		{750, "750 bps"},
		{1_500, "1.50 kbps"},
		{2_500_000, "2.50 Mbps"},
		{3_200_000_000, "3.20 Gbps"},
	}
	for _, c := range cases {
		if got := FormatRate(c.in); got != c.want {
			t.Errorf("FormatRate(%d) = %q, want %q", c.in, got, c.want)
		}
	}
}
