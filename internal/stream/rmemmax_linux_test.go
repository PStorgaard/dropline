//go:build linux

package stream

import "testing"

func TestParseRmemMax(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want int
		err  bool
	}{
		{"plain", "16777216", 16777216, false},
		{"trailing newline", "16777216\n", 16777216, false},
		{"leading whitespace", "  4096\n", 4096, false},
		{"crlf", "8192\r\n", 8192, false},
		{"empty", "", 0, true},
		{"non-numeric", "not-a-number\n", 0, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := parseRmemMax([]byte(c.in))
			if c.err {
				if err == nil {
					t.Fatalf("want error, got %d", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("err: %v", err)
			}
			if got != c.want {
				t.Errorf("want %d, got %d", c.want, got)
			}
		})
	}
}
