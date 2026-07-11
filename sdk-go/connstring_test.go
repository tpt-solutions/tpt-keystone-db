package keystone

import (
	"testing"
)

func TestParseConnString(t *testing.T) {
	cases := []struct {
		in       string
		wantHost string
		wantPort int
		wantErr  bool
	}{
		{"localhost", "localhost", DefaultPort, false},
		{"localhost:55432", "localhost", 55432, false},
		{"db.example:6543", "db.example", 6543, false},
		{"user@host:5432", "host", 5432, false},
		{"postgres://user@host:5432/mydb", "host", 5432, false},
		{"postgresql://user:secret@host:6543/mydb?sslmode=disable", "host", 6543, false},
		{"host/dbname", "host", DefaultPort, false},
		{"", "", 0, true},
		{":5432", "", 0, true},
		{"host:0", "", 0, true},
		{"host:70000", "", 0, true},
	}
	for _, c := range cases {
		host, port, err := ParseConnString(c.in)
		if c.wantErr {
			if err == nil {
				t.Errorf("ParseConnString(%q): expected error, got (%q,%d)", c.in, host, port)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseConnString(%q): unexpected error %v", c.in, err)
			continue
		}
		if host != c.wantHost || port != c.wantPort {
			t.Errorf("ParseConnString(%q) = (%q,%d), want (%q,%d)", c.in, host, port, c.wantHost, c.wantPort)
		}
	}
}
