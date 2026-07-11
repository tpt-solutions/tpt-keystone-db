package keystone

import (
	"fmt"
	"strconv"
	"strings"
)

// DefaultPort is the port tpt-keystone's Postgres-wire listener uses by
// default (matching the SDK's connect defaults).
const DefaultPort = 5432

// ParseConnString parses a Keystone connection target. It accepts the
// forms:
//
//	host
//	host:port
//	user@host:port
//	postgres://user@host:port/dbname?options...
//	postgresql://user@host:port/dbname?options...
//
// A missing port defaults to DefaultPort. It returns the host and port to
// dial, or an error if the string has no host or an out-of-range port.
func ParseConnString(s string) (host string, port int, err error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", 0, fmt.Errorf("keystone: empty connection string")
	}
	// Drop a scheme such as postgres:// or postgresql://.
	if i := strings.Index(s, "://"); i >= 0 {
		s = s[i+3:]
	}
	// Drop any path or query component after the authority.
	if i := strings.IndexAny(s, "/?"); i >= 0 {
		s = s[:i]
	}
	// Drop leading userinfo ("user@" or "user:pass@").
	if i := strings.LastIndex(s, "@"); i >= 0 {
		s = s[i+1:]
	}
	if s == "" {
		return "", 0, fmt.Errorf("keystone: connection string has no host")
	}

	host = s
	port = DefaultPort
	if i := strings.LastIndex(s, ":"); i >= 0 {
		candidate := s[i+1:]
		if _, e := strconv.Atoi(candidate); e == nil {
			p, e := strconv.Atoi(candidate)
			if e != nil {
				return "", 0, fmt.Errorf("keystone: invalid port %q: %w", candidate, e)
			}
			if p < 1 || p > 65535 {
				return "", 0, fmt.Errorf("keystone: port %d out of range", p)
			}
			host = s[:i]
			port = p
		}
	}
	if host == "" {
		return "", 0, fmt.Errorf("keystone: connection string has no host")
	}
	return host, port, nil
}
