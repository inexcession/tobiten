package config

import (
	"bufio"
	"io"
	"os"
	"strings"
)

// parseEnv reads environment variables from an io.Reader in .env format.
func parseEnv(r io.Reader) (map[string]string, error) {
	result := make(map[string]string)
	scanner := bufio.NewScanner(r)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		if strings.HasPrefix(line, "export ") {
			line = strings.TrimSpace(strings.TrimPrefix(line, "export "))
		}

		eqIdx := strings.Index(line, "=")
		if eqIdx == -1 {
			continue
		}

		key := strings.TrimSpace(line[:eqIdx])
		if key == "" {
			continue
		}

		val := strings.TrimSpace(line[eqIdx+1:])
		val = unquoteEnvVal(val)
		result[key] = val
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func unquoteEnvVal(val string) string {
	if len(val) >= 2 && val[0] == '"' && val[len(val)-1] == '"' {
		return unescapeDoubleQuoted(val[1 : len(val)-1])
	}
	if len(val) >= 2 && val[0] == '\'' && val[len(val)-1] == '\'' {
		return val[1 : len(val)-1]
	}
	// Strip trailing inline comments if unquoted (e.g. VAL=foo # comment)
	if idx := strings.Index(val, " #"); idx != -1 {
		val = strings.TrimSpace(val[:idx])
	}
	return val
}

// unescapeDoubleQuoted unescapes standard escape sequences in a single left-to-right pass
// so that `\\n` yields literal `\n`, not a newline.
func unescapeDoubleQuoted(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+1 < len(s) {
			switch s[i+1] {
			case 'n':
				b.WriteByte('\n')
				i++
			case 't':
				b.WriteByte('\t')
				i++
			case 'r':
				b.WriteByte('\r')
				i++
			case '"':
				b.WriteByte('"')
				i++
			case '\\':
				b.WriteByte('\\')
				i++
			default:
				b.WriteByte('\\')
			}
		} else {
			b.WriteByte(s[i])
		}
	}
	return b.String()
}

func loadEnvFile(path string) (map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return parseEnv(f)
}
