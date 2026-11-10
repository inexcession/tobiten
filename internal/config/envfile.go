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
		val = val[1 : len(val)-1]
		val = strings.ReplaceAll(val, `\n`, "\n")
		val = strings.ReplaceAll(val, `\t`, "\t")
		val = strings.ReplaceAll(val, `\r`, "\r")
		val = strings.ReplaceAll(val, `\"`, "\"")
		val = strings.ReplaceAll(val, `\\`, "\\")
		return val
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

func loadEnvFile(path string) (map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return parseEnv(f)
}
