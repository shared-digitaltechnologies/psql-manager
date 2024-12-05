package psqlmanager

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/user"
	"path/filepath"
	"strings"

	"github.com/shared-digitaltechnologies/psql-manager/db"
	psqlinit "github.com/shared-digitaltechnologies/psql-manager/init"
	psqlmigrate "github.com/shared-digitaltechnologies/psql-manager/migrate"
	psqlseed "github.com/shared-digitaltechnologies/psql-manager/seed"
)

type Config struct {
	ConnString

	DatabaseName string

	InitRunner                psqlinit.Runner
	ownsCurrentInitRepository bool

	SeederRunner                psqlseed.Runner
	ownsCurrentSeederRepository bool

	migrationProviderFactory *psqlmigrate.ProviderFactory
}

var GlobalConfig Config

func init() {
	GlobalConfig.ConnString = NewConnString()
}

func (c *Config) Copy() *Config {
	if c == nil {
		c = &GlobalConfig
	}

	var res Config
	res = *c
	res.ConnString = c.ConnString.Copy()

	if c.migrationProviderFactory != nil {
		res.migrationProviderFactory = c.migrationProviderFactory.Copy()
	}

	if c.ownsCurrentInitRepository {
		res.InitRunner.Repository = c.InitRunner.Repository.Copy()
		res.ownsCurrentInitRepository = true
	}

	if c.ownsCurrentSeederRepository {
		res.SeederRunner.Repository = c.SeederRunner.Repository.Copy()
		res.ownsCurrentSeederRepository = true
	}

	return &res
}

func NewConfig(options ...ConfigOption) (config *Config, err error) {
	config = &Config{
		ConnString: NewConnString(),
	}

	// Apply options
	err = config.extend(options...)
	if err != nil {
		return
	}

	return
}

func (c *Config) TargetDatabase() *db.Database {
	if c == nil {
		c = &GlobalConfig
	}

	name := c.DatabaseName
	if name == "" {
		connConfig, err := c.RootConnConfig()
		if err == nil {
			name = connConfig.Database
		} else {
			name = "postgres"
		}
	}

	return &db.Database{Name: name}
}

type ConnStringType int8

const (
	KeywordValueConnStringType ConnStringType = iota
	URLConnStringType
)

type ConnString struct {
	Type     ConnStringType
	settings map[string]string
}

func NewConnString() ConnString {
	return ConnString{
		settings: defaultSettings(),
	}
}

func (c *ConnString) Copy() (res ConnString) {
	res.Type = c.Type
	res.settings = make(map[string]string)

	for k, v := range c.settings {
		res.settings[k] = v
	}

	return res
}

func (c *ConnString) CopyWith(values map[string]string) ConnString {
	res := c.Copy()
	res.LoadSettings(values)
	return res
}

func (c *ConnString) Set(key string, value string) {
	if k, ok := settingAlias[key]; ok {
		key = k
	}
	c.settings[key] = value
}

func (c *ConnString) Get(key string) (string, bool) {
	if k, ok := settingAlias[key]; ok {
		key = k
	}
	val, present := c.settings[key]
	return val, present
}

func (c *ConnString) LoadSettings(values map[string]string) {
	for k, v := range values {
		c.Set(k, v)
	}
}

func isIPOnly(host string) bool {
	return net.ParseIP(strings.Trim(host, "[]")) != nil || !strings.Contains(host, ":")
}

func (c *ConnString) StringURL() string {
	var res url.URL
	values := make(url.Values)

	specialKeys := map[string]struct{}{
		"user":     {},
		"password": {},
		"host":     {},
		"port":     {},
		"database": {},
	}

	res.Scheme = "postgres"

	user, userOk := c.settings["user"]
	password, passwordOk := c.settings["password"]
	if userOk && passwordOk {
		res.User = url.UserPassword(user, password)
	} else if userOk {
		res.User = url.User(user)
	}

	res.Host = c.settings["host"] + ":" + c.settings["port"]
	res.Path = "/" + c.settings["database"]

	for k, v := range c.settings {
		if _, present := specialKeys[k]; present {
			continue
		}
		values.Set(k, v)
	}

	res.RawQuery = values.Encode()
	return res.String()
}

func (c *ConnString) LoadURL(connString string) error {
	parsedURL, err := url.Parse(connString)
	if err != nil {
		if urlErr := new(url.Error); errors.As(err, &urlErr) {
			return urlErr.Err
		}
		return err
	}

	if parsedURL.User != nil {
		c.settings["user"] = parsedURL.User.Username()
		if password, present := parsedURL.User.Password(); present {
			c.settings["password"] = password
		}
	}
	// Handle multiple host:port's in url.Host by splitting them into host,host,host and port,port,port.
	var hosts []string
	var ports []string
	for _, host := range strings.Split(parsedURL.Host, ",") {
		if host == "" {
			continue
		}
		if isIPOnly(host) {
			hosts = append(hosts, strings.Trim(host, "[]"))
			continue
		}
		h, p, err := net.SplitHostPort(host)
		if err != nil {
			return fmt.Errorf("failed to split host:port in '%s', err: %w", host, err)
		}
		if h != "" {
			hosts = append(hosts, h)
		}
		if p != "" {
			ports = append(ports, p)
		}
	}
	if len(hosts) > 0 {
		c.settings["host"] = strings.Join(hosts, ",")
	}
	if len(ports) > 0 {
		c.settings["port"] = strings.Join(ports, ",")
	}

	database := strings.TrimLeft(parsedURL.Path, "/")
	if database != "" {
		c.settings["database"] = database
	}

	nameMap := map[string]string{
		"dbname": "database",
	}

	for k, v := range parsedURL.Query() {
		if k2, present := nameMap[k]; present {
			k = k2
		}

		c.settings[k] = v[0]
	}

	return nil
}

var asciiSpace = [256]uint8{'\t': 1, '\n': 1, '\v': 1, '\f': 1, '\r': 1, ' ': 1}

func (c *ConnString) StringKeywordValue() string {
	settings := c.settings
	items := make([]string, len(settings))
	i := 0
	for k, val := range settings {
		val = strings.ReplaceAll(val, "\\", "\\\\")
		val = strings.ReplaceAll(val, "'", "\\'")
		if strings.ContainsAny(val, " \t\n\r\v\f") {
			val = "'" + val + "'"
		}
		items[i] = k + "=" + val
		i++
	}

	return strings.Join(items, " ")
}

func defaultSettings() map[string]string {
	settings := make(map[string]string)

	settings["host"] = defaultHost()
	settings["port"] = "5432"
	settings["user"] = "postgres"
	settings["database"] = "postgres"
	settings["target_session_attrs"] = "any"

	return settings
}

func (c *ConnString) LoadDefaultUserSettings() error {
	user, err := user.Current()
	if err == nil {
		c.settings["user"] = user.Username
		c.settings["passfile"] = filepath.Join(user.HomeDir, ".pgpass")
		c.settings["servicefile"] = filepath.Join(user.HomeDir, ".pg_service.conf")
		sslcert := filepath.Join(user.HomeDir, ".postgresql", "postgresql.crt")
		sslkey := filepath.Join(user.HomeDir, ".postgresql", "postgresql.key")
		if _, err := os.Stat(sslcert); err == nil {
			if _, err := os.Stat(sslkey); err == nil {
				// Both the cert and key must be present to use them, or do not use either
				c.settings["sslcert"] = sslcert
				c.settings["sslkey"] = sslkey
			}
		}
		sslrootcert := filepath.Join(user.HomeDir, ".postgresql", "root.crt")
		if _, err := os.Stat(sslrootcert); err == nil {
			c.settings["sslrootcert"] = sslrootcert
		}
	}
	return err
}

func defaultHost() string {
	candidatePaths := []string{
		"/var/run/postgresql", // Debian
		// "/private/tmp",        // OSX - homebrew
		// "/tmp",                // standard PostgreSQL
	}

	for _, path := range candidatePaths {
		if _, err := os.Stat(path); err == nil {
			return path
		}
	}

	return "localhost"
}

func (c *ConnString) Substitute(str string) string {
	for key, val := range c.settings {
		str = strings.ReplaceAll(str, "{"+key+"}", val)
	}

	for alias, key := range settingAlias {
		val := c.settings[key]
		str = strings.ReplaceAll(str, "{"+alias+"}", val)
	}
	kv := c.StringKeywordValue()
	url := c.StringURL()

	str = strings.ReplaceAll(str, "{}", kv)
	str = strings.ReplaceAll(str, "{url}", url)

	return str
}

func (c *ConnString) String() string {
	switch c.Type {
	case URLConnStringType:
		return c.StringURL()
	default:
		return c.StringKeywordValue()
	}
}

func (c *ConnString) LoadConnString(connString string) error {
	if strings.HasPrefix(connString, "postgres://") || strings.HasPrefix(connString, "postgresql://") {
		c.Type = URLConnStringType
		return c.LoadURL(connString)
	} else {
		c.Type = KeywordValueConnStringType
		return c.LoadKeywordValueString(connString)
	}
}

func (c *ConnString) LoadKeywordValueString(s string) error {
	for len(s) > 0 {
		var key, val string
		eqIdx := strings.IndexRune(s, '=')
		if eqIdx < 0 {
			return errors.New("invalid keyword/value")
		}

		key = strings.Trim(s[:eqIdx], " \t\n\r\v\f")
		s = strings.TrimLeft(s[eqIdx+1:], " \t\n\r\v\f")
		if len(s) == 0 {
		} else if s[0] != '\'' {
			end := 0
			for ; end < len(s); end++ {
				if asciiSpace[s[end]] == 1 {
					break
				}
				if s[end] == '\\' {
					end++
					if end == len(s) {
						return errors.New("invalid backslash")
					}
				}
			}
			val = strings.Replace(strings.Replace(s[:end], "\\\\", "\\", -1), "\\'", "'", -1)
			if end == len(s) {
				s = ""
			} else {
				s = s[end+1:]
			}
		} else { // quoted string
			s = s[1:]
			end := 0
			for ; end < len(s); end++ {
				if s[end] == '\'' {
					break
				}
				if s[end] == '\\' {
					end++
				}
			}
			if end == len(s) {
				return errors.New("unterminated quoted string in connection info string")
			}
			val = strings.Replace(strings.Replace(s[:end], "\\\\", "\\", -1), "\\'", "'", -1)
			if end == len(s) {
				s = ""
			} else {
				s = s[end+1:]
			}
		}

		if k, ok := settingAlias[key]; ok {
			key = k
		}

		if key == "" {
			return errors.New("invalid keyword/value")
		}

		c.settings[key] = val
	}

	return nil
}
