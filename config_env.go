package psqlmanager

import "os"

var settingAlias = map[string]string{
	"dbname": "database",
}

var envToSettingList = []struct {
	env     string
	setting string
}{
	{"DB_HOST", "host"},
	{"POSTGRES_HOST", "host"},
	{"PGHOST", "host"},
	{"DB_PORT", "port"},
	{"POSTGRES_PORT", "port"},
	{"PGPORT", "port"},
	{"DB_DATABASE", "database"},
	{"POSTGRES_DATABASE", "database"},
	{"PGDATABASE", "database"},
	{"DB_USER", "user"},
	{"POSTGRES_USER", "user"},
	{"PGUSER", "user"},
	{"DB_USERNAME", "user"},
	{"POSTGRES_USERNAME", "user"},
	{"DB_PASSWORD", "password"},
	{"PGPASSWORD", "password"},
	{"PGPASSFILE", "passfile"},
	{"PGAPPNAME", "application_name"},
	{"PGCONNECT_TIMEOUT", "connect_timeout"},
	{"DB_SSL", "sslmode"},
	{"POSTGRES_SSL", "sslmode"},
	{"PGSSLMODE", "sslmode"},
	{"PGSSLKEY", "sslkey"},
	{"PGSSLCERT", "sslcert"},
	{"PGSSLSNI", "sslsni"},
	{"PGSSLROOTCERT", "sslrootcert"},
	{"PGSSLPASSWORD", "sslpassword"},
	{"PGTARGETSESSIONATTRS", "target_session_attrs"},
	{"PGSERVICE", "service"},
	{"PGSERVICEFILE", "servicefile"},
}

func (c *ConnString) LoadEnvSettings() {
	for _, val := range envToSettingList {
		value := os.Getenv(val.env)
		if value != "" {
			c.settings[val.setting] = value
		}
	}
}

func (c *ConnString) Env() []string {
	var res []string
	for _, pair := range envToSettingList {
		if val, present := c.settings[pair.setting]; present {
			res = append(res, pair.env+"="+val)
		}
	}
	return res
}
