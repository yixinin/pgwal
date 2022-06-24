package pgwal

import (
	"fmt"
	"time"
)

var database = "postgres"
var dsn = fmt.Sprintf("postgres://postgres:1234qwer@postgres:5432/%s?sslmode=disable&replication=database", database)

type Options struct {
	Host string
	Port uint32

	User     string
	Password string
	Database string

	AppName string

	ReadTimeout time.Duration
}

func (o Options) Dsn() string {
	if o.Password == "" {
		o.Password = "pwd" // useless
	}
	var dsn = fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable&replication=database",
		o.User, o.Password, o.Host, o.Port, o.Database)
	return dsn
}

func (o Options) PublicationName() string {
	return fmt.Sprintf("%s_%s_pub", o.Database, o.AppName)
}
func (o Options) SlotName() string {
	return fmt.Sprintf("%s_%s_slot", o.Database, o.AppName)
}

func (o Options) PluginArguments() []string {
	return []string{
		"proto_version '1'",
		fmt.Sprintf("publication_names '%s'", o.PublicationName()),
	}
}
