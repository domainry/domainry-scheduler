package sqlite

import (
	ormdialect "github.com/domainry/domainry-orm/dialect"
	"github.com/domainry/domainry-scheduler-sdk/modulehost"
)

type Engine struct{ dialect ormdialect.Dialect }

func NewEngine() Engine {
	dialect, _ := ormdialect.New(ormdialect.SQLite)
	return Engine{dialect: dialect}
}

func (engine Engine) Renderer(schema string) modulehost.Dialect {
	return engine.dialect.WithSchema(schema)
}
