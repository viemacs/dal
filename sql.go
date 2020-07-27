package dal

import (
	"fmt"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

// ---- MySQL ----
// Write() can update a row on duplicated key.

var driverName = "mysql"
var queryVersion = "select version();"

func queryInsertOrIgnore(table, idColumn, idField string, tags, placeholders, updates []string, params []interface{}) (query string, args []interface{}) {

	query = fmt.Sprintf(`insert ignore into %s(%s) values(%s);`,
		table,
		strings.Join(tags, ","),
		strings.Join(placeholders, ","),
	)
	args = params
	return
}

func queryInsertOrUpdate(table, idColumn, idField string, tags, placeholders, updates []string, params []interface{}) (query string, args []interface{}) {

	query = fmt.Sprintf(`insert into %s(%s) values(%s) on duplicate key update %s;`,
		table,
		strings.Join(tags, ","),
		strings.Join(placeholders, ","),
		strings.Join(updates, ","),
	)
	args = append(params, params...)
	return
}
