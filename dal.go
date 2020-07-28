// Package dal provides a database access layer.
package dal

import (
	"database/sql"
	"fmt"
	"log"
	"reflect"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

// TODO: Model.Read() is not parallel-able
// Field `Records` contains the latest query results.
type Model struct {
	DriverName     string
	DataSourceName string
	db             *sql.DB
	Records        []interface{}
	rows           [][]interface{}
}

// this maintains only one database access object for every Drivername+DataSourceName
var connections map[string]*sql.DB = make(map[string]*sql.DB)

func (model *Model) init() (err error) {
	driverName := "mysql"
	if model.DriverName != driverName {
		panic(fmt.Sprintf(`driver name (%s) is unknown or does not match "%s"`, model.DriverName, driverName))
	}
	if model.DataSourceName == "" {
		return fmt.Errorf("datasource of model is not set yet")
	}
	key := model.DriverName + model.DataSourceName
	conn, ok := connections[key]
	if ok {
		model.db = conn
		return
	}

	model.db, err = sql.Open(model.DriverName, model.DataSourceName)
	if err != nil {
		return fmt.Errorf("%v\n dal.Init failed to connect database", err)
	}
	connections[key] = model.db
	return
}

func (model Model) SQL(query string) (err error) {
	if model.db == nil {
		if err = model.init(); err != nil {
			return fmt.Errorf("%v\n dal.SQL failed on model.Init", err)
		}
	}
	if _, err = model.db.Exec(query); err != nil {
		return fmt.Errorf("%v\n dal.SQL failed on model.db.Exec", err)
	}
	return
}

// Create does insert-ignore on the given table.
// The values must be a non-empty slice of the same type.
func (model Model) Create(table string, values interface{}) (rowsAffected int64, err error) {
	return model.write(table, values, "Create")
}

// Update does insert-update on the given table.
// The values must be a non-empty slice of the same type.
func (model Model) Update(table string, values interface{}) (rowsAffected int64, err error) {
	return model.write(table, values, "Update")
}

func (model Model) write(table string, values interface{}, mode string) (sumRowsAffected int64, err error) {
	if model.db == nil {
		if err = model.init(); err != nil {
			return sumRowsAffected, fmt.Errorf("%v\n dal.Write failed on model.init", err)
		}
	}

	rows := reflect.ValueOf(values)
	if rows.Kind() != reflect.Slice {
		return sumRowsAffected, fmt.Errorf("dal.%s: `values` is not a slice", mode)
	}
	if rows.Len() < 1 {
		return sumRowsAffected, fmt.Errorf("dal.%s: `values` has NO elements", mode)
	}

	var fields, tags, placeholders, updates []string
	tp := rows.Index(0).Type()
	numField := tp.NumField()
	for u := 0; u < numField; u++ {
		field, tag := tp.Field(u).Name, tp.Field(u).Tag.Get("field")
		if tag == "" {
			tag = field
		}
		fields, tags, placeholders, updates = append(fields, field),
			append(tags, tag), append(placeholders, "?"), append(updates, tag+"=?")
	}

	var query string
	switch mode {
	case "Create": // insert|ignore
		query = fmt.Sprintf(`insert ignore into %s(%s) values(%s);`,
			table,
			strings.Join(tags, ","),
			strings.Join(placeholders, ","),
		)
	case "Update": // insert|update
		query = fmt.Sprintf(`insert into %s(%s) values(%s) on duplicate key update %s;`,
			table,
			strings.Join(tags, ","),
			strings.Join(placeholders, ","),
			strings.Join(updates, ","),
		)
	}

	tx, _ := model.db.Begin()
	for i := 0; i < rows.Len(); i++ {
		stmt, err := tx.Prepare(query)
		if err != nil {
			return sumRowsAffected, fmt.Errorf(
				"%v\n dal.%s failed on transaction.Prepare for %s", err, mode, query)
		}

		row := rows.Index(i)
		var params []interface{}
		for u := 0; u < numField; u++ {
			params = append(params, row.FieldByName(fields[u]))
		}
		args := params // insert|ignore
		if mode == "Update" {
			args = append(args, params) // insert|update
		}
		res, err := stmt.Exec(args...)
		if err != nil {
			return sumRowsAffected, fmt.Errorf(
				"failed to write a record to table %s: %v", table, err)
		}
		rowsAffected, _ := res.RowsAffected()
		sumRowsAffected += rowsAffected // TBConfirm
	}
	if err := tx.Commit(); err != nil {
		return sumRowsAffected, fmt.Errorf(
			"%v\n failed to commit transaction on table %s", err, table)
	}
	return
}

func (model *Model) Read(table string, fields []string, condition string, readType interface{}) (err error) {
	if model.db == nil {
		if err = model.init(); err != nil {
			return fmt.Errorf("%v\n dal.Read failed on model.Init", err)
		}
	}

	// query
	query := fmt.Sprintf("select %s from %s %s", strings.Join(fields, ","), table, condition)
	var rows *sql.Rows
	if rows, err = model.db.Query(query); err != nil {
		return fmt.Errorf("%v\n dal.Read failed on model.db.Query", err)
	}
	defer rows.Close()

	// scan and set rows
	model.rows = [][]interface{}{}
	model.Records = []interface{}{}

	typ := reflect.TypeOf(readType)
	numField := typ.NumField()
	for rows.Next() {
		values := make([]interface{}, numField)
		for i := 0; i < numField; i++ {
			values[i] = reflect.New(reflect.PtrTo(typ.Field(i).Type)).Interface()
		}
		if err := rows.Scan(values...); err != nil {
			log.Println(err)
			continue
			// TODO: put errors into an error slice
		}
		model.rows = append(model.rows, values)

		elem := reflect.New(typ)
		for i := 0; i < numField; i++ {
			elem.Elem().FieldByName(typ.Field(i).Name).Set(reflect.ValueOf(values[i]).Elem().Elem())
		}
		model.Records = append(model.Records, elem.Elem().Interface())
	}
	return nil
}

func (model Model) Cleanup(table, fieldTime string, tm int64) (err error) {
	if model.db == nil {
		if err := model.init(); err != nil {
			panic(fmt.Errorf("%v\n dal.DBInfo failed on model.init", err))
		}
	}

	query, err := model.db.Prepare(fmt.Sprintf("delete from %s where %s < ?;", table, fieldTime))
	if err != nil {
		return fmt.Errorf("%v\n dal.Cleanup failed on model.db.Prepare", err)
	}
	res, err := query.Exec(tm)
	if err != nil {
		return fmt.Errorf("%v\n failed to cleanup outdated records in table %s", err, table)
	}
	rowsAffected, _ := res.RowsAffected()
	fmt.Printf("dal.Cleanup: cleanup %d records from table %s", rowsAffected, table)
	return
}

func (model Model) DBInfo() (info []string) {
	if model.db == nil {
		if err := model.init(); err != nil {
			panic(fmt.Errorf("%v\n dal.DBInfo failed on model.init", err))
		}
	}

	rows, err := model.db.Query("select version();")
	if err != nil {
		panic(err)
		return
	}
	for rows.Next() {
		var ver string
		if err := rows.Scan(&ver); err != nil {
			panic(fmt.Errorf("cannot get database version info, error: %v", err))
		}
		info = append(info, "system db version: "+ver)
	}
	return info
}
