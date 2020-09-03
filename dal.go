// Package dal provides a database access layer.
package dal

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

// TODO: Model.Read() is not parallel-able
// Field `Records` contains the latest query results.
// Default batch size is 4K, if record-size is 4K, the max_allowed_package 16M is reached.
type Model struct {
	DriverName     string
	DataSourceName string
	BatchSize      int
	Records        []interface{}
	rows           [][]interface{}
}

// `connections` maintains only one database access object for every Drivername+DataSourceName
var connections map[string]*sql.DB = make(map[string]*sql.DB)

func (model *Model) getConn() (conn *sql.DB, err error) {
	driverName := "mysql"
	if model.DriverName != driverName {
		return conn, fmt.Errorf(`model.driver name "%s" is not "%s"`, model.DriverName, driverName)
	}
	if model.DataSourceName == "" {
		return conn, fmt.Errorf("model.datasource is empty")
	}

	if model.BatchSize == 0 {
		model.BatchSize = 1 << 12 // 4k
	} else if model.BatchSize < 0 {
		return conn, fmt.Errorf("model.Batchsize cannot be negative")
	}

	key := model.DriverName + model.DataSourceName
	conn, ok := connections[key]
	if ok {
		return
	}

	conn, err = sql.Open(model.DriverName, model.DataSourceName)
	if err != nil {
		return conn, fmt.Errorf("%v\n model.getConn failed to connect database", err)
	}
	connections[key] = conn
	return
}

func (model Model) SQL(query string) (err error) {
	conn, err := model.getConn()
	if err != nil {
		return fmt.Errorf("%v\n dal.SQL failed on model.getConn", err)
	}
	if _, err = conn.Exec(query); err != nil {
		return fmt.Errorf("%v\n dal.SQL failed on conn.Exec", err)
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

func (model Model) write(table string, values interface{}, mode string) (rowsAffected int64, err error) {
	conn, err := model.getConn()
	if err != nil {
		return rowsAffected, fmt.Errorf("%v\n dal.%s failed on model.init", err, mode)
	}

	rows := reflect.ValueOf(values)
	if rows.Kind() != reflect.Slice {
		return rowsAffected, fmt.Errorf("dal.%s: `values` is not a slice", mode)
	}
	if rows.Len() < 1 {
		return rowsAffected, fmt.Errorf("dal.%s: `values` has NO elements", mode)
	}

	fields, querief, placeholder := parseValue(rows.Index(0), table, mode)
	step := model.Batchsize
	valuesLimit := 1<<16 - 1 // limit of placeholders in mysql: 65,535
	if size := valuesLimit / len(fields); size < step {
		step = size
	}
	tx, _ := conn.Begin()
	for i := 0; i < rows.Len(); i += step {
		placeholders := make([]string, 0, step)
		var params []interface{}
		for j := i; j < i+step && j < rows.Len(); j++ {
			placeholders = append(placeholders, placeholder)
			row := rows.Index(j)
			for u := 0; u < len(fields); u++ {
				// params = append(params, row.FieldByName(fields[u]))
				params = append(params, fmt.Sprintf("%v", row.FieldByName(fields[u])))
			}
		}

		query := fmt.Sprintf(querief, strings.Join(placeholders, ","))
		stmt, err := tx.Prepare(query)
		if err != nil {
			return rowsAffected, fmt.Errorf(
				"%v\n dal.%s failed on transaction.Prepare of %s",
				err, mode, fmt.Sprintf(querief, placeholder+",..."))
		}

		res, err := stmt.Exec(params...)
		if err != nil {
			dispLen, trailing := len(fields), ""
			if len(params) > dispLen {
				trailing = ",..."
			} else {
				dispLen = len(params)
			}
			return rowsAffected, fmt.Errorf(
				"%v\n model.%s failed to write a record to table %s, query: %v\n values: %v%s",
				err, mode, table, fmt.Sprintf(querief, placeholder+",..."), params[:dispLen], trailing)
		}
		affected, _ := res.RowsAffected()
		rowsAffected += affected
	}
	if err = tx.Commit(); err != nil {
		// rowsAffected is 0 if transaction failed
		return 0, fmt.Errorf(
			"%v\n dal.%s failed to commit transaction on table %s", err, mode, table)
	}
	return
}

func parseValue(rv reflect.Value, table, mode string) (fields []string, query, placeholder string) {
	var tags []string
	var parse func(v reflect.Value)
	parse = func(v reflect.Value) {
		numField := v.NumField()
		for u := 0; u < numField; u++ {
			if v.Field(u).Kind() == reflect.Struct {
				parse(v.Field(u))
				continue
			}
			field, tag := v.Type().Field(u).Name, v.Type().Field(u).Tag.Get("field")
			if tag == "" {
				tag = field
			}
			fields, tags = append(fields, field), append(tags, tag)
		}
	}
	parse(rv)

	placeholders, updates := make([]string, 0, len(tags)), make([]string, 0, len(tags))
	for _, tag := range tags {
		placeholders = append(placeholders, "?")
		updates = append(updates, fmt.Sprintf("%s=values(%s)", tag, tag))
	}
	placeholder = "(" + strings.Join(placeholders, ",") + ")"
	switch mode {
	case "Create": // insert|ignore
		query = fmt.Sprintf(`insert ignore into %s(%s) values %%s;`,
			table,
			strings.Join(tags, ","),
		)
	case "Update": // insert|update
		query = fmt.Sprintf(`insert into %s(%s) values %%s on duplicate key update %s;`,
			table,
			strings.Join(tags, ","),
			strings.Join(updates, ","),
		)
	}
	return
}

func (model *Model) Read(table string, fields []string, condition string, readType interface{}) (err error) {
	conn, err := model.getConn()
	if err != nil {
		return fmt.Errorf("%v\n dal.Read failed on model.Init", err)
	}

	// query
	query := fmt.Sprintf("select %s from %s %s", strings.Join(fields, ","), table, condition)
	var rows *sql.Rows
	if rows, err = conn.Query(query); err != nil {
		return fmt.Errorf("%v\n dal.Read failed on conn.Query", err)
	}
	defer rows.Close()

	// scan and set rows
	model.rows = [][]interface{}{}
	model.Records = []interface{}{}

	tp := reflect.TypeOf(readType)
	numField := tp.NumField()
	for rows.Next() {
		values := make([]interface{}, numField)
		for i := 0; i < numField; i++ {
			values[i] = reflect.New(reflect.PtrTo(tp.Field(i).Type)).Interface()
		}
		if err := rows.Scan(values...); err != nil {
			return fmt.Errorf("%v\n model.Scan failed", err)
		}
		model.rows = append(model.rows, values)

		elem := reflect.New(tp)
		for i := 0; i < numField; i++ {
			elem.Elem().FieldByName(tp.Field(i).Name).Set(reflect.ValueOf(values[i]).Elem().Elem())
		}
		model.Records = append(model.Records, elem.Elem().Interface())
	}
	return nil
}

func (model Model) Cleanup(table, fieldTime string, tm int64) (err error) {
	conn, err := model.getConn()
	if err != nil {
		panic(fmt.Errorf("%v\n dal.DBInfo failed on model.init", err))
	}

	query, err := conn.Prepare(fmt.Sprintf("delete from %s where %s < ?;", table, fieldTime))
	if err != nil {
		return fmt.Errorf("%v\n dal.Cleanup failed on conn.Prepare", err)
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
	conn, err := model.getConn()
	if err != nil {
		panic(fmt.Errorf("%v\n dal.DBInfo failed on model.init", err))
	}

	rows, err := conn.Query("select version();")
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
