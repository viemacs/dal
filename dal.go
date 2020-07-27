// Package dal provides a database access layer.
package dal

import (
	"database/sql"
	"fmt"
	"log"
	"reflect"
	"strings"
)

// Model.Read() is not parallel-able
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
	if model.DriverName != driverName {
		panic(fmt.Sprintf("driver name (%s) is unknown or does not match (%s)", model.DriverName, driverName))
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

	_, err = model.db.Exec(query)
	if err != nil {
		return fmt.Errorf("%v\n dal.SQL failed on model.db.Exec", err)
	}
	return
}

func (model Model) Insert(table string, values interface{}) (err error) {
	return model.write(table, values, "insert|ignore")
}

func (model Model) Write(table string, values interface{}) (err error) {
	return model.write(table, values, "insert|update")
}

func (model Model) write(table string, values interface{}, mode string) (err error) {
	if model.db == nil {
		if err = model.init(); err != nil {
			return fmt.Errorf("%v\n dal.Write failed on model.init", err)
		}
	}

	rows := reflect.ValueOf(values)
	if rows.Kind() != reflect.Slice {
		return fmt.Errorf("dal.Write: interface value is not a slice")
	}
	if rows.Len() < 1 {
		return fmt.Errorf("dal.Write: interface value has NO elements")
	}

	idColumn, idField := "id", ""
	var fields, tags, placeholders, updates []string
	tp := rows.Index(0).Type()
	numField := tp.NumField()
	for u := 0; u < numField; u++ {
		tag := tp.Field(u).Tag.Get("field")
		field := tp.Field(u).Name
		if tag == "" {
			tag = field
		}
		if strings.ToLower(tag) == idColumn {
			idField = field
		}
		fields = append(fields, field)
		tags = append(tags, tag)
		placeholders = append(placeholders, "?")
		updates = append(updates, tag+"=?")
	}

	var sumRowsAffected int64
	tx, _ := model.db.Begin()
	for i := 0; i < rows.Len(); i++ {
		row := rows.Index(i)
		var args, params []interface{}
		for u := 0; u < numField; u++ {
			params = append(params, fmt.Sprintf("%v", row.FieldByName(fields[u])))
		}
		var query string
		switch mode {
		case "insert|ignore":
			query, args = queryInsertOrIgnore(table, idColumn, idField, tags, placeholders, updates, params)
		case "insert|update":
			query, args = queryInsertOrUpdate(table, idColumn, idField, tags, placeholders, updates, params)
		default:
			return fmt.Errorf("unknown mode for writing records: %s", mode)
		}

		stmt, err := tx.Prepare(query)
		if err != nil {
			log.Printf("%v\n dal.Write failed on transaction.Prepare", err)
			// return fmt.Errorf("%v\n dal.Write failed on transaction.Prepare", err) // TODO
		}
		res, err := stmt.Exec(args...)
		if err != nil {
			log.Printf("failed to write a record to table %s: %v", table, err)
			continue
			// TODO: put errors into an error slice
		}
		rowsAffected, _ := res.RowsAffected()
		sumRowsAffected += rowsAffected
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("%v\n failed to commit transaction on table %s", err, table)
	}
	fmt.Printf("dal.Write: %d records are written in table %s", sumRowsAffected, table)
	return nil
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

	rows, err := model.db.Query(queryVersion)
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
