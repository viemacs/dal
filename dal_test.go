package dal

import (
	"fmt"
	"reflect"
	"testing"
)

func Test_DBInfo(t *testing.T) {
	model := Model{
		DriverName:     "postgres",
		DataSourceName: "invalid",
	}
	println(model.DriverName)
}

func TestModel(t *testing.T) {
	_ = Model{
		DriverName:     "nil",
		DataSourceName: "test@tcp(localhost)/test",
	}
}

func Test_write(t *testing.T) {
	model := Model{
		DriverName:     "mysql",
		DataSourceName: "test@tcp(localhost)/test",
	}
	defer func() {
		if err := model.SQL("drop table `user`;"); err != nil {
			t.Error(err)
		}
	}()

	if err := model.SQL("create table user (id int primary key, name varchar(64));"); err != nil {
		t.Error(err)
	}
	type T struct {
		ID   int    `field:"id"`
		Name string `field:"name"`
	}
	values := []T{
		{1, "a"},
		{2, "b"},
	}

	// version
	if info := model.DBInfo(); len(info) != 1 {
		t.Error("cannot get database version info")
		return
	}

	// write
	if _, err := model.Update("user", values); err != nil {
		t.Error(err)
		return
	}

	// read
	checkRead := func() {
		if err := model.Read("user", []string{"id", "name"}, "", T{}); err != nil {
			t.Error(err)
		}
		if a, b := len(model.Records), len(values); a != b {
			t.Errorf("length of query results (%d) != length of records (%d)", a, b)
		}
		for i := 0; i < len(model.Records); i++ {
			record := model.Records[i].(T)
			if record.ID != values[i].ID || record.Name != values[i].Name {
				t.Error("query results differs from origin values")
			}
		}
	}
	checkRead()

	// re-read
	checkRead()
}

func Test_parseValue(t *testing.T) {
	type being struct {
		Name string `field:"name"`
	}
	type person struct {
		being
		Age int `field:"age"`
	}
	p := person{
		being: being{
			Name: "John",
		},
		Age: 12,
	}
	fields, query, placeholder := parseValue(reflect.ValueOf(p), "staff", "Update")
	query = fmt.Sprintf(query, placeholder)

	tFields := []string{"Name", "Age"}
	if len(fields) != len(tFields) || fields[0] != tFields[0] || fields[1] != tFields[1] {
		t.Errorf("output fields %+v is different from %+v", fields, tFields)
	}
	tQuery := "insert into staff(name,age) values (?,?) on duplicate key update name=values(name),age=values(age);"
	if query != tQuery {
		t.Error("output query string is wrong")
	}
}
