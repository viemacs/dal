package dal

import (
	"testing"
)

func TestModel(t *testing.T) {
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
	}

	// write
	if err := model.Write("user", values); err != nil {
		t.Error(err)
	}

	// read
	checkRead := func() {
		if err := model.Read("user", []string{"id", "name"}, "", T{}); err != nil {
			t.Error(err)
		}
		if len(model.Records) != len(values) {
			t.Error("length of query results and records are not the same")
		}
		for i := 0; i < len(values); i++ {
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
