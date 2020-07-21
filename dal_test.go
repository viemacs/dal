package dal

import (
	"testing"
)

func Test_DBInfo(t *testing.T) {
	model := Model{
		DriverName:     "postgres",
		DataSourceName: "invalid",
	}
	println(model.DriverName)
}
