DAL
===

DAL provides a Database Access Layer in Go for MySQL.

## Requirements
- Go 1.10 or higher
- MySQL(4.1+) or MariaDB

## Installation
This package can be installed with the go get command.

``` go
go get github.com/viemacs/dal
```

## API Reference
Examples can be found in the test file.

A model should be initialized first with connection options.

- Initialize a Model of MySQL.

``` go
model := Model{
    DriverName:     "mysql",
    DataSourceName: "user:password@tcp(hostIP)/database",
}
```

- Write values to database.

``` go
type T struct {
     ID int `field:"id"`
}
values := []T{{1}}
if err := model.Write("tableName", values); err != nil {
	dealWithError(err)
}
```

- Read records from database.

``` go
if err := model.Read("tablename", []string{"id"}, "", T{}); err != nil {
	dealWithError(err)
}
for _, v := range model.Records {
    handleRecord(v.(T))
}
```

- Get version of database.

``` go
if info := model.DBInfo(); len(info) != 1 {
	dealWithError(err)
}
```


Program with fail with panic if the connection to database cannot be established. The caller of this module can recover properly.

## Todo
1. check type of parameter `values` in Write()
2. `Read` function skips passing query fields, or match query results with fields of struct.
3. Support values of nested struct in `Read` function.

## Issue
1. `Exec` accepts normal interface, but currently values are all printed into strings.

## TBConfirm
1. sumRowsAffected: are records in database changed before tx.commit() ?

## Notice
1. Values of nested struct with duplicated fields are not supported yet.
   During writing, the duplicated fields will generate duplicated fields in SQL.
   During reading, only the first one of the duplicated fields will be filled.
