Gorm Adapter Ex
====
This gorm-adapter-ex is an extended version of [Gorm-Adapter](https://github.com/casbin/gorm-adapter).
It introduces great and implements [proposal of customizability](https://github.com/casbin/gorm-adapter/issues/180) by reflection. The database structure can be arbitrary as long as it has `{"ID", "Ptype", "V0"}` three fields.

```golang
	type TestCasbinRule struct { //arbitrary number of variable fields
		ID    uint   `gorm:"primaryKey;autoIncrement"`
		Ptype string `gorm:"size:16"`
		V0    string `gorm:"size:128"`
		V1    string `gorm:"size:128"`
		V2    string `gorm:"size:256"`
		DeletedAt gorm.DeletedAt
	}

	// Create an adapter
	a, _ := NewAdapterByDBWithCustomTable(db, &TestCasbinRule{}, "test_casbin_rule")
	//...
```

Background for why we create a new fork:

[casbin/gorm-adapter#179 (comment)](https://github.com/casbin/gorm-adapter/issues/179#issuecomment-1228530939)

[casbin/gorm-adapter#168 (comment)](https://github.com/casbin/gorm-adapter/pull/168#issuecomment-1228556830) 


# About Gorm Adapter

Gorm Adapter is the [Gorm](https://gorm.io/gorm) adapter for [Casbin](https://github.com/casbin/casbin). With this library, Casbin can load policy from Gorm supported database or save policy to it.

Based on [Officially Supported Databases](https://v1.gorm.io/docs/connecting_to_the_database.html#Supported-Databases), The current supported databases are:

- MySQL
- PostgreSQL
- SQL Server
- Sqlite3
> gorm-adapter use ``github.com/glebarez/sqlite`` instead of gorm official sqlite driver ``gorm.io/driver/sqlite`` because the latter needs ``cgo`` support. But there is almost no difference between the two driver. If there is a difference in use, please submit an issue.

- other 3rd-party supported DBs in Gorm website or other places.

## Installation

    go get github.com/casbin/gorm-adapter/v3

## Simple Example

```go
package main

import (
	"github.com/casbin/casbin/v2"
	gormadapter "github.com/casbin/gorm-adapter/v3"
	_ "github.com/go-sql-driver/mysql"
)

func main() {
	// Initialize a Gorm adapter and use it in a Casbin enforcer:
	// The adapter will use the MySQL database named "casbin".
	// If it doesn't exist, the adapter will create it automatically.
	// You can also use an already existing gorm instance with gormadapter.NewAdapterByDB(gormInstance)
	a, _ := gormadapter.NewAdapter("mysql", "mysql_username:mysql_password@tcp(127.0.0.1:3306)/") // Your driver and data source.
	e, _ := casbin.NewEnforcer("examples/rbac_model.conf", a)
	
	// Or you can use an existing DB "abc" like this:
	// The adapter will use the table named "casbin_rule".
	// If it doesn't exist, the adapter will create it automatically.
	// a := gormadapter.NewAdapter("mysql", "mysql_username:mysql_password@tcp(127.0.0.1:3306)/abc", true)

	// Load the policy from DB.
	e.LoadPolicy()
	
	// Check the permission.
	e.Enforce("alice", "data1", "read")
	
	// Modify the policy.
	// e.AddPolicy(...)
	// e.RemovePolicy(...)
	
	// Save the policy back to DB.
	e.SavePolicy()
}
```
## Turn off AutoMigrate
New an adapter will use ``AutoMigrate`` by default for create table, if you want to turn it off, please use API ``TurnOffAutoMigrate(db *gorm.DB) *gorm.DB``. See example: 
```go
db, err := gorm.Open(mysql.Open("root:@tcp(127.0.0.1:3306)/casbin"), &gorm.Config{})
TurnOffAutoMigrate(db)
// a,_ := NewAdapterByDB(...)
// a,_ := NewAdapterByDBUseTableName(...)
a,_ := NewAdapterByDBWithCustomTable(...)
```
Find out more details at [gorm-adapter#162](https://github.com/casbin/gorm-adapter/issues/162)
## Customize table columns example
You can change the gorm struct tags, but the table structure must stay the same.
```go
package main

import (
	"github.com/casbin/casbin/v2"
	gormadapter "github.com/casbin/gorm-adapter/v3"
	"gorm.io/gorm"
)

func main() {
	// Increase the column size to 512.
	type CasbinRule struct {
		ID    uint   `gorm:"primaryKey;autoIncrement"`
		Ptype string `gorm:"size:512;uniqueIndex:unique_index"`
		V0    string `gorm:"size:512;uniqueIndex:unique_index"`
		V1    string `gorm:"size:512;uniqueIndex:unique_index"`
		V2    string `gorm:"size:512;uniqueIndex:unique_index"`
		V3    string `gorm:"size:512;uniqueIndex:unique_index"`
		V4    string `gorm:"size:512;uniqueIndex:unique_index"`
		V5    string `gorm:"size:512;uniqueIndex:unique_index"`
	}

	db, _ := gorm.Open(...)

	// Initialize a Gorm adapter and use it in a Casbin enforcer:
	// The adapter will use an existing gorm.DB instnace.
	a, _ := gormadapter.NewAdapterByDBWithCustomTable(db, &CasbinRule{}) 
	e, _ := casbin.NewEnforcer("examples/rbac_model.conf", a)
	
	// Load the policy from DB.
	e.LoadPolicy()
	
	// Check the permission.
	e.Enforce("alice", "data1", "read")
	
	// Modify the policy.
	// e.AddPolicy(...)
	// e.RemovePolicy(...)
	
	// Save the policy back to DB.
	e.SavePolicy()
}
```

## Getting Help

- [Casbin](https://github.com/casbin/casbin)

## License

This project is under Apache 2.0 License. See the [LICENSE](LICENSE) file for the full license text.
