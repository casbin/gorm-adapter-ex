// Copyright 2017 The casbin Authors. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gormadapter

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strconv"
	"strings"

	"github.com/casbin/casbin/v2/model"
	"github.com/casbin/casbin/v2/persist"
	"github.com/glebarez/sqlite"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlserver"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/plugin/dbresolver"
)

const (
	defaultDatabaseName = "casbin"
	defaultTableName    = "casbin_rule"
	disableMigrateKey   = "disableMigrateKey"
	customTableKey      = "customTableKey"
	maxVariableNumber   = 20 // up to 20 variable, i.e., V0-V20
)

var (
	errUnsupportedFilter = errors.New("unsupported filter type")
)

type CasbinRule struct {
	ID    uint   `gorm:"primaryKey;autoIncrement"`
	Ptype string `gorm:"size:100"`
	V0    string `gorm:"size:100"`
	V1    string `gorm:"size:100"`
	V2    string `gorm:"size:100"`
	V3    string `gorm:"size:100"`
	V4    string `gorm:"size:100"`
	V5    string `gorm:"size:100"`
}

func (CasbinRule) TableName() string {
	return "casbin_rule"
}

type Filter struct {
	Ptype []string
	V0    []string
	V1    []string
	V2    []string
	V3    []string
	V4    []string
	V5    []string
}

// CustomizedFilter Usage:
// 	filter := CustomizedFilter{
//		Fields: []string{"ptype", "V3"},
//		Values: [][]string{
//			{"p"},
//			{"read", "write"},
//		},
//	}
type CustomizedFilter struct {
	Fields []string
	Values [][]string
}

type BatchFilter struct {
	filters []interface{}
}

// Adapter represents the Gorm adapter for policy storage.
type Adapter struct {
	driverName     string
	dataSourceName string
	databaseName   string
	tablePrefix    string
	tableName      string
	dbSpecified    bool
	db             *gorm.DB
	isFiltered     bool

	customTable     interface{}
	customTableType reflect.Type
	maxVarLabel     int
}

// finalizer is the destructor for Adapter.
func finalizer(a *Adapter) {
	sqlDB, err := a.db.DB()
	if err != nil {
		panic(err)
	}
	err = sqlDB.Close()
	if err != nil {
		panic(err)
	}
}

func (a *Adapter) initVarNum() {
	if a.customTable == nil {
		return
	}

	modelType := reflect.TypeOf(a.customTable)
	if modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
	}
	a.customTableType = modelType

	// already ensure `customTable` has {"ID", "Ptype", "V0"} fields
	i := 0
	for i = 1; i < maxVariableNumber; i++ {
		if _, ok := modelType.FieldByName("V" + strconv.Itoa(i)); !ok {
			a.maxVarLabel = i - 1
			break
		}
	}
	a.maxVarLabel = i - 1
}

//Select conn according to table name（use map store name-index）
type specificPolicy int

func (p *specificPolicy) Resolve(connPools []gorm.ConnPool) gorm.ConnPool {
	return connPools[*p]
}

type DbPool struct {
	dbMap  map[string]specificPolicy
	policy *specificPolicy
	source *gorm.DB
}

func (dbPool *DbPool) switchDb(dbName string) *gorm.DB {
	*dbPool.policy = dbPool.dbMap[dbName]
	return dbPool.source.Clauses(dbresolver.Write)
}

// NewAdapter is the constructor for Adapter.
// Params : databaseName,tableName,dbSpecified
//			databaseName,{tableName/dbSpecified}
//			{database/dbSpecified}
// databaseName and tableName are user defined.
// Their default value are "casbin" and "casbin_rule"
//
// dbSpecified is an optional bool parameter. The default value is false.
// It's up to whether you have specified an existing DB in dataSourceName.
// If dbSpecified == true, you need to make sure the DB in dataSourceName exists.
// If dbSpecified == false, the adapter will automatically create a DB named databaseName.
func NewAdapter(driverName string, dataSourceName string, params ...interface{}) (*Adapter, error) {
	a := &Adapter{}
	a.driverName = driverName
	a.dataSourceName = dataSourceName

	a.tableName = defaultTableName
	a.databaseName = defaultDatabaseName
	a.dbSpecified = false

	if len(params) == 1 {
		switch p1 := params[0].(type) {
		case bool:
			a.dbSpecified = p1
		case string:
			a.databaseName = p1
		default:
			return nil, errors.New("wrong format")
		}
	} else if len(params) == 2 {
		switch p2 := params[1].(type) {
		case bool:
			a.dbSpecified = p2
			p1, ok := params[0].(string)
			if !ok {
				return nil, errors.New("wrong format")
			}
			a.databaseName = p1
		case string:
			p1, ok := params[0].(string)
			if !ok {
				return nil, errors.New("wrong format")
			}
			a.databaseName = p1
			a.tableName = p2
		default:
			return nil, errors.New("wrong format")
		}
	} else if len(params) == 3 {
		if p3, ok := params[2].(bool); ok {
			a.dbSpecified = p3
			a.databaseName = params[0].(string)
			a.tableName = params[1].(string)
		} else {
			return nil, errors.New("wrong format")
		}
	} else if len(params) != 0 {
		return nil, errors.New("too many parameters")
	}

	// Open the DB, create it if not existed.
	err := a.Open()
	if err != nil {
		return nil, err
	}

	// Call the destructor when the object is released.
	runtime.SetFinalizer(a, finalizer)

	return a, nil
}

// NewAdapterByDBUseTableName creates gorm-adapter by an existing Gorm instance and the specified table prefix and table name
// Example: gormadapter.NewAdapterByDBUseTableName(&db, "cms", "casbin") Automatically generate table name like this "cms_casbin"
func NewAdapterByDBUseTableName(db *gorm.DB, prefix string, tableName string) (*Adapter, error) {
	if len(tableName) == 0 {
		tableName = defaultTableName
	}

	a := &Adapter{
		tablePrefix: prefix,
		tableName:   tableName,
		customTable: db.Statement.Context.Value(customTableKey),
	}

	a.db = db.Scopes(a.casbinRuleTable()).Session(&gorm.Session{Context: db.Statement.Context})

	err := a.createTable()
	if err != nil {
		return nil, err
	}

	a.initVarNum()

	return a, nil
}

// InitDbResolver multiple databases support
// Example usage:
// dbPool,err := InitDbResolver([]gorm.Dialector{mysql.Open(dsn),mysql.Open(dsn2)},[]string{"casbin1","casbin2"})
// a := initAdapterWithGormInstanceByMulDb(t,dbPool,"casbin1","","casbin_rule1")
// a = initAdapterWithGormInstanceByMulDb(t,dbPool,"casbin2","","casbin_rule2")/*
func InitDbResolver(dbArr []gorm.Dialector, dbNames []string) (DbPool, error) {
	if len(dbArr) == 0 {
		panic("dbArr len is 0")
	}
	source, e := gorm.Open(dbArr[0])
	if e != nil {
		panic(e.Error())
	}
	var p specificPolicy
	p = 0
	err := source.Use(dbresolver.Register(dbresolver.Config{Policy: &p, Sources: dbArr}))
	dbMap := make(map[string]specificPolicy)
	for i := 0; i < len(dbNames); i++ {
		dbMap[dbNames[i]] = specificPolicy(i)
	}
	return DbPool{dbMap: dbMap, policy: &p, source: source}, err
}

func NewAdapterByMulDb(dbPool DbPool, dbName string, prefix string, tableName string) (*Adapter, error) {
	//change DB
	dbPool.switchDb(dbName)

	return NewAdapterByDBUseTableName(dbPool.source, prefix, tableName)
}

// NewFilteredAdapter is the constructor for FilteredAdapter.
// Casbin will not automatically call LoadPolicy() for a filtered adapter.
func NewFilteredAdapter(driverName string, dataSourceName string, params ...interface{}) (*Adapter, error) {
	adapter, err := NewAdapter(driverName, dataSourceName, params...)
	if err != nil {
		return nil, err
	}
	adapter.isFiltered = true
	return adapter, err
}

// NewAdapterByDB creates gorm-adapter by an existing Gorm instance
func NewAdapterByDB(db *gorm.DB) (*Adapter, error) {
	return NewAdapterByDBUseTableName(db, "", defaultTableName)
}

func TurnOffAutoMigrate(db *gorm.DB) {
	ctx := db.Statement.Context
	if ctx == nil {
		ctx = context.Background()
	}

	ctx = context.WithValue(ctx, disableMigrateKey, false)

	*db = *db.WithContext(ctx)
}

func NewAdapterByDBWithCustomTable(db *gorm.DB, t interface{}, tableName ...string) (*Adapter, error) {

	//ensure `customTable` has {"ID", "Ptype", "V0"} fields
	r := reflect.TypeOf(t)
	if r.Kind() == reflect.Ptr {
		r = r.Elem()
	}
	for _, field := range []string{"ID", "Ptype", "V0"} {
		if _, ok := r.FieldByName(field); !ok {
			return nil, fmt.Errorf("The custom table has no column named `%s`", field)
		}
	}

	ctx := db.Statement.Context
	if ctx == nil {
		ctx = context.Background()
	}

	ctx = context.WithValue(ctx, customTableKey, t)

	curTableName := defaultTableName
	if len(tableName) > 0 {
		curTableName = tableName[0]
	}

	return NewAdapterByDBUseTableName(db.WithContext(ctx), "", curTableName)
}

func openDBConnection(driverName, dataSourceName string) (*gorm.DB, error) {
	var err error
	var db *gorm.DB
	if driverName == "postgres" {
		db, err = gorm.Open(postgres.Open(dataSourceName), &gorm.Config{})
	} else if driverName == "mysql" {
		db, err = gorm.Open(mysql.Open(dataSourceName), &gorm.Config{})
	} else if driverName == "sqlserver" {
		db, err = gorm.Open(sqlserver.Open(dataSourceName), &gorm.Config{})
	} else if driverName == "sqlite3" {
		db, err = gorm.Open(sqlite.Open(dataSourceName), &gorm.Config{})
	} else {
		return nil, errors.New("Database dialect '" + driverName + "' is not supported. Supported databases are postgres, mysql and sqlserver")
	}
	if err != nil {
		return nil, err
	}
	return db, err
}

func (a *Adapter) createDatabase() error {
	var err error
	db, err := openDBConnection(a.driverName, a.dataSourceName)
	if err != nil {
		return err
	}
	if a.driverName == "postgres" {
		if err = db.Exec("CREATE DATABASE " + a.databaseName).Error; err != nil {
			// 42P04 is	duplicate_database
			if strings.Contains(fmt.Sprintf("%s", err), "42P04") {
				return nil
			}
		}
	} else if a.driverName != "sqlite3" {
		err = db.Exec("CREATE DATABASE IF NOT EXISTS " + a.databaseName).Error
	}
	if err != nil {
		return err
	}
	return nil
}

func (a *Adapter) Open() error {
	var err error
	var db *gorm.DB

	if a.dbSpecified {
		db, err = openDBConnection(a.driverName, a.dataSourceName)
		if err != nil {
			return err
		}
	} else {
		if err = a.createDatabase(); err != nil {
			return err
		}
		if a.driverName == "postgres" {
			db, err = openDBConnection(a.driverName, a.dataSourceName+" dbname="+a.databaseName)
		} else if a.driverName == "sqlite3" {
			db, err = openDBConnection(a.driverName, a.dataSourceName)
		} else {
			db, err = openDBConnection(a.driverName, a.dataSourceName+a.databaseName)
		}
		if err != nil {
			return err
		}
	}

	a.db = db.Scopes(a.casbinRuleTable()).Session(&gorm.Session{})
	return a.createTable()
}

// AddLogger adds logger to db
func (a *Adapter) AddLogger(l logger.Interface) {
	a.db = a.db.Session(&gorm.Session{Logger: l, Context: a.db.Statement.Context})
}

func (a *Adapter) Close() error {
	finalizer(a)
	return nil
}

// getTableObject return the dynamic table object
func (a *Adapter) getTableObject() interface{} {
	if a.customTable == nil {
		return &CasbinRule{}
	}
	return a.customTable
}

func (a *Adapter) customizedDBDelete(db *gorm.DB, conds ...interface{}) error {
	return db.Delete(a.getTableObject(), conds...).Error
}

func (a *Adapter) customizedDBFind(db *gorm.DB, linesInterfacePtr *[]interface{}, conds ...interface{}) error {
	var linesPtr interface{}

	if a.customTable == nil {
		lines := make([]CasbinRule, 0)
		linesPtr = &lines
	} else {
		linesPtr = reflect.New(reflect.SliceOf(a.customTableType)).Interface()
	}

	err := db.Find(linesPtr, conds...).Error
	*linesInterfacePtr = *a.rulesToInterfaceArray(linesPtr)
	return err
}

func (a *Adapter) customizedDBCreate(db *gorm.DB, linePtr *interface{}) error {
	linesPtr := a.rulesFromInterfaceArray(&[]interface{}{*linePtr})
	return db.Create(linesPtr).Error
}

func (a *Adapter) customizedDBCreateMany(db *gorm.DB, linesInterfacePtr *[]interface{}) error {
	linesPtr := a.rulesFromInterfaceArray(linesInterfacePtr)
	return db.Create(linesPtr).Error
}

func (a *Adapter) rulesFromInterfaceArray(linesInterfacePtr *[]interface{}) interface{} {
	if a.customTable == nil {
		lines := make([]CasbinRule, 0, len(*linesInterfacePtr))
		for _, line := range *linesInterfacePtr {
			lines = append(lines, line.(CasbinRule))
		}
		return &lines
	}

	linesReflection := reflect.New(reflect.SliceOf(a.customTableType)).Elem()
	for _, line := range *linesInterfacePtr {
		linesReflection.Set(reflect.Append(linesReflection, reflect.ValueOf(line)))
	}

	return linesReflection.Addr().Interface()
}

// linesPtrInterface: *[]CasbinRule or *[]customizedCasbinRule
func (a *Adapter) rulesToInterfaceArray(linesPtrInterface interface{}) *[]interface{} {
	if a.customTable == nil {
		linesPtrValue := linesPtrInterface.(*[]CasbinRule)
		linesValue := *linesPtrValue
		lines := make([]interface{}, 0, len(linesValue))
		for _, line := range linesValue {
			lines = append(lines, line)
		}
		return &lines
	}

	linesValue := reflect.ValueOf(linesPtrInterface).Elem()
	lines := make([]interface{}, 0, linesValue.Len())
	for i := 0; i < linesValue.Len(); i++ {
		lines = append(lines, linesValue.Index(i).Interface())
	}

	return &lines
}

func (a *Adapter) getFullTableName() string {
	if a.tablePrefix != "" {
		return a.tablePrefix + "_" + a.tableName
	}
	return a.tableName
}

func (a *Adapter) casbinRuleTable() func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		tableName := a.getFullTableName()
		return db.Table(tableName)
	}
}

func (a *Adapter) createTable() error {
	disableMigrate := a.db.Statement.Context.Value(disableMigrateKey)
	if disableMigrate != nil {
		return nil
	}

	t := a.db.Statement.Context.Value(customTableKey)

	if t != nil {
		return a.db.AutoMigrate(t)
	}

	t = a.getTableObject()
	if err := a.db.AutoMigrate(t); err != nil {
		return err
	}

	tableName := a.getFullTableName()
	index := strings.ReplaceAll("idx_"+tableName, ".", "_")
	hasIndex := a.db.Migrator().HasIndex(t, index)
	if !hasIndex {
		if a.customTable == nil {
			if err := a.db.Exec(fmt.Sprintf("CREATE UNIQUE INDEX %s ON %s (ptype,v0,v1,v2,v3,v4,v5)", index, tableName)).Error; err != nil {
				return err
			}
		} else {
			columnName := []string{"v0"}
			for i := 1; i <= a.maxVarLabel; i++ {
				columnName = append(columnName, "v"+strconv.Itoa(i))
			}
			if err := a.db.Exec(fmt.Sprintf(
				"CREATE UNIQUE INDEX %s ON %s (ptype,%s)",
				index,
				tableName,
				strings.Join(columnName, ","),
			)).Error; err != nil {
				return err
			}
		}
	}
	return nil
}

func (a *Adapter) dropTable() error {
	t := a.db.Statement.Context.Value(customTableKey)
	if t == nil {
		return a.db.Migrator().DropTable(a.getTableObject())
	}

	return a.db.Migrator().DropTable(t)
}

func (a *Adapter) truncateTable() error {
	if a.db.Config.Name() == sqlite.DriverName {
		return a.db.Exec(fmt.Sprintf("delete from %s", a.getFullTableName())).Error
	}
	return a.db.Exec(fmt.Sprintf("truncate table %s", a.getFullTableName())).Error
}

// LoadPolicy loads policy from database.
func (a *Adapter) LoadPolicy(model model.Model) error {
	var lines []interface{} // []CasbinRule or []customizedCasbinRule

	if err := a.customizedDBFind(a.db.Order("ID"), &lines); err != nil {
		return err
	}

	for _, line := range lines {
		persist.LoadPolicyArray(a.ruleToStringArray(line), model)
	}

	return nil
}

// LoadFilteredPolicy loads only policy rules that match the filter.
func (a *Adapter) LoadFilteredPolicy(model model.Model, filter interface{}) error {
	var lines []interface{} // []CasbinRule or []customizedCasbinRule

	batchFilter := BatchFilter{
		filters: []interface{}{},
	}
	switch filterValue := filter.(type) {
	case Filter, CustomizedFilter:
		batchFilter.filters = []interface{}{filterValue}
	case BatchFilter:
		batchFilter = filterValue
	default:
		return errUnsupportedFilter
	}

	for _, f := range batchFilter.filters {
		queryStr, queryArgs, err := a.filterToDbQuery(a.db, f)
		if err != nil {
			return err
		}
		//if err = a.db.Where(queryStr, queryArgs...).Order("ID").Find(&lines).Error; err != nil {
		if err = a.customizedDBFind(a.db.Where(queryStr, queryArgs...).Order("ID"), &lines); err != nil {
			return err
		}

		for _, line := range lines {
			persist.LoadPolicyArray(a.ruleToStringArray(line), model)
		}
	}
	a.isFiltered = true

	return nil
}

// IsFiltered returns true if the loaded policy has been filtered.
func (a *Adapter) IsFiltered() bool {
	return a.isFiltered
}

// filterToQuery builds the gorm query to match the rule filter to use within a scope.
func (a *Adapter) filterToDbQuery(db *gorm.DB, filterInterface interface{}) (string, []interface{}, error) {
	queryArgs := []interface{}{}
	queryStr := ""

	switch filter := filterInterface.(type) {
	case Filter:
		if len(filter.Ptype) > 0 {
			queryStr += "ptype in (?)"
			queryArgs = append(queryArgs, filter.Ptype)
		}
		if len(filter.V0) > 0 {
			queryStr += "v0 in (?)"
			queryArgs = append(queryArgs, filter.V0)
		}
		if len(filter.V1) > 0 {
			queryStr += "v1 in (?)"
			queryArgs = append(queryArgs, filter.V1)
		}
		if len(filter.V2) > 0 {
			queryStr += "v2 in (?)"
			queryArgs = append(queryArgs, filter.V2)
		}
		if len(filter.V3) > 0 {
			queryStr += "v3 in (?)"
			queryArgs = append(queryArgs, filter.V3)
		}
		if len(filter.V4) > 0 {
			queryStr += "v4 in (?)"
			queryArgs = append(queryArgs, filter.V4)
		}
		if len(filter.V5) > 0 {
			queryStr += "v5 in (?)"
			queryArgs = append(queryArgs, filter.V5)
		}
	case CustomizedFilter:
		if len(filter.Fields) != len(filter.Values) {
			return "", nil, errors.New("the number of CustomizedFilter's fields and values must be the same")
		}
		for i := 0; i < len(filter.Fields); i++ {
			// e.g.: "ptype in (?) "
			queryStr += db.NamingStrategy.ColumnName(a.getFullTableName(), filter.Fields[i]) + " in (?) "
			queryArgs = append(queryArgs, filter.Values[i])
		}
	default:
		return "", nil, errUnsupportedFilter
	}
	return queryStr, queryArgs, nil
}

// SavePolicy saves policy to database.
func (a *Adapter) SavePolicy(model model.Model) error {
	err := a.truncateTable()
	if err != nil {
		return err
	}

	var lines []interface{}
	flushEvery := 1000
	for ptype, ast := range model["p"] {
		for _, rule := range ast.Policy {
			lines = append(lines, a.ruleFromStringArray(ptype, rule))
			if len(lines) > flushEvery {
				//if err := a.db.Create(&lines).Error; err != nil {
				if err := a.customizedDBCreateMany(a.db, &lines); err != nil {
					return err
				}
				lines = nil
			}
		}
	}

	for ptype, ast := range model["g"] {
		for _, rule := range ast.Policy {
			lines = append(lines, a.ruleFromStringArray(ptype, rule))
			if len(lines) > flushEvery {
				if err := a.customizedDBCreateMany(a.db, &lines); err != nil {
					return err
				}
				lines = nil
			}
		}
	}
	if len(lines) > 0 {
		if err := a.customizedDBCreateMany(a.db, &lines); err != nil {
			return err
		}
	}

	return nil
}

// AddPolicy adds a policy rule to the storage.
func (a *Adapter) AddPolicy(sec string, ptype string, rule []string) error {
	line := a.ruleFromStringArray(ptype, rule)
	err := a.customizedDBCreate(a.db, &line)
	return err
}

// RemovePolicy removes a policy rule from the storage.
func (a *Adapter) RemovePolicy(sec string, ptype string, rule []string) error {
	line := a.ruleFromStringArray(ptype, rule)
	err := a.rawDelete(a.db, line) //can't use db.Delete as we're not using primary key http://jinzhu.me/gorm/crud.html#delete
	return err
}

// AddPolicies adds multiple policy rules to the storage.
func (a *Adapter) AddPolicies(sec string, ptype string, rules [][]string) error {
	var lines []interface{}
	for _, rule := range rules {
		line := a.ruleFromStringArray(ptype, rule)
		lines = append(lines, line)
	}
	return a.customizedDBCreateMany(a.db, &lines)
	//return a.db.Create(&lines).Error
}

// RemovePolicies removes multiple policy rules from the storage.
func (a *Adapter) RemovePolicies(sec string, ptype string, rules [][]string) error {
	return a.db.Transaction(func(tx *gorm.DB) error {
		for _, rule := range rules {
			line := a.ruleFromStringArray(ptype, rule)
			if err := a.rawDelete(tx, line); err != nil { //can't use db.Delete as we're not using primary key http://jinzhu.me/gorm/crud.html#delete
				return err
			}
		}
		return nil
	})
}

// RemoveFilteredPolicy removes policy rules that match the filter from the storage.
func (a *Adapter) RemoveFilteredPolicy(sec string, ptype string, fieldIndex int, fieldValues ...string) error {
	var line interface{}

	line = a.ruleFromFilteredField(ptype, fieldIndex, fieldValues...)

	err := a.rawDelete(a.db, line)
	return err
}

func (a *Adapter) rawDelete(db *gorm.DB, line interface{}) error {
	queryStr, queryArgs := a.ruleToDbQuery(line)
	args := append([]interface{}{queryStr}, queryArgs...)
	err := a.customizedDBDelete(db, args...)
	return err
}

// UpdatePolicy updates a new policy rule to DB.
func (a *Adapter) UpdatePolicy(sec string, ptype string, oldRule, newPolicy []string) error {
	oldLine := a.ruleFromStringArray(ptype, oldRule)
	newLine := a.ruleFromStringArray(ptype, newPolicy)
	return a.db.Model(&oldLine).Where(oldLine).Updates(newLine).Error
}

func (a *Adapter) UpdatePolicies(sec string, ptype string, oldRules, newRules [][]string) error {
	oldPolicies := make([]interface{}, 0, len(oldRules))
	newPolicies := make([]interface{}, 0, len(oldRules))
	for _, oldRule := range oldRules {
		oldPolicies = append(oldPolicies, a.ruleFromStringArray(ptype, oldRule))
	}
	for _, newRule := range newRules {
		newPolicies = append(newPolicies, a.ruleFromStringArray(ptype, newRule))
	}
	tx := a.db.Begin()
	for i := range oldPolicies {
		if err := tx.Model(oldPolicies[i]).Where(oldPolicies[i]).Updates(newPolicies[i]).Error; err != nil {
			tx.Rollback()
			return err
		}
	}
	return tx.Commit().Error
}

func (a *Adapter) ruleFromFilteredField(ptype string, fieldIndex int, fieldValues ...string) interface{} {
	if a.customTable == nil {
		line := &CasbinRule{}

		line.Ptype = ptype
		if fieldIndex <= 0 && 0 < fieldIndex+len(fieldValues) {
			line.V0 = fieldValues[0-fieldIndex]
		}
		if fieldIndex <= 1 && 1 < fieldIndex+len(fieldValues) {
			line.V1 = fieldValues[1-fieldIndex]
		}
		if fieldIndex <= 2 && 2 < fieldIndex+len(fieldValues) {
			line.V2 = fieldValues[2-fieldIndex]
		}
		if fieldIndex <= 3 && 3 < fieldIndex+len(fieldValues) {
			line.V3 = fieldValues[3-fieldIndex]
		}
		if fieldIndex <= 4 && 4 < fieldIndex+len(fieldValues) {
			line.V4 = fieldValues[4-fieldIndex]
		}
		if fieldIndex <= 5 && 5 < fieldIndex+len(fieldValues) {
			line.V5 = fieldValues[5-fieldIndex]
		}

		return *line
	}

	line := reflect.New(a.customTableType).Elem() // returned value is a pointer
	line.FieldByName("Ptype").SetString(ptype)

	if fieldIndex < 0 {
		return line.Interface()
	}

	for i := 0; i < len(fieldValues); i++ {
		index := i + fieldIndex
		if index > a.maxVarLabel {
			break
		}
		field := "V" + strconv.Itoa(index)
		line.FieldByName(field).SetString(fieldValues[i])
	}
	return line.Interface()
}

func (a *Adapter) UpdateFilteredPolicies(sec string, ptype string, newPolicies [][]string, fieldIndex int, fieldValues ...string) ([][]string, error) {
	// UpdateFilteredPolicies deletes old rules and adds new rules.
	line := a.ruleFromFilteredField(ptype, fieldIndex, fieldValues...)

	newP := make([]interface{}, 0, len(newPolicies))
	oldP := make([]interface{}, 0)
	for _, newRule := range newPolicies {
		newP = append(newP, a.ruleFromStringArray(ptype, newRule))
	}

	tx := a.db.Begin()
	str, args := a.ruleToDbQuery(line)

	for i := range newP {
		//if err := tx.Where(str, args...).Find(&oldP).Error; err != nil {
		if err := a.customizedDBFind(tx.Where(str, args...), &oldP); err != nil {
			tx.Rollback()
			return nil, err
		}
		//if err := tx.Where(str, args...).Delete([]CasbinRule{}).Error; err != nil {
		if err := a.customizedDBDelete(tx.Where(str, args...)); err != nil {
			tx.Rollback()
			return nil, err
		}
		//if err := tx.Create(&newP[i]).Error; err != nil {
		if err := a.customizedDBCreate(tx.Where(str, args...), &newP[i]); err != nil {
			tx.Rollback()
			return nil, err
		}
	}

	// return deleted rules
	oldPolicies := make([][]string, 0)
	for _, v := range oldP {
		oldPolicy := a.ruleToStringArray(v)
		oldPolicies = append(oldPolicies, oldPolicy)
	}
	return oldPolicies, tx.Commit().Error
}

func (a *Adapter) ruleToDbQuery(lineInterface interface{}) (string, []interface{}) {
	if a.customTable == nil {
		line := lineInterface.(CasbinRule)
		queryArgs := []interface{}{line.Ptype}

		queryStr := "ptype = ?"
		if line.V0 != "" {
			queryStr += " and v0 = ?"
			queryArgs = append(queryArgs, line.V0)
		}
		if line.V1 != "" {
			queryStr += " and v1 = ?"
			queryArgs = append(queryArgs, line.V1)
		}
		if line.V2 != "" {
			queryStr += " and v2 = ?"
			queryArgs = append(queryArgs, line.V2)
		}
		if line.V3 != "" {
			queryStr += " and v3 = ?"
			queryArgs = append(queryArgs, line.V3)
		}
		if line.V4 != "" {
			queryStr += " and v4 = ?"
			queryArgs = append(queryArgs, line.V4)
		}
		if line.V5 != "" {
			queryStr += " and v5 = ?"
			queryArgs = append(queryArgs, line.V5)
		}
		//args := append([]interface{}{queryStr}, queryArgs...)

		return queryStr, queryArgs
	}

	line := reflect.ValueOf(lineInterface)

	queryArgs := []interface{}{line.FieldByName("Ptype").Interface()}
	queryStr := "ptype = ?"

	for i := 0; i <= a.maxVarLabel; i++ {
		field := "V" + strconv.Itoa(i)

		if line.FieldByName(field).String() == "" {
			continue
		}
		queryStr += " and v" + strconv.Itoa(i) + " = ?"
		queryArgs = append(queryArgs, line.FieldByName(field).Interface())
	}

	//args := append([]interface{}{queryStr}, queryArgs...)

	return queryStr, queryArgs
}

func (a *Adapter) ruleToStringArray(lineInterface interface{}) []string {
	if a.customTable == nil {
		line := lineInterface.(CasbinRule)

		var p = []string{line.Ptype,
			line.V0, line.V1, line.V2,
			line.V3, line.V4, line.V5}

		index := len(p) - 1
		for p[index] == "" {
			index--
		}
		index += 1
		p = p[:index]
		return p
	}

	line := reflect.ValueOf(lineInterface)
	p := []string{line.FieldByName("Ptype").String(), line.FieldByName("V0").String()}

	for j := 1; j <= a.maxVarLabel; j++ {
		field := "V" + strconv.Itoa(j)
		value := line.FieldByName(field).String()
		if value == "" {
			break
		}
		p = append(p, value)
	}

	return p
}

// return a CasbinRule or customizedCasbinRule
func (a *Adapter) ruleFromStringArray(ptype string, rule []string) interface{} {
	if a.customTable == nil {
		line := &CasbinRule{}

		line.Ptype = ptype
		if len(rule) > 0 {
			line.V0 = rule[0]
		}
		if len(rule) > 1 {
			line.V1 = rule[1]
		}
		if len(rule) > 2 {
			line.V2 = rule[2]
		}
		if len(rule) > 3 {
			line.V3 = rule[3]
		}
		if len(rule) > 4 {
			line.V4 = rule[4]
		}
		if len(rule) > 5 {
			line.V5 = rule[5]
		}

		return *line
	}

	line := reflect.New(a.customTableType).Elem()
	line.FieldByName("Ptype").SetString(ptype)

	for i := 0; i < len(rule); i++ {
		field := "V" + strconv.Itoa(i)
		line.FieldByName(field).SetString(rule[i])
	}
	return line.Interface()
}
