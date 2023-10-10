package skydba

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"

	"github.com/elliotchance/orderedmap"
	"suntech.com.vn/skylib/skyutl.git/skyutl"
)

//Q struct
type Q struct {
	mutex sync.RWMutex
	DB    *sql.DB
}

//NewQuery function
func NewQuery(db *sql.DB) *Q {
	return &Q{DB: db}
}

//DefaultQuery function
func DefaultQuery() *Q {
	return &Q{DB: MainDB}
}

// Read function
// SELECT * FROM model [WHERE ...] [ORDER BY ...]
// Select one, many or all record(s) from table which name <model>
// Return read record(s) and set to model
// model: struct or array of struct
// orderBy ="field1, field2, ....[desc]"
// condFields: conditional fields

// var tables []Table{}{Table{Field: "A"}, Table{Field: "B"}}
// err := q.Read(&tables, "", []string{"field"})
func (q *Q) Read(model interface{}, orderBy string, condFields []string) error {
	return q.read(model, orderBy, condFields)
}

// ReadWithWhere function
// SELECT * FROM model [WHERE ...] [ORDER BY ...]
// Select one, many or all record(s) from table which name <model>
// Return read record(s) and set to model
// model: struct or array of struct
// orderBy ="field1, field2, ....[desc]"
// whereCond: condition
// params: array of values

// var tables []Table{}
// err := q.ReadWithWhere(&tables, "name LIKE $1", []interface{}{"%abc%"})
func (q *Q) ReadWithWhere(model interface{}, orderBy, whereCond string, params []interface{}) error {
	return q.readWithWhere(model, orderBy, whereCond, params)
}

// ReadWithID function
// SELECT * FROM model [WHERE id = ... OR id = ...] [ORDER BY ...]
// Select one, many record(s) from table which name <model>
// Return read record(s) and set to model
// model: struct or array of struct
// orderBy ="field1, field2, ....[desc]"

// var tables []Table{}{Table{Id: 1}, Table{Id: 2}}
// err := q.ReadWithID(&tables, "sort desc")

func (q *Q) ReadWithID(model interface{}, orderBy string) error {
	return q.read(model, orderBy, []string{"id"})
}

func (q *Q) ReadWithCode(model interface{}, orderBy string) error {
	return q.read(model, orderBy, []string{"code"})
}

// ReadWithCond function
// SELECT * FROM model [WHERE ...] [ORDER BY ...]
// Select one, many record(s) from table which name <model>
// Return read record(s) and set to model
// model: struct or array of struct
// orderBy ="field1, field2, ....[desc]"
// condMap: conditional map

// var tables []Table{}
// condMap := orderedmap.NewOrderedMap()
// condMap.Set("name", "<value>")
// err := q.ReadWithCond(&tables, "", condMap)
func (q *Q) ReadWithCond(model interface{}, orderBy string, condMap *orderedmap.OrderedMap) error {
	return q.readWithCond(model, orderBy, condMap)
}

// Insert function
// INSERT INTO model (...) values (...)
// Insert one or more record into the model table
// Return inserted record(s) and set to model
// model: struct or array of struct
// ignoreFields: List of fields that you don't want to store

// var tables []Table{}{Table{Field: "A"}, Table{Field: "B"}}
// err := q.Insert(ctx, &tables)
func (q *Q) Insert(ctx context.Context, model interface{}, ignoreFields ...string) error {
	return q.insert(q.DB, ctx, model, ignoreFields...)
}

func (q *Q) TxInsert(tx *sql.Tx, ctx context.Context, model interface{}, ignoreFields ...string) error {
	return q.insert(tx, ctx, model, ignoreFields...)
}

func (q *Q) InsertWithID(ctx context.Context, model interface{}, ignoreFields ...string) error {
	return q.insertWithID(q.DB, ctx, model, ignoreFields...)
}

func (q *Q) TxInsertWithID(tx *sql.Tx, ctx context.Context, model interface{}, ignoreFields ...string) error {
	return q.insertWithID(tx, ctx, model, ignoreFields...)
}

// Update function
// UPDATE model SET ... WHERE ...
// Update one or more record(s) of model table
// Return updated record set to model
// model: struct or array of struc
// condFields: conditional fields

// var tables []Table{}{Table{Field: "A"}, Table{Field: "B"}}
// err := q.Remove(&tables, []string{"field"})
func (q *Q) Update(ctx context.Context, model interface{}, condFields []string, ignoreFields ...string) error {
	return q.updateOneOrMany(q.DB, ctx, model, condFields, ignoreFields...)
}

func (q *Q) TxUpdate(tx *sql.Tx, ctx context.Context, model interface{}, condFields []string, ignoreFields ...string) error {
	return q.updateOneOrMany(tx, ctx, model, condFields, ignoreFields...)
}

func (q *Q) UpdateWithID(ctx context.Context, model interface{}, ignoreFields ...string) error {
	return q.update(q.DB, ctx, model, []string{"id"}, ignoreFields...)
}

func (q *Q) TxUpdateWithID(tx *sql.Tx, ctx context.Context, model interface{}, ignoreFields ...string) error {
	return q.update(tx, ctx, model, []string{"id"}, ignoreFields...)
}

func (q *Q) UpdateWithCode(ctx context.Context, model interface{}, ignoreFields ...string) error {
	return q.update(q.DB, ctx, model, []string{"code"}, ignoreFields...)
}

func (q *Q) TxUpdateWithCode(tx *sql.Tx, ctx context.Context, model interface{}, ignoreFields ...string) error {
	return q.update(tx, ctx, model, []string{"code"}, ignoreFields...)
}

func (q *Q) UpdateWithCond(ctx context.Context, model interface{}, condMap *orderedmap.OrderedMap, ignoreFields ...string) error {
	return q.updateWithCond(q.DB, ctx, model, condMap, ignoreFields...)
}

func (q *Q) TxUpdateWithCond(tx *sql.Tx, ctx context.Context, model interface{}, condMap *orderedmap.OrderedMap, ignoreFields ...string) error {
	return q.updateWithCond(tx, ctx, model, condMap, ignoreFields...)
}

// UpdateWithWhere function
// UPDATE model SET ... WHERE ...
// Update one or more record(s) of model table
// Return updated record set to model
// model: struct or array of struct
// whereCond: condition
// params: list of values

// var tables []Table{}
// err := q.UpdateWithWhere(ctx, &tables, "code = $1 AND deleted_by = $2", []interface{}{"abc", 1111})
func (q *Q) UpdateWithWhere(ctx context.Context, model interface{}, whereCond string, params []interface{}, ignoreFields ...string) error {
	return q.updateWithWhere(q.DB, ctx, model, whereCond, params, ignoreFields...)
}

func (q *Q) TxUpdateWithWhere(tx *sql.Tx, ctx context.Context, model interface{}, whereCond string, params []interface{}, ignoreFields ...string) error {
	return q.updateWithWhere(tx, ctx, model, whereCond, params, ignoreFields...)
}

// UpdateFieldsWithExtCond function
// role := Role{
// 	Name: skyutl.AddrOfString("abcxxxxxxx"),
// }

// fieldMap := orderedmap.NewOrderedMap()
// condMap := orderedmap.NewOrderedMap()
// fieldMap.Set("version", 1)
// condMap.Set("id != ALL", pq.Array([]int64{2592762348913559961, 2592762579189238165}))
// condMap.Set("disabled = ", false)
// if err := q.UpdateFieldsWithExtCond(context.Background(), &role, fieldMap, condMap); err != nil {
// 	fmt.Println(err)
// }
func (q *Q) UpdateFieldsWithExtCond(ctx context.Context, model interface{}, fieldMap *orderedmap.OrderedMap, condMap *orderedmap.OrderedMap) error {
	return q.updateFieldsWithExtCond(q.DB, ctx, model, fieldMap, condMap)
}

func (q *Q) TxUpdateFieldsWithExtCond(tx *sql.Tx, ctx context.Context, model interface{}, fieldMap *orderedmap.OrderedMap, condMap *orderedmap.OrderedMap) error {
	return q.updateFieldsWithExtCond(tx, ctx, model, fieldMap, condMap)
}

// Delete function
// UPDATE model SET deleted_by = $1, deleted_at = $2 [WHERE ...]
// Make one, many or all record(s) into deleted state
// Return deleted record(s) set to model
// model: struct or array of struct
// condFields: conditional fields

// var tables []Table{}{Table{Field: "A"}, Table{Field: "B"}}
// err := q.Delete(ctx, &tables, []string{"field"})
func (q *Q) Delete(ctx context.Context, model interface{}, condFields []string) error {
	return q.delete(q.DB, ctx, model, condFields)
}

func (q *Q) TxDelete(tx *sql.Tx, ctx context.Context, model interface{}, condFields []string) error {
	return q.delete(tx, ctx, model, condFields)
}

// DeleteWithID function
// UPDATE model SET deleted_by = $1, deleted_at = $2 [WHERE ...]
// Make one, many or all record(s) into deleted state
// Return deleted record(s) and set to model
// model: struct or array of struct

// var tables []Table{}{Table{Id: 1}, Table{Id: 2}}
// err := q.DeleteWithID(ctx, &tables)
func (q *Q) DeleteWithID(ctx context.Context, model interface{}) error {
	return q.delete(q.DB, ctx, model, []string{"id"})
}

func (q *Q) TxDeleteWithID(tx *sql.Tx, ctx context.Context, model interface{}) error {
	return q.delete(tx, ctx, model, []string{"id"})
}

// DeleteWithCond function
// UPDATE model SET deleted_by = $1, deleted_at = $2 [WHERE ...]
// Make one, many or all record(s) into deleted state
// Return deleted record(s) and set to model
// model: struct or array of struct
// condFields: conditional fields

// var tables []Table{}{Table{Name: "A"}, Table{Name: "B"}}
// condMap := orderedmap.NewOrderedMap()
// condMap.Set("name", "<value>")
// err := q.DeleteWithCond(ctx, &tables, condMap)
func (q *Q) DeleteWithCond(ctx context.Context, model interface{}, condMap *orderedmap.OrderedMap) error {
	return q.deleteWithCond(q.DB, ctx, model, condMap)
}

func (q *Q) TxDeleteWithCond(tx *sql.Tx, ctx context.Context, model interface{}, condMap *orderedmap.OrderedMap) error {
	return q.deleteWithCond(tx, ctx, model, condMap)
}

// Remove function
// DELETE FROM model WHERE ...
// Delete one or more record(s) of model table
// Return deleted record(s) and set to model
// model: struct or array of struct
// condFields: conditional fields

// var tables []Table{}{Table{Field: "A"}, Table{Field: "B"}}
// err := q.Remove(&tables, []string{"field"})
func (q *Q) Remove(model interface{}, condFields []string) error {
	return q.remove(q.DB, model, condFields)
}

func (q *Q) TxRemove(tx *sql.Tx, model interface{}, condFields []string) error {
	return q.remove(tx, model, condFields)
}

func (q *Q) RemoveWithID(model interface{}) error {
	return q.remove(q.DB, model, []string{"id"})
}

func (q *Q) TxRemoveWithID(tx *sql.Tx, model interface{}) error {
	return q.remove(tx, model, []string{"id"})
}

func (q *Q) RemoveWithCond(model interface{}, condMap *orderedmap.OrderedMap) error {
	return q.removeWithCond(q.DB, model, condMap)
}

func (q *Q) TxRemoveWithCond(tx *sql.Tx, model interface{}, condMap *orderedmap.OrderedMap) error {
	return q.removeWithCond(tx, model, condMap)
}

// Upsert function
// Insert or update base on condFields
// Return the upserted record and set to model
// model: struct
// condFields: Conditional fields
// ignoreFields: List of fields that you don't want to store

// var table Table{Id: 1, Field: "A"}
// err := q.Upsert(ctx, &table, []string{"id"})
func (q *Q) Upsert(ctx context.Context, model interface{}, condFields []string, ignoreFields ...string) error {
	return q.upsertMany(q.DB, ctx, model, condFields, ignoreFields...)
}

func (q *Q) TxUpsert(tx *sql.Tx, ctx context.Context, model interface{}, condFields []string, ignoreFields ...string) error {
	return q.upsertMany(tx, ctx, model, condFields, ignoreFields...)
}

// UpsertWithID function
// Insert or update base on ID. ID > 0: Update, ID = 0: Insert
// Return the upserted record and set to model
// model: struct
// ignoreFields: List of fields that you don't want to store
// var table Table{Id: 1, Field: "A"}
// err := q.UpsertWithID(ctx, &table, []string{})
func (q *Q) UpsertWithID(ctx context.Context, model interface{}, ignoreFields ...string) error {
	return q.upsertMany(q.DB, ctx, model, []string{"id"}, ignoreFields...)
}

func (q *Q) Test() {
	sql := `
		update role set code = $1 where id = $2;
		update role set code = $1 where id = $2;
	`

	q.Exec(sql, "abcdsdfsd", 2800737722178734175)
}

func (q *Q) UpsertWithIDValidateCodeRequire(ctx context.Context, model interface{}, ignoreFields ...string) error {
	modelValue, err := checkIsAPointerToArrayOrStruct(model)
	if err != nil {
		return err
	}

	idField := modelValue.FieldByName("Id")
	var idValue int64
	if idField.CanInterface() {
		idValue = idField.Interface().(int64)
	}

	var codeValue string
	codeField := modelValue.FieldByName("Code")
	if codeField.CanInterface() {
		codeValue = codeField.Interface().(string)
	}

	if codeValue == "" {
		return skyutl.Error400("SYS.MSG.REQUIRED_VALUE", "code", nil)
	}

	tableName := GetTableName(model)

	if idValue != 0 { // update mode
		if isDuplicated, _ := IsTextValueDuplicated(tableName, "code", codeValue, idValue); isDuplicated > 0 {
			return skyutl.DuplicatedError("code")
		}
	} else { // insert mode
		if isExisted, _ := IsTextValueExisted(tableName, "code", codeValue); isExisted > 0 {
			return skyutl.ExistedError("code")
		}
	}

	if err := q.UpsertWithID(ctx, model); err != nil {
		return err
	}
	return nil
}

func (q *Q) UpsertWithIDValidateCodeNoRequire(ctx context.Context, model interface{}, ignoreFields ...string) error {
	modelValue, err := checkIsAPointerToArrayOrStruct(model)
	if err != nil {
		return err
	}

	idField := modelValue.FieldByName("Id")
	var idValue int64
	if idField.CanInterface() {
		idValue = idField.Interface().(int64)
	}

	var codeValue string
	codeField := modelValue.FieldByName("Code")
	if codeField.CanInterface() {
		codeValue = codeField.Interface().(string)
	}

	tableName := GetTableName(model)

	if idValue != 0 { // update mode
		if codeValue != "" {
			if isDuplicated, _ := IsTextValueDuplicated(tableName, "code", codeValue, idValue); isDuplicated > 0 {
				return skyutl.DuplicatedError("code")
			}
		}
	} else { // insert mode
		if codeValue != "" {
			if isExisted, _ := IsTextValueExisted(tableName, "code", codeValue); isExisted > 0 {
				return skyutl.ExistedError("code")
			}
		}

	}

	if err := q.UpsertWithID(ctx, model); err != nil {
		return err
	}
	return nil
}

func (q *Q) TxUpsertWithIDValidateCodeName(tx *sql.Tx, ctx context.Context, model interface{}, ignoreFields ...string) error {
	return q.upsertWithIDValidateCodeName(tx, ctx, model, ignoreFields...)
}

func (q *Q) UpsertWithIDValidateCodeName(ctx context.Context, model interface{}, ignoreFields ...string) error {
	return q.upsertWithIDValidateCodeName(q.DB, ctx, model, ignoreFields...)
}

func (q *Q) upsertWithIDValidateCodeName(datasource interface{}, ctx context.Context, model interface{}, ignoreFields ...string) error {
	modelValue, err := checkIsAPointerToArrayOrStruct(model)
	if err != nil {
		return err
	}

	idField := modelValue.FieldByName("Id")
	var idValue int64
	if idField.CanInterface() {
		idValue = idField.Interface().(int64)
	}

	var codeValue, nameValue string
	codeField := modelValue.FieldByName("Code")
	if codeField.CanInterface() {
		codeValue = codeField.Interface().(string)
	}

	nameField := modelValue.FieldByName("Name")
	if codeField.CanInterface() {
		nameValue = nameField.Interface().(string)
	}

	if nameValue == "" {
		return skyutl.Error400("SYS.MSG.REQUIRED_VALUE", "name", nil)
	}

	tableName := GetTableName(model)

	if idValue != 0 { // update mode
		if codeValue != "" {
			if isDuplicated, _ := IsTextValueDuplicated(tableName, "code", codeValue, idValue); isDuplicated > 0 {
				return skyutl.DuplicatedError("code")
			}
		}

		if isDuplicated, _ := IsTextValueDuplicated(tableName, "name", nameValue, idValue); isDuplicated > 0 {
			return skyutl.DuplicatedError("name")
		}
	} else { // insert mode
		if codeValue != "" {
			if isExisted, _ := IsTextValueExisted(tableName, "code", codeValue); isExisted > 0 {
				return skyutl.ExistedError("code")
			}
		}

		if isExisted, _ := IsTextValueExisted(tableName, "name", nameValue); isExisted > 0 {
			return skyutl.ExistedError("name")
		}
	}

	if err := q.upsert(datasource, ctx, model, []string{"id"}, ignoreFields...); err != nil {
		return err
	}
	
	return nil
}

func (q *Q) TxUpsertWithID(tx *sql.Tx, ctx context.Context, model interface{}, ignoreFields ...string) error {
	return q.upsert(tx, ctx, model, []string{"id"}, ignoreFields...)
}

func (q *Q) UpsertWithCond(ctx context.Context, model interface{}, condMap *orderedmap.OrderedMap, ignoreFields ...string) error {
	return q.upsertWithCond(q.DB, ctx, model, condMap, ignoreFields...)
}

func (q *Q) TxUpsertWithCond(tx *sql.Tx, ctx context.Context, model interface{}, condMap *orderedmap.OrderedMap, ignoreFields ...string) error {
	return q.upsertWithCond(tx, ctx, model, condMap, ignoreFields...)
}

// CallFunc function
// Call postgres function
// Return an error

// var rows []Table{}
// err := q.CallFunc("func_name", "", []interface{}{100, 200}, &rows, "")
func (q *Q) CallFunc(funcName, alias string, params []interface{}, out ...interface{}) error {
	return q.callFunc(q.DB, funcName, alias, params, out...)
}

func (q *Q) CallUnSecurityFunc(funcName, alias string, params []interface{}, out ...interface{}) error {
	return q.callUnSecurityFunc(q.DB, funcName, alias, params, out...)
}

func (q *Q) TxCallFunc(tx *sql.Tx, funcName, alias string, params []interface{}, out ...interface{}) error {
	return q.callFunc(tx, funcName, alias, params, out...)
}

func (q *Q) TxQuery(tx *sql.Tx, _sql string, params []interface{}, out ...interface{}) error {
	return q.query(tx, _sql, params, out...)
}

// CallFunc function
// Execute sql query with params list and set output model to out variable
// Return an error

// const sqlStr = "SELECT * FROM table1 WHERE id=$1 OR id=$2"
// var tables []Table{}
// err := q.Query(sql, []interface{}{100, 200}, &tables)
func (q *Q) Query(sql string, params []interface{}, out ...interface{}) error {
	return q.query(q.DB, sql, params, out...)
}

//TxExec function
//Transactional execuate sql query with or without params, return number of effected record
func (q *Q) TxExec(tx *sql.Tx, sql string, params ...interface{}) (int64, error) {
	return q.exec(tx, sql, params...)
}

//Exec function
//Execuate sql query with or without params, return number of effected record
func (q *Q) Exec(sql string, params ...interface{}) (int64, error) {
	return q.exec(q.DB, sql, params...)
}

// batch insert one or many
func (q *Q) BatchInsert(ctx context.Context, input interface{}, ignoreFields ...string) error {
	return q.batchInsert(q.DB, ctx, input, ignoreFields...)
}

// Transaction batch insert one or many
func (q *Q) TxBatchInsert(tx *sql.Tx, ctx context.Context, input interface{}, ignoreFields ...string) error {
	return q.batchInsert(tx, ctx, input, ignoreFields...)
}

func (q *Q) BatchUpdate(ctx context.Context, model interface{}, condFields []string, ignoreFields ...string) error {
	return q.batchUpdate(q.DB, ctx, model, condFields, ignoreFields...)
}

func (q *Q) TxBatchUpdate(tx *sql.Tx, ctx context.Context, model interface{}, condFields []string, ignoreFields ...string) error {
	return q.batchUpdate(tx, ctx, model, condFields, ignoreFields...)
}

func (q *Q) BatchDelete(ctx context.Context, model interface{}, condFields []string) error {
	return q.batchDelete(q.DB, ctx, model, condFields)
}

func (q *Q) TxBatchDelete(tx *sql.Tx, ctx context.Context, model interface{}, condFields []string) error {
	return q.batchDelete(tx, ctx, model, condFields)
}

func (q *Q) BatchUpsert(ctx context.Context, model interface{}, condFields []string, ignoreFields ...string) error {
	return q.batchUpsert(q.DB, ctx, model, condFields, ignoreFields...)
}

func (q *Q) TxBatchUpsert(tx *sql.Tx, ctx context.Context, model interface{}, condFields []string, ignoreFields ...string) error {
	return q.batchUpsert(tx, ctx, model, condFields, ignoreFields...)
}

func (q *Q) UpdateManyWithReturn(ctx context.Context, model interface{}, condFields []string, ignoreFields ...string) error {
	return q.updateManyWithReturn(q.DB, ctx, model, condFields, ignoreFields...)
}

func (q *Q) TxUpdateManyWithReturn(tx *sql.Tx, ctx context.Context, model interface{}, condFields []string, ignoreFields ...string) error {
	return q.updateManyWithReturn(tx, ctx, model, condFields, ignoreFields...)
}

func (q *Q) BatchUpsertWithReturn(ctx context.Context, model interface{}, condFields []string, ignoreFields ...string) error {
	return q.batchUpsertWithReturn(q.DB, ctx, model, condFields, ignoreFields...)
}

func (q *Q) TxBatchUpsertWithReturn(tx *sql.Tx, ctx context.Context, model interface{}, condFields []string, ignoreFields ...string) error {
	return q.batchUpsertWithReturn(tx, ctx, model, condFields, ignoreFields...)
}

func (q *Q) FindWorkListTesting(model interface{}, tableName string, columns ...string) error {

	if len(columns) == 0 {
		columns = []string{"id"}
	} else {
		columns = append(columns, "id")
	}

	sql := fmt.Sprintf(`
		SELECT %s, disabled, greatest(created_at, updated_at) as last_access
		FROM %s
		WHERE deleted_by = 0
		ORDER BY last_access DESC
	`, strings.Join(columns, ", "), tableName)

	if err := q.Query(sql, nil, model); err != nil {
		return err
	}

	return nil
}
