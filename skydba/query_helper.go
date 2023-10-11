package skydba

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	// "encoding/json"
	"os"

	"github.com/elliotchance/orderedmap"
	"github.com/lib/pq"
	"github.com/stoewer/go-strcase"
	"github.com/nkocsea/skylib_skylog/skylog"
	"github.com/nkocsea/skylib_skyutl/skyutl"
)

type outParamType int

const (
	aRRAY outParamType = iota
	sTRUCT
	bASIC
	eRROR
)

const (
	TABLE_NAME       = "SystemTableName"
	ID               = "Id"
	CREATED_AT_FIELD = "CreatedAt"
	CREATED_BY_FIELD = "CreatedBy"
	UPDATED_BY_FIELD = "UpdatedBy"
	UPDATED_AT_FIELD = "UpdatedAt"
	DELETED_BY_FIELD = "DeletedBy"
	DELETED_AT_FIELD = "DeletedAt"
)

var (
	SystemField    = []interface{}{TABLE_NAME, ID, CREATED_AT_FIELD, CREATED_BY_FIELD, UPDATED_BY_FIELD, UPDATED_AT_FIELD, DELETED_BY_FIELD, DELETED_AT_FIELD}
	DisableInfoLog = false
)

func checkIsAPointerToArrayOrStruct(model interface{}) (reflect.Value, error) {
	modelValue := reflect.ValueOf(model)

	if modelValue.Kind() != reflect.Ptr {
		return reflect.Value{}, errors.New("require a pointer to a struct or an array of a struct")
	} else {
		modelValue = modelValue.Elem()
		if modelValue.Kind() == reflect.Ptr {
			return reflect.Value{}, errors.New("require a pointer to a struct or an array of a struct. Not a pointer to a pointer")
		}

		// TODO
		if modelValue.Kind() != reflect.Slice && modelValue.Kind() != reflect.Struct {
			return reflect.Value{}, errors.New("require a pointer to a struct or an array of a struct")
		}
	}

	return modelValue, nil
}

func GetTableName(model interface{}) string {
	tableName := skyutl.GetFieldValueOfStruct(model, TABLE_NAME)
	if tableName == nil || tableName == "" {
		protobufTag := skyutl.GetFieldTagValueOfStruct(model, TABLE_NAME, "protobuf")
		splits := strings.Split(protobufTag, ",")
		for _, split := range splits {
			if strings.Contains(split, "table_name=") {
				s := strings.Split(split, "=")
				if len(s) > 1 {
					return s[1]
				}
			}
		}

		tableName = skyutl.GetFieldTagValueOfStruct(model, TABLE_NAME, "default")
		if tableName == "" {
			tableName, _ = skyutl.GetStructNameInSnakeCase(model)
		}

	}
	return tableName.(string)
}

//getFieldValueOfStruct function
func getFieldValueOfStructByValue(values reflect.Value, fieldName string) interface{} {
	if values.Kind() == reflect.Ptr {
		values = values.Elem()
	}

	if values.Kind() == reflect.Slice {
		values = values.Index(0)
	}

	if values.Kind() == reflect.Ptr {
		values = values.Elem()
	}

	field := values.FieldByName(fieldName)

	if !field.IsValid() {
		return nil
	}

	if field.Kind() == reflect.Ptr {
		if !field.IsNil() {
			return reflect.Indirect(field).Interface()
		} else {
			return nil
		}

	} else {
		return field.Interface()
	}
}

func GetTableNameByValue(model reflect.Value) string {
	tableName := getFieldValueOfStructByValue(model, TABLE_NAME)
	if tableName == nil || tableName == "" {
		protobufTag := skyutl.GetFieldTagValueOfStruct(model.Interface(), TABLE_NAME, "protobuf")
		splits := strings.Split(protobufTag, ",")
		for _, split := range splits {
			if strings.Contains(split, "table_name=") {
				s := strings.Split(split, "=")
				if len(s) > 1 {
					return s[1]
				}
			}
		}

		tableName = skyutl.GetFieldTagValueOfStruct(model, TABLE_NAME, "default")
		if tableName == "" {
			tableName = strcase.SnakeCase(skyutl.GetStructNameFromValue(model))
		}

	}
	return tableName.(string)
}

func (q *Q) read(model interface{}, orderBy string, condFields []string) error {
	modelValue, err := checkIsAPointerToArrayOrStruct(model)
	if err != nil {
		return err
	}
	var values []interface{}
	var cond string
	tableName := GetTableName(model)
	if modelValue.Kind() == reflect.Struct {
		values, cond = makeConditionValue(1, modelValue, condFields...)
	} else if modelValue.Kind() == reflect.Slice {
		values, cond = makeConditionManyValue(modelValue, condFields...)
	}
	var sqlStr string
	if strings.Trim(cond, " ") != "" {
		sqlStr = "SELECT * FROM " + tableName + " WHERE " + cond
	} else {
		sqlStr = "SELECT * FROM " + tableName
	}

	if orderBy != "" {
		sqlStr += " ORDER BY " + orderBy
	}

	skyutl.ResetSliceOrStruct(model)
	return q.Query(sqlStr, values, model)
}

func (q *Q) readWithCond(model interface{}, orderBy string, condMap *orderedmap.OrderedMap) error {
	if _, err := checkIsAPointerToArrayOrStruct(model); err != nil {
		return err
	}

	tableName := GetTableName(model)
	values, cond := makeConditionMap(1, condMap)
	sqlStr := "SELECT * FROM " + tableName

	if strings.Trim(cond, " ") != "" {
		sqlStr += " WHERE " + cond
	}

	if strings.Trim(orderBy, " ") != "" {
		sqlStr += " ORDER BY " + orderBy
	}

	skyutl.ResetSliceOrStruct(model)
	return q.Query(sqlStr, values, model)
}

func (q *Q) delete(dataSource interface{}, ctx context.Context, model interface{}, condFields []string) error {
	modelValue, err := checkIsAPointerToArrayOrStruct(model)
	if err != nil {
		return err
	}

	if err := q.read(model, "", condFields); err != nil {
		return err
	}

	var values []interface{}
	var cond string
	tableName := GetTableName(model)

	if modelValue.Kind() == reflect.Struct {
		values, cond = makeConditionValue(1, modelValue, condFields...)
	} else if modelValue.Kind() == reflect.Slice {
		values, cond = makeConditionManyValue(modelValue, condFields...)
	}

	userID, err := skyutl.GetUserID(ctx)
	if err != nil {
		return err
	}

	var sqlStr string
	if strings.Trim(cond, " ") != "" {
		sqlStr = fmt.Sprintf("UPDATE %v SET deleted_by = %v, deleted_at = date_generator() WHERE %v RETURNING *", tableName, userID, cond)
	} else {
		sqlStr = fmt.Sprintf("UPDATE %v SET deleted_by = %v, deleted_at = date_generator() RETURNING *", tableName, userID)
	}

	if reflect.TypeOf(dataSource).String() == "*sql.Tx" {
		return q.TxQuery(dataSource.(*sql.Tx), sqlStr, values, &model)
	} else {
		return q.Query(sqlStr, values, &model)
	}
}

func (q *Q) deleteWithCond(dataSource interface{}, ctx context.Context, model interface{}, condMap *orderedmap.OrderedMap) error {
	if _, err := checkIsAPointerToArrayOrStruct(model); err != nil {
		return err
	}

	if err := q.readWithCond(model, "", condMap); err != nil {
		return err
	}

	tableName := GetTableName(model)

	values, cond := makeConditionMap(1, condMap)

	userID, err := skyutl.GetUserID(ctx)
	if err != nil {
		return err
	}

	var sqlStr string
	if strings.Trim(cond, " ") != "" {
		sqlStr = fmt.Sprintf("UPDATE %v SET deleted_by = %v, deleted_at = date_generator() WHERE %v RETURNING *", tableName, userID, cond)
	} else {
		sqlStr = fmt.Sprintf("UPDATE %v SET deleted_by = %v, deleted_at = date_generator() RETURNING *", tableName, userID)
	}

	if reflect.TypeOf(dataSource).String() == "*sql.Tx" {
		return q.TxQuery(dataSource.(*sql.Tx), sqlStr, values, &model)
	} else {
		return q.Query(sqlStr, values, &model)
	}
}

func (q *Q) insert(dataSource interface{}, ctx context.Context, input interface{}, ignoreFields ...string) error {

	if _, err := checkIsAPointerToArrayOrStruct(input); err != nil {
		return err
	}

	typeOf := reflect.TypeOf(input)
	if typeOf.Kind() == reflect.Ptr {
		typeOf = typeOf.Elem()
	}

	if typeOf.Kind() == reflect.Slice {
		valueOf := reflect.ValueOf(input)
		if valueOf.Kind() == reflect.Ptr {
			valueOf = valueOf.Elem()
		}

		if valueOf.Len() == 0 {
			return nil
		}

	}

	tableName := GetTableName(input)
	ignoreFieldsMap := skyutl.ToMap(true, ignoreFields...)

	cols, _ := getFieldListOfStruct(input, ignoreFieldsMap, true)

	cols = append([]string{"created_by"}, cols...)

	_sql := []string{"INSERT INTO " + tableName + "(" + strings.Join(cols, ", ") + ") values "}

	params, data := makeInsertFields(ctx, input, ignoreFieldsMap, true)

	_sql = append(_sql, strings.Join(params, ", "))

	_sql = append(_sql, "RETURNING *")

	sqlStr := strings.Join(_sql, " ")

	data = escapeParam(data)
	logSQLAndParam(sqlStr, data)

	var rows *sql.Rows
	var err error
	if reflect.TypeOf(dataSource).String() == "*sql.Tx" {
		rows, err = dataSource.(*sql.Tx).Query(sqlStr, data...)

	} else {
		rows, err = dataSource.(*sql.DB).Query(sqlStr, data...)
	}

	if err != nil {
		skylog.Error(err)
		return err
	}
	defer rows.Close()

	return scanAndFill(input, rows)
}

func (q *Q) insertByValue(dataSource interface{}, ctx context.Context, input reflect.Value, ignoreFields ...string) error {
	valueOf := input
	if valueOf.Kind() == reflect.Ptr {
		valueOf = valueOf.Elem()
	}

	if valueOf.Kind() == reflect.Slice {
		if valueOf.Len() == 0 {
			return nil
		}
	}

	tableName := GetTableNameByValue(input)
	ignoreFieldsMap := skyutl.ToMap(true, ignoreFields...)

	cols, _ := getFieldListOfStructByValue(input, ignoreFieldsMap, false)

	cols = append([]string{"created_by"}, cols...)

	_sql := []string{"WITH test AS (INSERT INTO " + tableName + "(" + strings.Join(cols, ", ") + ") VALUES "}

	paramValues := makeUpdateManyValuesByValue(input, ignoreFieldsMap, true)

	_sql = append(_sql, strings.Join(paramValues, ", "))

	_sql = append(_sql, "RETURNING * ) SELECT * FROM test ORDER BY id")

	sqlStr := strings.Join(_sql, " ")
	data := []interface{}{}
	data = escapeParam(data)
	logSQLAndParam(sqlStr, data)

	sqlStr = strings.ReplaceAll(sqlStr, "%", "pt")

	f, err1 := os.OpenFile("./test_sql.txt", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0777)
	if err1 != nil {
		fmt.Println(err1)
	}
	defer f.Close()
	if _, err := f.WriteString(sqlStr); err != nil {
		fmt.Println(err)
	}

	var rows *sql.Rows
	var err error
	if reflect.TypeOf(dataSource).String() == "*sql.Tx" {
		rows, err = dataSource.(*sql.Tx).Query(sqlStr, data...)
	} else {
		rows, err = dataSource.(*sql.DB).Query(sqlStr, data...)
	}

	if err != nil {
		skylog.Error(err)
		return err
	}
	defer rows.Close()

	return scanAndFillByValue(input, rows)
}

func (q *Q) insertWithID(dataSource interface{}, ctx context.Context, input interface{}, ignoreFields ...string) error {
	if _, err := checkIsAPointerToArrayOrStruct(input); err != nil {
		return err
	}

	tableName := GetTableName(input)
	ignoreFieldsMap := skyutl.ToMap(true, ignoreFields...)

	cols, _ := getFieldListOfStruct(input, ignoreFieldsMap, true)

	cols = append([]string{"created_by"}, cols...)

	_sql := []string{"INSERT INTO " + tableName + "(" + strings.Join(cols, ", ") + ") values "}

	params, data := makeInsertFields(ctx, input, ignoreFieldsMap, true)

	_sql = append(_sql, strings.Join(params, ", "))

	_sql = append(_sql, "RETURNING *")

	sqlStr := strings.Join(_sql, " ")
	data = escapeParam(data)
	logSQLAndParam(sqlStr, data)

	var rows *sql.Rows
	var err error
	if reflect.TypeOf(dataSource).String() == "*sql.Tx" {
		rows, err = dataSource.(*sql.Tx).Query(sqlStr, data...)

	} else {
		rows, err = dataSource.(*sql.DB).Query(sqlStr, data...)
	}

	if err != nil {
		skylog.Error(err)
		return err
	}
	defer rows.Close()

	return scanAndFill(input, rows)
}

func (q *Q) remove(dataSource interface{}, model interface{}, condFields []string) error {
	modelValue, err := checkIsAPointerToArrayOrStruct(model)
	if err != nil {
		return err
	}

	var values []interface{}
	var cond string
	tableName := GetTableName(model)

	if modelValue.Kind() == reflect.Struct {
		values, cond = makeConditionValue(1, modelValue, condFields...)
	} else if modelValue.Kind() == reflect.Slice {
		values, cond = makeConditionManyValue(modelValue, condFields...)
	}

	var sqlStr string
	if strings.Trim(cond, " ") != "" {
		sqlStr = fmt.Sprintf("DELETE FROM %v WHERE %v RETURNING *", tableName, cond)
	} else {
		sqlStr = fmt.Sprintf("DELETE FROM %v RETURNING *", tableName)
	}
	skyutl.ResetSliceOrStruct(model)
	if reflect.TypeOf(dataSource).String() == "*sql.Tx" {
		return q.TxQuery(dataSource.(*sql.Tx), sqlStr, values, model)
	} else {
		return q.Query(sqlStr, values, model)
	}
}

func (q *Q) removeWithCond(dataSource interface{}, model interface{}, condMap *orderedmap.OrderedMap) error {
	if _, err := checkIsAPointerToArrayOrStruct(model); err != nil {
		return err
	}

	tableName := GetTableName(model)

	values, cond := makeConditionMap(1, condMap)

	sqlStr := fmt.Sprintf("DELETE FROM %v WHERE %v RETURNING *", tableName, cond)
	skyutl.ResetSliceOrStruct(model)
	if reflect.TypeOf(dataSource).String() == "*sql.Tx" {
		return q.TxQuery(dataSource.(*sql.Tx), sqlStr, values, model)
	} else {
		return q.Query(sqlStr, values, model)
	}
}

func isPointerToStruct(modelValue *reflect.Value) error {
	if modelValue.Kind() == reflect.Ptr {
		*modelValue = modelValue.Elem()
		if modelValue.Kind() == reflect.Slice {
			return errors.New("require a pointer to a struct")
		}
	} else {
		return errors.New("require a pointer to a struct")
	}
	return nil
}

func logSQLAndParam(sqlStr string, paramValues []interface{}) {
	if !DisableInfoLog {
		skylog.Infof(sqlStr)
		skylog.Infof("=============================BEGIN PARAMS=============================")
		for index, value := range paramValues {
			if len(fmt.Sprintf("%v", value)) > 1024 {
				skylog.Errorf("\t\t\t$%v: %v", index+1, "Value to large")
			} else {
				skylog.Errorf("\t\t\t$%v: %v", index+1, value)
			}

		}
		skylog.Infof("=============================END PARAMS=============================")

	}
}

func lenArrayOfStruct(input interface{}) int {
	valueOf := reflect.ValueOf(input)
	if valueOf.Kind() == reflect.Ptr {
		valueOf = valueOf.Elem()
	}

	if valueOf.Kind() == reflect.Slice {
		return valueOf.Len()
	} else {
		return -1
	}
}

func (q *Q) updateOneOrMany(dataSource interface{}, ctx context.Context, model interface{}, condFields []string, ignoreFields ...string) error {

	typeOf := reflect.TypeOf(model)
	if typeOf.Kind() == reflect.Ptr {
		typeOf = typeOf.Elem()
	}

	if typeOf.Kind() == reflect.Slice {
		valueOf := reflect.ValueOf(model)
		if valueOf.Kind() == reflect.Ptr {
			valueOf = valueOf.Elem()
		}

		for i := 0; i < valueOf.Len(); i++ {
			if valueOf.Index(i).Kind() == reflect.Ptr {
				if err := q.update(dataSource, ctx, valueOf.Index(i).Interface(), condFields, ignoreFields...); err != nil {
					return err
				}
			} else {
				if err := q.update(dataSource, ctx, valueOf.Index(i).Addr().Interface(), condFields, ignoreFields...); err != nil {
					return err
				}
			}
		}
		return nil
	} else {
		return q.update(dataSource, ctx, model, condFields, ignoreFields...)
	}
}

func (q *Q) update(dataSource interface{}, ctx context.Context, model interface{}, condFields []string, ignoreFields ...string) error {
	modelValue := reflect.ValueOf(model)
	if err := isPointerToStruct(&modelValue); err != nil {
		return err
	}

	tableName := GetTableName(model)
	ignoreFieldsMap := skyutl.ToMap(true, ignoreFields...)

	_sql := []string{"UPDATE " + tableName + " SET updated_by = $1, updated_at = date_generator(), "}

	updateFieldStr, updateValues := makeUpdateFields(model, ignoreFieldsMap)

	_sql = append(_sql, updateFieldStr)
	values, cond := makeConditionValue(len(updateValues)+2, modelValue, condFields...)

	if strings.Trim(cond, " ") != "" {
		_sql = append(_sql, " WHERE "+cond)
	}

	_sql = append(_sql, "RETURNING *")

	sqlStr := strings.Join(_sql, " ")

	var rows *sql.Rows
	var err error

	paramValues := append(updateValues, values...)

	userID, _ := skyutl.GetUserID(ctx)

	paramValues = append([]interface{}{userID}, paramValues...)

	paramValues = escapeParam(paramValues)
	logSQLAndParam(sqlStr, paramValues)

	if reflect.TypeOf(dataSource).String() == "*sql.Tx" {
		rows, err = dataSource.(*sql.Tx).Query(sqlStr, paramValues...)
	} else {
		rows, err = dataSource.(*sql.DB).Query(sqlStr, paramValues...)
	}

	if err != nil {
		skylog.Error(err)
		return err
	}
	defer rows.Close()
	return scanAndFill(model, rows)
}

func makeUpdateManyFields(cols []string) []string {
	results := []string{}
	for i := range cols {
		results = append(results, fmt.Sprintf("%v=tmp.%v", cols[i], cols[i]))
	}
	return results
}

func makeUpdateManyCond(tableName string, condFields []string) []string {
	results := []string{}
	for i := range condFields {
		results = append(results, fmt.Sprintf("%v.%v=tmp.%v", tableName, condFields[i], condFields[i]))
	}
	return results
}

func makeDeleteManyCond(tableName string, condFields []string) []string {
	results := []string{}
	for i := range condFields {
		results = append(results, fmt.Sprintf("%v=%v", condFields[i], condFields[i]))
	}
	return results
}

func (q *Q) updateManyWithReturn(dataSource interface{}, ctx context.Context, model interface{}, condFields []string, ignoreFields ...string) error {
	if _, err := checkIsAPointerToArrayOrStruct(model); err != nil {
		return err
	}

	typeOf := reflect.TypeOf(model)
	if typeOf.Kind() == reflect.Ptr {
		typeOf = typeOf.Elem()
	}

	if typeOf.Kind() == reflect.Slice {
		valueOf := reflect.ValueOf(model)
		if valueOf.Kind() == reflect.Ptr {
			valueOf = valueOf.Elem()
		}

		if valueOf.Len() == 0 {
			return nil
		}
	}

	tableName := GetTableName(model)
	ignoreFieldsMap := skyutl.ToMap(true, ignoreFields...)

	_sql := "UPDATE " + tableName + " SET "
	cols, _ := getFieldListOfStruct(model, ignoreFieldsMap, false)
	updateFields := makeUpdateManyFields(cols)
	if len(updateFields) > 0 {
		_sql += strings.Join(updateFields, ", ") + ","
	}
	_sql += " updated_by=$1, updated_at=date_generator() "
	paramValues := makeUpdateManyValues(model, ignoreFieldsMap, true)
	_sql += " FROM (VALUES " + strings.Join(paramValues, ", ") + ") AS tmp (id, " + strings.Join(cols, ", ") + ")"
	_sql += " WHERE " + strings.Join(makeUpdateManyCond(tableName, condFields), ", ")
	_sql += " RETURNING*"

	userID, _ := skyutl.GetUserID(ctx)
	data := []interface{}{userID}

	var rows *sql.Rows
	var err error

	logSQLAndParam(_sql, data)

	if reflect.TypeOf(dataSource).String() == "*sql.Tx" {
		rows, err = dataSource.(*sql.Tx).Query(_sql, data...)
	} else {
		rows, err = dataSource.(*sql.DB).Query(_sql, data...)
	}

	if err != nil {
		skylog.Error(err)
		return err
	}
	defer rows.Close()
	return scanAndFill(model, rows)
}

func (q *Q) updateManyWithReturnByValue(dataSource interface{}, ctx context.Context, model reflect.Value, condFields []string, ignoreFields ...string) error {
	valueOf := model
	if valueOf.Kind() == reflect.Ptr {
		valueOf = valueOf.Elem()
	}

	if valueOf.Kind() == reflect.Slice {
		if valueOf.Len() == 0 {
			return nil
		}
	}

	tableName := GetTableNameByValue(model)
	ignoreFieldsMap := skyutl.ToMap(true, ignoreFields...)

	_sql := "UPDATE " + tableName + " SET "
	cols, _ := getFieldListOfStructByValue(model, ignoreFieldsMap, false)
	updateFields := makeUpdateManyFields(cols)
	if len(updateFields) > 0 {
		_sql += strings.Join(updateFields, ", ") + ","
	}
	_sql += " updated_by=$1, updated_at=date_generator() "
	paramValues := makeUpdateManyValuesByValue(model, ignoreFieldsMap, true)
	_sql += " FROM (VALUES " + strings.Join(paramValues, ", ") + ") AS tmp (id, " + strings.Join(cols, ", ") + ")"
	_sql += " WHERE " + strings.Join(makeUpdateManyCond(tableName, condFields), ", ")
	_sql += " RETURNING*"

	userID, _ := skyutl.GetUserID(ctx)
	data := []interface{}{userID}

	var rows *sql.Rows
	var err error

	logSQLAndParam(_sql, data)

	if reflect.TypeOf(dataSource).String() == "*sql.Tx" {
		rows, err = dataSource.(*sql.Tx).Query(_sql, data...)
	} else {
		rows, err = dataSource.(*sql.DB).Query(_sql, data...)
	}

	if err != nil {
		skylog.Error(err)
		return err
	}
	defer rows.Close()
	return scanAndFillByValue(model, rows)
}

func (q *Q) deleteManyWithReturnByValue(dataSource interface{}, ctx context.Context, model reflect.Value, condFields []string) error {
	valueOf := model
	if valueOf.Kind() == reflect.Ptr {
		valueOf = valueOf.Elem()
	}

	if valueOf.Kind() == reflect.Slice {
		if valueOf.Len() == 0 {
			return nil
		}
	}

	tableName := GetTableNameByValue(model)

	_sql := "UPDATE " + tableName + " SET "
	// cols, _ := getFieldListOfStructByValue(*model, ignoreFieldsMap, false)
	// updateFields := makeUpdateManyFields(cols)
	// if (len(updateFields) > 0) {
	// 	_sql += strings.Join(updateFields, ", ") + ","
	// }
	_sql += " deleted_by=$1, deleted_at=date_generator() "
	paramValues := makeDeleteManyValuesByValue(model, condFields, true)
	_sql += " FROM (VALUES " + strings.Join(paramValues, ", ") + ") AS tmp (id)"
	_sql += " WHERE " + strings.Join(makeUpdateManyCond(tableName, condFields), ", ")
	_sql += " RETURNING*"

	userID, _ := skyutl.GetUserID(ctx)
	data := []interface{}{userID}

	var rows *sql.Rows
	var err error

	logSQLAndParam(_sql, data)

	if reflect.TypeOf(dataSource).String() == "*sql.Tx" {
		rows, err = dataSource.(*sql.Tx).Query(_sql, data...)
	} else {
		rows, err = dataSource.(*sql.DB).Query(_sql, data...)
	}

	if err != nil {
		skylog.Error(err)
		return err
	}
	defer rows.Close()
	return scanAndFillByValue(model, rows)
}

func (q *Q) updateWithCond(dataSource interface{}, ctx context.Context, model interface{}, condMap *orderedmap.OrderedMap, ignoreFields ...string) error {
	modelValue := reflect.ValueOf(model)
	if err := isPointerToStruct(&modelValue); err != nil {
		return err
	}

	tableName := GetTableName(model)
	ignoreFieldsMap := skyutl.ToMap(true, ignoreFields...)

	_sql := []string{"UPDATE " + tableName + " SET updated_by = $1, updated_at = date_generator(), "}

	updateFieldStr, updateValues := makeUpdateFields(model, ignoreFieldsMap)

	_sql = append(_sql, updateFieldStr)
	values, cond := makeConditionMap(len(updateValues)+2, condMap)

	if strings.Trim(cond, " ") != "" {
		_sql = append(_sql, " WHERE "+cond)
	}

	_sql = append(_sql, "RETURNING *")

	sqlStr := strings.Join(_sql, " ")

	var rows *sql.Rows
	var err error

	userID, _ := skyutl.GetUserID(ctx)
	paramValues := append(updateValues, values...)
	paramValues = append([]interface{}{userID}, paramValues...)

	paramValues = escapeParam(paramValues)
	logSQLAndParam(sqlStr, paramValues)

	if reflect.TypeOf(dataSource).String() == "*sql.Tx" {
		rows, err = dataSource.(*sql.Tx).Query(sqlStr, paramValues...)
	} else {
		rows, err = dataSource.(*sql.DB).Query(sqlStr, paramValues...)
	}

	if err != nil {
		skylog.Error(err)
		return err
	}
	defer rows.Close()
	return scanAndFill(model, rows)
}

func (q *Q) updateWithWhere(dataSource interface{}, ctx context.Context, model interface{}, whereCond string, params []interface{}, ignoreFields ...string) error {
	modelValue := reflect.ValueOf(model)
	if err := isPointerToStruct(&modelValue); err != nil {
		return err
	}

	tableName := GetTableName(model)
	ignoreFieldsMap := skyutl.ToMap(true, ignoreFields...)

	_sql := []string{fmt.Sprintf("UPDATE %v SET updated_by = $%v, updated_at = date_generator(), ", tableName, len(params)+1)}

	updateFieldStr, updateValues := makeUpdateFieldsWithParam(model, ignoreFieldsMap, len(params)+2)

	_sql = append(_sql, updateFieldStr)

	if strings.Trim(whereCond, " ") != "" {
		_sql = append(_sql, " WHERE "+whereCond)
	}

	_sql = append(_sql, "RETURNING *")

	sqlStr := strings.Join(_sql, " ")

	var rows *sql.Rows
	var err error

	userID, _ := skyutl.GetUserID(ctx)
	paramValues := params
	paramValues = append(paramValues, []interface{}{userID}...)
	paramValues = append(paramValues, updateValues...)

	paramValues = escapeParam(paramValues)
	logSQLAndParam(sqlStr, paramValues)

	if reflect.TypeOf(dataSource).String() == "*sql.Tx" {
		rows, err = dataSource.(*sql.Tx).Query(sqlStr, paramValues...)
	} else {
		rows, err = dataSource.(*sql.DB).Query(sqlStr, paramValues...)
	}

	if err != nil {
		skylog.Error(err)
		return err
	}
	defer rows.Close()
	return scanAndFill(model, rows)
}

func (q *Q) updateFieldsWithExtCond(dataSource interface{}, ctx context.Context, model interface{}, fieldMap *orderedmap.OrderedMap, condMap *orderedmap.OrderedMap) error {
	// modelValue := reflect.ValueOf(model)
	// if err := isPointerToStruct(&modelValue); err != nil {
	// 	return err
	// }

	if _, err := checkIsAPointerToArrayOrStruct(model); err != nil {
		return err
	}

	tableName := GetTableName(model)

	_sql := []string{"UPDATE " + tableName + " SET updated_by = $1, updated_at = date_generator(), "}

	updateValues, updateFieldStr := makeUpdateExtFields(fieldMap)

	_sql = append(_sql, updateFieldStr)
	values, cond := makeExtConditionMap(len(updateValues)+2, condMap)

	if strings.Trim(cond, " ") != "" {
		_sql = append(_sql, " WHERE "+cond)
	}

	_sql = append(_sql, "RETURNING *")

	sqlStr := strings.Join(_sql, " ")

	var rows *sql.Rows
	var err error

	paramValues := append(updateValues, values...)

	userID, _ := skyutl.GetUserID(ctx)
	paramValues = append([]interface{}{userID}, paramValues...)

	paramValues = escapeParam(paramValues)
	logSQLAndParam(sqlStr, paramValues)

	if reflect.TypeOf(dataSource).String() == "*sql.Tx" {
		rows, err = dataSource.(*sql.Tx).Query(sqlStr, paramValues...)
	} else {
		rows, err = dataSource.(*sql.DB).Query(sqlStr, paramValues...)
	}

	if err != nil {
		skylog.Error(err)
		return err
	}
	defer rows.Close()
	return scanAndFill(model, rows)
}

func (q *Q) upsert(dataSource interface{}, ctx context.Context, model interface{}, condFields []string, ignoreFields ...string) error {
	if len(condFields) == 0 {
		return skylog.ReturnError(errors.New("condFields is required"))
	}
	if _, err := checkIsAPointerToArrayOrStruct(model); err != nil {
		return err
	}

	updatedRows, err := q.execUpdate(ctx, dataSource, model, condFields, ignoreFields...)
	if err != nil {
		skylog.Error(err)
		return err
	}

	if updatedRows > 0 {
		return nil
	}

	return q.insert(dataSource, ctx, model, ignoreFields...)
}

func (q *Q) upsertMany(dataSource interface{}, ctx context.Context, model interface{}, condFields []string, ignoreFields ...string) error {
	typeOf := reflect.TypeOf(model)
	if typeOf.Kind() == reflect.Ptr {
		typeOf = typeOf.Elem()
	}

	if typeOf.Kind() == reflect.Slice {
		valueOf := reflect.ValueOf(model)
		if valueOf.Kind() == reflect.Ptr {
			valueOf = valueOf.Elem()
		}

		if valueOf.Len() == 0 {
			return nil
		}

		for i := 0; i < valueOf.Len(); i++ {
			if valueOf.Index(i).Kind() == reflect.Ptr {
				if err := q.upsert(dataSource, ctx, valueOf.Index(i).Interface(), condFields, ignoreFields...); err != nil {
					return err
				}
			} else {
				if err := q.upsert(dataSource, ctx, valueOf.Index(i).Addr().Interface(), condFields, ignoreFields...); err != nil {
					return err
				}
			}
		}
		return nil
	} else {
		return q.upsert(dataSource, ctx, model, condFields, ignoreFields...)
	}
}

func (q *Q) upsertWithCond(dataSource interface{}, ctx context.Context, model interface{}, condMap *orderedmap.OrderedMap, ignoreFields ...string) error {
	if _, err := checkIsAPointerToArrayOrStruct(model); err != nil {
		return err
	}

	updatedRows, err := q.execUpdateWithCond(ctx, dataSource, model, condMap, ignoreFields...)
	if err != nil {
		skylog.Error(err)
		return err
	}

	if updatedRows > 0 {
		return nil
	}

	return q.insert(dataSource, ctx, model, ignoreFields...)
}

func (q *Q) execUpdate(ctx context.Context, dataSource interface{}, model interface{}, condFields []string, ignoreFields ...string) (int64, error) {
	modelValue, err := checkIsAPointerToArrayOrStruct(model)
	if err != nil {
		return 0, err
	}

	tableName := GetTableName(model)

	ignoreFieldsMap := skyutl.ToMap(true, ignoreFields...)

	_sql := []string{"UPDATE " + tableName + " SET updated_by = $1, updated_at = date_generator(), "}

	updateFieldStr, updateValues := makeUpdateFields(model, ignoreFieldsMap)

	_sql = append(_sql, updateFieldStr)
	values, cond := makeConditionValue(len(updateValues)+2, modelValue, condFields...)

	if strings.Trim(cond, " ") != "" {
		_sql = append(_sql, " WHERE "+cond)
	}

	_sql = append(_sql, "RETURNING *")

	sqlStr := strings.Join(_sql, " ")

	var result sql.Result

	paramValues := append(updateValues, values...)

	userID, _ := skyutl.GetUserID(ctx)
	paramValues = append([]interface{}{userID}, paramValues...)

	paramValues = escapeParam(paramValues)
	logSQLAndParam(sqlStr, paramValues)

	if reflect.TypeOf(dataSource).String() == "*sql.Tx" {
		result, err = dataSource.(*sql.Tx).Exec(sqlStr, paramValues...)
	} else {
		result, err = dataSource.(*sql.DB).Exec(sqlStr, paramValues...)
	}

	if err != nil {
		skylog.Error(err)
		return 0, err
	}

	rowsAffected, _ := result.RowsAffected()

	return rowsAffected, nil
}

func (q *Q) execUpdateWithCond(ctx context.Context, dataSource interface{}, input interface{}, cond *orderedmap.OrderedMap, ignoreFields ...string) (int64, error) {

	if !skyutl.IsStructOrPtrToStruct(input) {
		return 0, errors.New("[input] param must be struct or pointer to struct")
	}

	tableName := GetTableName(input)
	ignoreFieldsMap := skyutl.ToMap(true, ignoreFields...)

	_sql := []string{"UPDATE " + tableName + " SET updated_by = $1, updated_at = date_generator(), "}

	updateFieldStr, updateValues := makeUpdateFields(input, ignoreFieldsMap)

	_sql = append(_sql, updateFieldStr)
	whereFieldStr, condValues := makeWhereCond(cond, len(updateValues)+1)

	if strings.Trim(whereFieldStr, " ") != "" {
		_sql = append(_sql, " WHERE "+whereFieldStr)
	}

	_sql = append(_sql, "RETURNING *")

	sqlStr := strings.Join(_sql, " ")

	var result sql.Result
	var err error

	paramValues := append(updateValues, condValues...)

	userID, _ := skyutl.GetUserID(ctx)
	paramValues = append([]interface{}{userID}, paramValues...)

	paramValues = escapeParam(paramValues)
	logSQLAndParam(sqlStr, paramValues)

	if reflect.TypeOf(dataSource).String() == "*sql.Tx" {
		result, err = dataSource.(*sql.Tx).Exec(sqlStr, paramValues...)
	} else {
		result, err = dataSource.(*sql.DB).Exec(sqlStr, paramValues...)
	}

	if err != nil {
		skylog.Error(err)
		return 0, err
	}

	rowsAffected, _ := result.RowsAffected()

	return rowsAffected, nil
}

func (q *Q) query(dataSource interface{}, _sql string, params []interface{}, out ...interface{}) error {
	paramType, err := checkOutParam(out)
	if err != nil {
		skylog.Error(err)
		return err
	}

	params = escapeParam(params)
	logSQLAndParam(_sql, params)

	var rows *sql.Rows

	if reflect.TypeOf(dataSource).String() == "*sql.Tx" {
		rows, err = dataSource.(*sql.Tx).Query(_sql, params...)
	} else {
		rows, err = dataSource.(*sql.DB).Query(_sql, params...)
	}

	if err != nil {
		skylog.Error(err)
		return err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		skylog.Error(err)
		return err
	}

	count := len(columns)

	for rows.Next() {
		values := make([]interface{}, count)
		valuePtrs := make([]interface{}, count)
		for i := range columns {
			valuePtrs[i] = &values[i]
		}
		rows.Scan(valuePtrs...)
		fields := []string{}
		for _, col := range columns {
			// val := values[i]

			// switch val.(type) {
			// case []uint8:
			// 	b, ok := val.([]byte)
			// 	if ok {
			// 		v := string(b)
			// 		values[i] = v
			// 	} 
			// }
			fields = append(fields, col)
		}

		fillOut(fields, values, out)
		if paramType != aRRAY {
			break
		}
	}

	return nil
}

func (q *Q) unSecurityQuery(dataSource interface{}, _sql string, params []interface{}, out ...interface{}) error {
	paramType, err := checkOutParam(out)
	if err != nil {
		skylog.Error(err)
		return err
	}

	//params = escapeParam(params)
	logSQLAndParam(_sql, params)

	var rows *sql.Rows

	if reflect.TypeOf(dataSource).String() == "*sql.Tx" {
		rows, err = dataSource.(*sql.Tx).Query(_sql, params...)
	} else {
		rows, err = dataSource.(*sql.DB).Query(_sql, params...)
	}

	if err != nil {
		skylog.Error(err)
		return err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		skylog.Error(err)
		return err
	}

	count := len(columns)

	for rows.Next() {
		values := make([]interface{}, count)
		valuePtrs := make([]interface{}, count)
		for i := range columns {
			valuePtrs[i] = &values[i]
		}
		rows.Scan(valuePtrs...)
		fields := []string{}
		for _, col := range columns {
			// val := values[i]

			// switch val.(type) {
			// case []uint8:
			// 	b, ok := val.([]byte)
			// 	if ok {
			// 		v := string(b)
			// 		values[i] = v
			// 	} 
			// }
			fields = append(fields, col)
		}
		fillOut(fields, values, out)
		if paramType != aRRAY {
			break
		}
	}

	return nil
}

func (q *Q) exec(dataSource interface{}, _sql string, params ...interface{}) (int64, error) {
	var ret sql.Result
	var err error
	params = escapeParam(params)
	if reflect.TypeOf(dataSource).String() == "*sql.Tx" {
		ret, err = dataSource.(*sql.Tx).Exec(_sql, params...)
	} else {
		ret, err = dataSource.(*sql.DB).Exec(_sql, params...)
	}

	if err != nil {
		skylog.Error(err)
		return 0, err
	}

	rowsAffected, _ := ret.RowsAffected()
	return rowsAffected, nil
}

func makeInsertFields(ctx context.Context, input interface{}, ignoreFields map[string]interface{}, includeID bool) ([]string, []interface{}) {
	userID, _ := skyutl.GetUserID(ctx)

	inputValue := reflect.ValueOf(input)

	if inputValue.Kind() == reflect.Ptr {
		inputValue = inputValue.Elem()
	}

	var sliceType reflect.Type
	if inputValue.Kind() == reflect.Slice {
		sliceType = inputValue.Type()
	}

	structType := inputValue.Type()
	if sliceType != nil {
		structType = sliceType.Elem()

		if structType.Kind() == reflect.Ptr {
			structType = structType.Elem()
		}
	}
	values := []interface{}{}
	cols := []string{}
	count := 2 //$1: created_by

	if inputValue.Kind() == reflect.Slice {
		for i := 0; i < inputValue.Len(); i++ {
			v := inputValue.Index(i)
			if v.Kind() == reflect.Ptr {
				v = v.Elem()
			}

			param := fmt.Sprintf("($%v, ", count-1)
			values = append(values, []interface{}{userID}...)
			for j := 0; j < structType.NumField(); j++ {
				fieldName := structType.Field(j).Name
				snakeCaseFieldName := strcase.SnakeCase(fieldName)
				if v.Field(j).CanInterface() && ((includeID && fieldName == "Id" && ((v.Field(j).Kind() != reflect.Ptr && (v.Field(j).Interface()).(int64) > 0) || (v.Field(j).Kind() == reflect.Ptr && (reflect.Indirect(v.Field(j)).Interface()).(int64) > 0))) || skyutl.FirstIndexOf(SystemField, fieldName) < 0) && (ignoreFields[snakeCaseFieldName] == nil || !ignoreFields[snakeCaseFieldName].(bool)) {

					_isTransient := skyutl.IsTransient(skyutl.GetFieldTagValueOfStruct(input, fieldName, "readonly"))

					if !_isTransient {
						param += fmt.Sprintf("$%v, ", (count))
						count++
						if v.Field(j).Kind() == reflect.Ptr {
							if v.Field(j).IsNil() {
								values = append(values, nil)
							} else {
								values = append(values, reflect.Indirect(v.Field(j)).Interface())
							}

						} else {
							values = append(values, v.Field(j).Interface())
						}
					}

				}
			}
			count += 1
			param = strings.TrimSuffix(param, ", ")
			param += ")"
			cols = append(cols, param)
		}
		return []string{strings.Join(cols, ", ")}, values
	} else {
		for j := 0; j < structType.NumField(); j++ {
			fieldName := structType.Field(j).Name
			snakeCaseFieldName := strcase.SnakeCase(fieldName)
			if inputValue.Field(j).CanInterface() && ((includeID && fieldName == "Id" && ((inputValue.Field(j).Kind() != reflect.Ptr && (inputValue.Field(j).Interface()).(int64) > 0) || (inputValue.Field(j).Kind() == reflect.Ptr && (reflect.Indirect(inputValue.Field(j)).Interface()).(int64)> 0)) ) || skyutl.FirstIndexOf(SystemField, fieldName) < 0) && (ignoreFields[snakeCaseFieldName] == nil || !ignoreFields[snakeCaseFieldName].(bool)) {
				_isTransient := skyutl.IsTransient(skyutl.GetFieldTagValueOfStruct(input, fieldName, "readonly"))
				if !_isTransient {
					cols = append(cols, fmt.Sprintf("$%v", (count)))
					count++

					if inputValue.Field(j).Kind() == reflect.Ptr {
						if inputValue.Field(j).IsNil() {
							values = append(values, nil)
						} else {
							values = append(values, reflect.Indirect(inputValue.Field(j)).Interface())
						}

					} else {
						values = append(values, inputValue.Field(j).Interface())
					}
				}

			}
		}
		cols = append([]string{"$1"}, cols...)
		values = append([]interface{}{userID}, values...)
		return []string{"(" + strings.Join(cols, ", ") + ")"}, values
	}
}

func makeUpdateManyValues(input interface{}, ignoreFields map[string]interface{}, includeID bool) []string {
	inputValue := reflect.ValueOf(input)

	if inputValue.Kind() == reflect.Ptr {
		inputValue = inputValue.Elem()
	}

	var sliceType reflect.Type
	if inputValue.Kind() == reflect.Slice {
		sliceType = inputValue.Type()
	}

	structType := inputValue.Type()
	if sliceType != nil {
		structType = sliceType.Elem()

		if structType.Kind() == reflect.Ptr {
			structType = structType.Elem()
		}
	}
	cols := []string{}

	if inputValue.Kind() == reflect.Slice {
		for i := 0; i < inputValue.Len(); i++ {
			v := inputValue.Index(i)
			if v.Kind() == reflect.Ptr {
				v = v.Elem()
			}

			param := "("
			for j := 0; j < structType.NumField(); j++ {
				fieldName := structType.Field(j).Name

				snakeCaseFieldName := strcase.SnakeCase(fieldName)
				if v.Field(j).CanInterface() && ((includeID && fieldName == "Id") || skyutl.FirstIndexOf(SystemField, fieldName) < 0) && (ignoreFields[snakeCaseFieldName] == nil || !ignoreFields[snakeCaseFieldName].(bool)) {
					_isTransient := skyutl.IsTransient(skyutl.GetFieldTagValueOfStruct(input, fieldName, "readonly"))

					if !_isTransient {
						if v.Field(j).Kind() == reflect.Ptr {
							if v.Field(j).IsNil() {
								param += "NULL, "
							} else {
								param += fmt.Sprintf("%v, ", reflect.Indirect(v.Field(j)).Interface())
							}

						} else {
							param += fmt.Sprintf("%v, ", GetSqlValue(v.Field(j).Interface()))
						}
					}

				}
			}
			param = strings.TrimSuffix(param, ", ")
			param += ")"
			cols = append(cols, param)
		}
		return []string{strings.Join(cols, ", ")}
	} else {
		for j := 0; j < structType.NumField(); j++ {
			fieldName := structType.Field(j).Name
			snakeCaseFieldName := strcase.SnakeCase(fieldName)
			if inputValue.Field(j).CanInterface() && ((includeID && fieldName == "Id") || skyutl.FirstIndexOf(SystemField, fieldName) < 0) && (ignoreFields[snakeCaseFieldName] == nil || !ignoreFields[snakeCaseFieldName].(bool)) {
				_isTransient := skyutl.IsTransient(skyutl.GetFieldTagValueOfStruct(input, fieldName, "readonly"))
				if !_isTransient {
					if inputValue.Field(j).Kind() == reflect.Ptr {
						if inputValue.Field(j).IsNil() {
							cols = append(cols, "NULL")
						} else {
							cols = append(cols, fmt.Sprintf("%v", reflect.Indirect(inputValue.Field(j)).Interface()))
						}

					} else {
						cols = append(cols, fmt.Sprintf("%v", inputValue.Field(j).Interface()))
					}
				}

			}
		}
		return []string{"(" + strings.Join(cols, ", ") + ")"}
	}
}

func makeUpdateManyValuesByValue(inputValue reflect.Value, ignoreFields map[string]interface{}, includeID bool) []string {
	input := inputValue.Interface()

	if inputValue.Kind() == reflect.Ptr {
		inputValue = inputValue.Elem()
	}

	var sliceType reflect.Type
	if inputValue.Kind() == reflect.Slice {
		sliceType = inputValue.Type()
	}

	structType := inputValue.Type()
	if sliceType != nil {
		structType = sliceType.Elem()

		if structType.Kind() == reflect.Ptr {
			structType = structType.Elem()
		}
	}
	cols := []string{}

	if inputValue.Kind() == reflect.Slice {
		for i := 0; i < inputValue.Len(); i++ {
			v := inputValue.Index(i)
			if v.Kind() == reflect.Ptr {
				v = v.Elem()
			}

			param := "("
			for j := 0; j < structType.NumField(); j++ {
				fieldName := structType.Field(j).Name

				snakeCaseFieldName := strcase.SnakeCase(fieldName)
				if v.Field(j).CanInterface() && ((includeID && fieldName == "Id") || skyutl.FirstIndexOf(SystemField, fieldName) < 0) && (ignoreFields[snakeCaseFieldName] == nil || !ignoreFields[snakeCaseFieldName].(bool)) {
					_isTransient := skyutl.IsTransient(skyutl.GetFieldTagValueOfStruct(input, fieldName, "readonly"))

					if !_isTransient {
						if v.Field(j).Kind() == reflect.Ptr {
							if v.Field(j).IsNil() {
								param += "NULL, "
							} else {
								param += fmt.Sprintf("%v, ", reflect.Indirect(v.Field(j)).Interface())
							}

						} else {
							param += fmt.Sprintf("%v, ", GetSqlValue(v.Field(j).Interface()))
						}
					}

				}
			}
			param = strings.TrimSuffix(param, ", ")
			param += ")"
			cols = append(cols, param)
		}
		return []string{strings.Join(cols, ", ")}
	} else {
		for j := 0; j < structType.NumField(); j++ {
			fieldName := structType.Field(j).Name
			snakeCaseFieldName := strcase.SnakeCase(fieldName)
			if inputValue.Field(j).CanInterface() && ((includeID && fieldName == "Id") || skyutl.FirstIndexOf(SystemField, fieldName) < 0) && (ignoreFields[snakeCaseFieldName] == nil || !ignoreFields[snakeCaseFieldName].(bool)) {
				_isTransient := skyutl.IsTransient(skyutl.GetFieldTagValueOfStruct(input, fieldName, "readonly"))
				if !_isTransient {
					if inputValue.Field(j).Kind() == reflect.Ptr {
						if inputValue.Field(j).IsNil() {
							cols = append(cols, "NULL")
						} else {
							cols = append(cols, fmt.Sprintf("%v", reflect.Indirect(inputValue.Field(j)).Interface()))
						}

					} else {
						cols = append(cols, fmt.Sprintf("%v", inputValue.Field(j).Interface()))
					}
				}

			}
		}
		return []string{"(" + strings.Join(cols, ", ") + ")"}
	}
}

func makeDeleteManyValuesByValue(inputValue reflect.Value, condFields []string, includeID bool) []string {
	input := inputValue.Interface()

	if inputValue.Kind() == reflect.Ptr {
		inputValue = inputValue.Elem()
	}

	var sliceType reflect.Type
	if inputValue.Kind() == reflect.Slice {
		sliceType = inputValue.Type()
	}

	structType := inputValue.Type()
	if sliceType != nil {
		structType = sliceType.Elem()

		if structType.Kind() == reflect.Ptr {
			structType = structType.Elem()
		}
	}
	cols := []string{}

	if inputValue.Kind() == reflect.Slice {
		for i := 0; i < inputValue.Len(); i++ {
			v := inputValue.Index(i)
			if v.Kind() == reflect.Ptr {
				v = v.Elem()
			}

			param := "("
			for j := 0; j < structType.NumField(); j++ {
				fieldName := structType.Field(j).Name

				if v.Field(j).CanInterface() && (includeID && fieldName == "Id") {
					_isTransient := skyutl.IsTransient(skyutl.GetFieldTagValueOfStruct(input, fieldName, "readonly"))

					if !_isTransient {
						if v.Field(j).Kind() == reflect.Ptr {
							if v.Field(j).IsNil() {
								param += "NULL, "
							} else {
								param += fmt.Sprintf("%v, ", reflect.Indirect(v.Field(j)).Interface())
							}

						} else {
							param += fmt.Sprintf("%v, ", GetSqlValue(v.Field(j).Interface()))
						}
					}

				}
			}
			param = strings.TrimSuffix(param, ", ")
			param += ")"
			cols = append(cols, param)
		}
		return []string{strings.Join(cols, ", ")}
	} else {
		for j := 0; j < structType.NumField(); j++ {
			fieldName := structType.Field(j).Name
			if inputValue.Field(j).CanInterface() && (includeID && fieldName == "Id") {
				_isTransient := skyutl.IsTransient(skyutl.GetFieldTagValueOfStruct(input, fieldName, "readonly"))
				if !_isTransient {
					if inputValue.Field(j).Kind() == reflect.Ptr {
						if inputValue.Field(j).IsNil() {
							cols = append(cols, "NULL")
						} else {
							cols = append(cols, fmt.Sprintf("%v", reflect.Indirect(inputValue.Field(j)).Interface()))
						}
					} else {
						cols = append(cols, fmt.Sprintf("%v", inputValue.Field(j).Interface()))
					}
				}

			}
		}
		return []string{"(" + strings.Join(cols, ", ") + ")"}
	}
}

func makeInsertListFields(ctx context.Context, input interface{}, ignoreFields map[string]interface{}, includeID bool) ([]string, []interface{}) {
	userID, _ := skyutl.GetUserID(ctx)

	inputValue := reflect.ValueOf(input)

	if inputValue.Kind() == reflect.Ptr {
		inputValue = inputValue.Elem()
	}

	var sliceType reflect.Type
	if inputValue.Kind() == reflect.Slice {
		sliceType = inputValue.Type()
	}

	structType := inputValue.Type()
	if sliceType != nil {
		structType = sliceType.Elem()

		if structType.Kind() == reflect.Ptr {
			structType = structType.Elem()
		}
	}
	values := []interface{}{}
	cols := []string{}

	if inputValue.Kind() == reflect.Slice {
		for i := 0; i < inputValue.Len(); i++ {
			v := inputValue.Index(i)
			if v.Kind() == reflect.Ptr {
				v = v.Elem()
			}

			param := "(%L, "
			values = append(values, []interface{}{userID}...)
			for j := 0; j < structType.NumField(); j++ {
				fieldName := structType.Field(j).Name
				snakeCaseFieldName := strcase.SnakeCase(fieldName)
				if v.Field(j).CanInterface() && ((includeID && fieldName == "Id") || skyutl.FirstIndexOf(SystemField, fieldName) < 0) && (ignoreFields[snakeCaseFieldName] == nil || !ignoreFields[snakeCaseFieldName].(bool)) {

					_isTransient := skyutl.IsTransient(skyutl.GetFieldTagValueOfStruct(input, fieldName, "readonly"))

					if !_isTransient {
						param += "%L, "
						if v.Field(j).Kind() == reflect.Ptr {
							if v.Field(j).IsNil() {
								values = append(values, nil)
							} else {
								values = append(values, reflect.Indirect(v.Field(j)).Interface())
							}

						} else {
							values = append(values, v.Field(j).Interface())
						}
					}

				}
			}
			param = strings.TrimSuffix(param, ", ")
			param += ")"
			cols = append(cols, param)
		}
		return []string{strings.Join(cols, ", ")}, values
	} else {
		for j := 0; j < structType.NumField(); j++ {
			fieldName := structType.Field(j).Name
			snakeCaseFieldName := strcase.SnakeCase(fieldName)
			if inputValue.Field(j).CanInterface() && ((includeID && fieldName == "Id") || skyutl.FirstIndexOf(SystemField, fieldName) < 0) && (ignoreFields[snakeCaseFieldName] == nil || !ignoreFields[snakeCaseFieldName].(bool)) {
				_isTransient := skyutl.IsTransient(skyutl.GetFieldTagValueOfStruct(input, fieldName, "readonly"))
				if !_isTransient {
					cols = append(cols, "%L")

					if inputValue.Field(j).Kind() == reflect.Ptr {
						if inputValue.Field(j).IsNil() {
							values = append(values, nil)
						} else {
							values = append(values, reflect.Indirect(inputValue.Field(j)).Interface())
						}

					} else {
						values = append(values, inputValue.Field(j).Interface())
					}
				}

			}
		}
		cols = append([]string{"%L"}, cols...)
		values = append([]interface{}{userID}, values...)
		return []string{"(" + strings.Join(cols, ", ") + ")"}, values
	}
}

func makeWhereCond(cond *orderedmap.OrderedMap, startParam int) (string, []interface{}) {
	values := []interface{}{}
	cols := []string{}
	count := startParam + 1

	for _, key := range cond.Keys() {
		value, _ := cond.Get(key)
		if value == nil && !strings.Contains(key.(string), " ") {
			cols = append(cols, fmt.Sprintf("%v IS NULL", key))
		} else if value == nil && strings.Contains(key.(string), " ") {
			cols = append(cols, fmt.Sprintf("%v", key))
		} else {
			if strings.Contains(key.(string), " ") && (strings.Contains(key.(string), " ANY") || strings.Contains(key.(string), " ALL")) {
				cols = append(cols, fmt.Sprintf("%v ($%v)", key, count))
			} else if strings.Contains(key.(string), " ") {
				cols = append(cols, fmt.Sprintf("%v $%v", key, count))
			} else {
				cols = append(cols, fmt.Sprintf("%v = $%v", key, count))
			}

			values = append(values, value)
			count++
		}
	}

	return strings.Join(cols, " AND "), values
}

func scanAndFill(input interface{}, rows *sql.Rows) error {
	columns, err := rows.Columns()
	if err != nil {
		skylog.Error(err)
		return err
	}

	count := len(columns)

	var cachedInput interface{}
	if len := lenArrayOfStruct(input); len > -1 {
		// TODO
		// sliceOfInput := reflect.SliceOf(skyutl.GetStructType(input))

		// cachedInput = reflect.New(reflect.Indirect(reflect.ValueOf(input)).Type()).Interface()
		// reflect.ValueOf(cachedInput).Elem().Set(reflect.MakeSlice(sliceOfInput, len, len))
	} else {
		cachedInput = reflect.New(skyutl.GetStructType(input)).Interface()
	}

	if err := skyutl.TransientStructConvert(input, cachedInput); cachedInput != nil && err != nil {
		return err
	}

	skyutl.ResetSliceOrStruct(input)

	for rows.Next() {
		values := make([]interface{}, count)
		valuePtrs := make([]interface{}, count)
		for i := range columns {
			valuePtrs[i] = &values[i]
		}
		rows.Scan(valuePtrs...)
		fields := []string{}
		// for i, col := range columns {
		// 	val := values[i]

		// 	b, ok := val.([]byte)
		// 	var v interface{}
		// 	if ok {
		// 		v = string(b)
		// 	} else {
		// 		v = val
		// 	}

		// 	fields = append(fields, col)
		// 	values[i] = v
		// }

		for _, col := range columns {
			// val := values[i]

			// switch val.(type) {
			// case []uint8:
			// 	b, ok := val.([]byte)
			// 	if ok {
			// 		v := string(b)
			// 		values[i] = v
			// 	} 
			// }
			fields = append(fields, col)
		}

		fillOut(fields, values, input)
	}

	if len := lenArrayOfStruct(input); len > -1 {
		// TODO
		// if err := skyutl.TransientStructConvert(cachedInput, input); err != nil {
		// 	return err
		// }
	} else {
		if err := skyutl.TransientStructConvert(cachedInput, input); err != nil {
			return err
		}
	}
	return nil
}

func scanAndFillByValue(inputValue reflect.Value, rows *sql.Rows) error {
	input := inputValue.Interface()
	columns, err := rows.Columns()
	if err != nil {
		skylog.Error(err)
		return err
	}

	count := len(columns)

	var cachedInput interface{}
	if len := lenArrayOfStruct(input); len > -1 {
		// TODO
	} else {
		cachedInput = reflect.New(skyutl.GetStructType(input)).Interface()
	}

	if err := skyutl.TransientStructConvert(input, cachedInput); cachedInput != nil && err != nil {
		return err
	}

	skyutl.ResetSliceOrStruct(input)

	for rows.Next() {
		values := make([]interface{}, count)
		valuePtrs := make([]interface{}, count)
		for i := range columns {
			valuePtrs[i] = &values[i]
		}
		rows.Scan(valuePtrs...)
		fields := []string{}
		for _, col := range columns {
			// val := values[i]

			// b, ok := val.([]byte)
			// var v interface{}
			// if ok {
			// 	v = string(b)
			// } else {
			// 	v = val
			// }

			fields = append(fields, col)
			// values[i] = v
		}

		fillOutByValue(fields, values, inputValue)
	}

	if len := lenArrayOfStruct(input); len > -1 {
		// TODO
	} else {
		if err := skyutl.TransientStructConvert(cachedInput, input); err != nil {
			return err
		}
	}
	return nil
}

func makeConditionValue(start int, modelValue reflect.Value, condFields ...string) ([]interface{}, string) {
	cond := []string{}
	values := []interface{}{}
	count := start
	for _, key := range condFields {
		field := modelValue.FieldByName(strcase.UpperCamelCase(key))
		if field.IsValid() && field.CanInterface() {
			cond = append(cond, fmt.Sprintf("%v = $%v", key, count))
			values = append(values, field.Interface())
			count++
		}
	}
	return values, strings.Join(cond, " AND ")
}

func makeBatchConditionValue(modelValue reflect.Value, condFields ...string) ([]interface{}, string) {
	cond := []string{}
	values := []interface{}{}
	for _, key := range condFields {
		field := modelValue.FieldByName(strcase.UpperCamelCase(key))
		if field.IsValid() && field.CanInterface() {
			cond = append(cond, fmt.Sprintf("%v", key)+"=%L")
			values = append(values, field.Interface())
		}
	}
	return values, strings.Join(cond, " AND ")
}

func makeConditionMap(start int, condMap *orderedmap.OrderedMap) ([]interface{}, string) {
	cond := []string{}
	values := []interface{}{}
	count := start

	for _, key := range condMap.Keys() {
		value, _ := condMap.Get(key)
		if value == nil && !strings.Contains(key.(string), " ") {
			cond = append(cond, fmt.Sprintf("%v IS NULL", key))
		} else if value == nil && strings.Contains(key.(string), " ") {
			cond = append(cond, fmt.Sprintf("%v", key))
		} else {
			if strings.Contains(key.(string), " ") && (strings.Contains(key.(string), " ANY") || strings.Contains(key.(string), " ALL")) {
				cond = append(cond, fmt.Sprintf("%v ($%v)", key, count))
			} else if strings.Contains(key.(string), " ") {
				cond = append(cond, fmt.Sprintf("%v $%v", key, count))
			} else {
				cond = append(cond, fmt.Sprintf("%v = $%v", key, count))
			}

			values = append(values, value)
			count++
		}
	}
	return values, strings.Join(cond, " AND ")
}

func makeExtConditionMap(start int, condMap *orderedmap.OrderedMap) ([]interface{}, string) {
	cond := []string{}
	values := []interface{}{}
	count := start

	for _, key := range condMap.Keys() {
		value, _ := condMap.Get(key)
		if value == nil && !strings.Contains(key.(string), " ") {
			cond = append(cond, fmt.Sprintf("%v IS NULL", key))
		} else if value == nil && strings.Contains(key.(string), " ") {
			cond = append(cond, fmt.Sprintf("%v", key))
		} else {
			if key != nil && (strings.Contains(key.(string), " ANY") || strings.Contains(key.(string), " ALL")) {
				cond = append(cond, fmt.Sprintf("%v ($%v)", key, count))
			} else {
				cond = append(cond, fmt.Sprintf("%v $%v", key, count))
			}

			values = append(values, value)
			count++
		}
	}
	return values, strings.Join(cond, " AND ")
}

func makeConditionManyValue(modelValue reflect.Value, condFields ...string) ([]interface{}, string) {
	cond := []string{}
	values := []interface{}{}
	count := 1
	for i := 0; i < modelValue.Len(); i++ {
		subCond := []string{}
		for _, key := range condFields {
			field := modelValue.Index(i).FieldByName(strcase.UpperCamelCase(key))
			if field.IsValid() && field.CanInterface() {
				subCond = append(subCond, fmt.Sprintf("%v = $%v", key, count))
				values = append(values, field.Interface())
				count++
			}
		}
		cond = append(cond, strings.Join(subCond, " AND "))
	}

	return values, strings.Join(cond, " OR ")
}

func makeUpdateExtFields(fieldMap *orderedmap.OrderedMap) ([]interface{}, string) {
	cond := []string{}
	values := []interface{}{}
	count := 2 //$1: updated_by, $2: updated_at

	for _, key := range fieldMap.Keys() {
		value, _ := fieldMap.Get(key)
		if value == nil && !strings.Contains(key.(string), " ") {
			cond = append(cond, fmt.Sprintf("%v IS NULL", key))
		} else if value == nil && strings.Contains(key.(string), " ") {
			cond = append(cond, fmt.Sprintf("%v", key))
		} else {
			cond = append(cond, fmt.Sprintf(" %v = $%v", key, count))
			values = append(values, value)
			count++
		}
	}
	return values, strings.Join(cond, ", ")
}

func makeUpdateFields(input interface{}, ignoreFields map[string]interface{}) (string, []interface{}) {
	count := 2 // $1: UpdatedBy
	return makeUpdateFieldsWithParam(input, ignoreFields, count)
}

func makeUpdateFieldsWithParam(input interface{}, ignoreFields map[string]interface{}, startParam int) (string, []interface{}) {
	inputValue := reflect.ValueOf(input)

	if inputValue.Kind() == reflect.Ptr {
		inputValue = inputValue.Elem()
	}
	structType := inputValue.Type()

	values := []interface{}{}
	cols := []string{}
	count := startParam

	for j := 0; j < structType.NumField(); j++ {
		fieldName := structType.Field(j).Name
		snakeCaseFieldName := strcase.SnakeCase(fieldName)
		if inputValue.Field(j).CanInterface() && skyutl.FirstIndexOf(SystemField, fieldName) < 0 && (ignoreFields[snakeCaseFieldName] == nil || !ignoreFields[snakeCaseFieldName].(bool)) {
			_isTransient := skyutl.IsTransient(skyutl.GetFieldTagValueOfStruct(input, fieldName, "readonly"))
			if !_isTransient {
				cols = append(cols, fmt.Sprintf("%v = $%v", snakeCaseFieldName, count))
				count++

				if inputValue.Field(j).Kind() == reflect.Ptr {
					if inputValue.Field(j).IsNil() {
						values = append(values, nil)
					} else {
						values = append(values, reflect.Indirect(inputValue.Field(j)).Interface())
					}

				} else {
					values = append(values, inputValue.Field(j).Interface())
				}
			}

		}
	}
	return strings.Join(cols, ", "), values
}

func makeBatchUpdateFieldsWithParam(input interface{}, ignoreFields map[string]interface{}) (string, []interface{}) {
	inputValue := reflect.ValueOf(input)

	if inputValue.Kind() == reflect.Ptr {
		inputValue = inputValue.Elem()
	}
	structType := inputValue.Type()

	values := []interface{}{}
	cols := []string{}

	for j := 0; j < structType.NumField(); j++ {
		fieldName := structType.Field(j).Name
		snakeCaseFieldName := strcase.SnakeCase(fieldName)
		if inputValue.Field(j).CanInterface() && skyutl.FirstIndexOf(SystemField, fieldName) < 0 && (ignoreFields[snakeCaseFieldName] == nil || !ignoreFields[snakeCaseFieldName].(bool)) {
			_isTransient := skyutl.IsTransient(skyutl.GetFieldTagValueOfStruct(input, fieldName, "readonly"))
			if !_isTransient {
				cols = append(cols, fmt.Sprintf("%v", snakeCaseFieldName)+"=%L")

				if inputValue.Field(j).Kind() == reflect.Ptr {
					if inputValue.Field(j).IsNil() {
						values = append(values, nil)
					} else {
						values = append(values, reflect.Indirect(inputValue.Field(j)).Interface())
					}

				} else {
					values = append(values, inputValue.Field(j).Interface())
				}
			}

		}
	}
	return strings.Join(cols, ", "), values
}

func getFieldListOfStruct(input interface{}, ignoreFields map[string]interface{}, includeID bool) ([]string, error) {
	inputValue := reflect.ValueOf(input)

	if inputValue.Kind() == reflect.Ptr {
		inputValue = inputValue.Elem()
	}

	var sliceType reflect.Type
	if inputValue.Kind() == reflect.Slice {
		sliceType = inputValue.Type()
		inputValue = inputValue.Index(0)
	}

	structType := inputValue.Type()
	if sliceType != nil {
		structType = sliceType.Elem()
	}

	if structType.Kind() == reflect.Ptr {
		structType = structType.Elem()
		inputValue = inputValue.Elem()
	}

	cols := []string{}
	for i := 0; i < structType.NumField(); i++ {
		fieldName := structType.Field(i).Name
		if inputValue.Field(i).CanInterface() && ((includeID && fieldName == "Id" && ((inputValue.Field(i).Kind() != reflect.Ptr && (inputValue.Field(i).Interface()).(int64) > 0) || (inputValue.Field(i).Kind() == reflect.Ptr && (reflect.Indirect(inputValue.Field(i)).Interface()).(int64)> 0)) ) || skyutl.FirstIndexOf(SystemField, fieldName) < 0) {
			_isTransient := skyutl.IsTransient(skyutl.GetFieldTagValueOfStruct(input, fieldName, "readonly"))
			if !_isTransient {
				snakeCaseFieldName := strcase.SnakeCase(fieldName)
				if includeID {
					if ignoreFields[snakeCaseFieldName] == nil || !ignoreFields[snakeCaseFieldName].(bool) {
						cols = append(cols, snakeCaseFieldName)
					}
				} else {
					if fieldName != "Id" && (ignoreFields[snakeCaseFieldName] == nil || !ignoreFields[snakeCaseFieldName].(bool)) {
						cols = append(cols, snakeCaseFieldName)
					}
				}
			}

		}

	}
	return cols, nil
}

func getFieldListOfStructByValue(inputValue reflect.Value, ignoreFields map[string]interface{}, includeID bool) ([]string, error) {
	input := inputValue.Interface()
	if inputValue.Kind() == reflect.Ptr {
		inputValue = inputValue.Elem()
	}

	var sliceType reflect.Type

	if inputValue.Kind() == reflect.Slice && inputValue.Len() > 0 {
		sliceType = inputValue.Type()
		inputValue = inputValue.Index(0)
	}

	structType := inputValue.Type()
	if sliceType != nil {
		structType = sliceType.Elem()
	}

	if structType.Kind() == reflect.Ptr {
		structType = structType.Elem()
		inputValue = inputValue.Elem()
	}

	cols := []string{}
	for i := 0; i < structType.NumField(); i++ {
		fieldName := structType.Field(i).Name
		if inputValue.Field(i).CanInterface() && ((includeID && fieldName == "Id") || skyutl.FirstIndexOf(SystemField, fieldName) < 0) {
			_isTransient := skyutl.IsTransient(skyutl.GetFieldTagValueOfStruct(input, fieldName, "readonly"))
			if !_isTransient {
				snakeCaseFieldName := strcase.SnakeCase(fieldName)
				if includeID {
					if ignoreFields[snakeCaseFieldName] == nil || !ignoreFields[snakeCaseFieldName].(bool) {
						cols = append(cols, snakeCaseFieldName)
					}
				} else {
					if fieldName != "Id" && (ignoreFields[snakeCaseFieldName] == nil || !ignoreFields[snakeCaseFieldName].(bool)) {
						cols = append(cols, snakeCaseFieldName)
					}
				}
			}
		}
	}
	return cols, nil
}

func fillOut(fields []string, values []interface{}, out interface{}) {
	outValues := reflect.ValueOf(out)
	if reflect.TypeOf(out).Kind() == reflect.Ptr && outValues.Kind() == reflect.Ptr {
		outValues = outValues.Elem()
	}

	if reflect.TypeOf(out).Kind() == reflect.Ptr && outValues.Kind() == reflect.Struct { //struct
		fillStruct(fields, values, outValues.Addr())
	} else if reflect.TypeOf(out).Kind() == reflect.Ptr && outValues.Kind() == reflect.Slice { //array
		sliceType := outValues.Type()
		structType := sliceType.Elem()

		if structType.Kind() == reflect.Ptr {
			structType = structType.Elem()
			newStruct := reflect.New(structType)
			fillStruct(fields, values, newStruct)
			outValues.Set(reflect.Append(outValues, newStruct))
		} else {
			newStruct := reflect.New(structType)
			fillStruct(fields, values, newStruct)
			outValues.Set(reflect.Append(outValues, reflect.Indirect(newStruct)))
		}
	} else {
		for i := 0; i < outValues.Len(); i++ {
			ele := outValues.Index(i).Elem().Elem()
			if ele.Kind() == reflect.Slice {
				sliceType := ele.Type()
				structType := sliceType.Elem()
				if structType.Kind() == reflect.Ptr {
					structType = structType.Elem()
					newStruct := reflect.New(structType)
					fillStruct(fields, values, newStruct)
					ele.Set(reflect.Append(ele, newStruct))
				} else if structType.Kind() == reflect.Struct {
					newStruct := reflect.New(structType)
					fillStruct(fields, values, newStruct)
					ele.Set(reflect.Append(ele, reflect.Indirect(newStruct)))
				} else {
					ele.Set(reflect.Append(ele, reflect.ValueOf(values[0])))
				}
			} else if ele.Kind() == reflect.Struct {
				fillStruct(fields, values, ele.Addr())
			} else {
				if i < len(values) {
					skyutl.SetReflectValue("NoStructName", "NoFieldName", outValues.Index(i).Elem().Elem(), values[i])
				}

			}
		}
	}

}

func fillOutByValue(fields []string, values []interface{}, outValues reflect.Value) {
	out := outValues.Interface()
	if reflect.TypeOf(out).Kind() == reflect.Ptr && outValues.Kind() == reflect.Ptr {
		outValues = outValues.Elem()
	}

	if outValues.Kind() == reflect.Struct { //struct
		fillStruct(fields, values, outValues.Addr())
	} else if outValues.Kind() == reflect.Slice { //array
		sliceType := outValues.Type()
		structType := sliceType.Elem()
		if structType.Kind() == reflect.Ptr {
			structType = structType.Elem()
			newStruct := reflect.New(structType)
			fillStruct(fields, values, newStruct)
			outValues.Set(reflect.Append(outValues, newStruct))
		} else {
			newStruct := reflect.New(structType)
			fillStruct(fields, values, newStruct)
			outValues.Set(reflect.Append(outValues, reflect.Indirect(newStruct)))
		}
	} else {
		for i := 0; i < outValues.Len(); i++ {
			ele := outValues.Index(i).Elem()
			if ele.Kind() == reflect.Slice {
				sliceType := ele.Type()
				structType := sliceType.Elem()
				if structType.Kind() == reflect.Ptr {
					structType = structType.Elem()
					newStruct := reflect.New(structType)
					fillStruct(fields, values, newStruct)
					ele.Set(reflect.Append(ele, newStruct))
				} else if structType.Kind() == reflect.Struct {
					newStruct := reflect.New(structType)
					fillStruct(fields, values, newStruct)
					ele.Set(reflect.Append(ele, reflect.Indirect(newStruct)))
				} else {
					ele.Set(reflect.Append(ele, reflect.ValueOf(values[0])))
				}
			} else if ele.Kind() == reflect.Struct {
				fillStruct(fields, values, ele.Addr())
			} else {
				if i < len(values) {
					skyutl.SetReflectValue("NoStructName", "NoFieldName", outValues.Index(i).Elem().Elem(), values[i])
				}
			}
		}
	}

}

func fillStruct(fields []string, values []interface{}, outStruct reflect.Value) {
	structName := skyutl.GetStructNameFromValue(outStruct)
	for index, field := range fields {
		fieldName := strcase.UpperCamelCase(field)
		fieldEle := outStruct.Elem().FieldByName(fieldName)
		if fieldEle.IsValid() {
			skyutl.SetReflectValue(structName, fieldName, fieldEle, values[index])
			// if fieldEle.Type().Kind() == reflect.Slice {
			// 	// TODO
			// 	// skyutl.SetReflectValue(structName, fieldName, fieldEle, values[index])
			// } else {
			// 	skyutl.SetReflectValue(structName, fieldName, fieldEle, values[index])
			// }
		}

	}
}

// func makeAndCondition(param *orderedmap.OrderedMap) string {
// 	cond := []string{}
// 	count := 1
// 	for _, key := range param.Keys() {
// 		cond = append(cond, fmt.Sprintf("%v = $%v", key, count))
// 		count++
// 	}
// 	return strings.Join(cond, " AND ")
// }

func checkOutParam(out interface{}) (outParamType, error) {
	outValues := reflect.ValueOf(out)
	isArray, isStruct, isBasic := false, false, false

	for i := 0; i < outValues.Len(); i++ {
		if outValues.Index(i).Elem().Kind() != reflect.Ptr {
			return eRROR, fmt.Errorf("parameter at index [%v] must be a pointer", i+2)
		}
		ele := outValues.Index(i).Elem().Elem()
		if ele.Kind() == reflect.Slice {
			isArray = true
		} else if ele.Kind() == reflect.Struct {
			isStruct = true
		} else {
			isBasic = true
		}
	}

	if (isArray && isStruct) || (isArray && isBasic) || (isStruct && isBasic) {
		return eRROR, errors.New("too many type of out param is not support")
	}

	if isArray {
		return aRRAY, nil
	} else if isStruct {
		return sTRUCT, nil
	}
	return bASIC, nil
}

func (q *Q) callFunc(dataSource interface{}, funcName, alias string, params []interface{}, out ...interface{}) error {
	if !DisableInfoLog {
		skylog.Infof("=============================BENGIN FUNC=============================")
	}
	start := time.Now().UnixNano() / (int64(time.Millisecond) / int64(time.Nanosecond))
	sql := "SELECT * FROM " + funcName + "("

	positions := []string{}
	for index := range params {
		positions = append(positions, fmt.Sprintf("$%v", index+1))
	}

	sql += strings.Join(positions, ", ") + ")"

	if alias != "" {
		sql += " AS " + alias
	}

	res := q.query(dataSource, sql, params, out...)
	if !DisableInfoLog {
		end := time.Now().UnixNano() / (int64(time.Millisecond) / int64(time.Nanosecond))
		skylog.Infof("Function: " + funcName)
		skylog.Infof("Took: " + fmt.Sprintf("%v millisecond(s)", (end-start)))
		skylog.Infof("=============================END FUNC=============================")
	}
	return res
}

func (q *Q) callUnSecurityFunc(dataSource interface{}, funcName, alias string, params []interface{}, out ...interface{}) error {
	if !DisableInfoLog {
		skylog.Infof("=============================BENGIN FUNC=============================")
	}
	start := time.Now().UnixNano() / (int64(time.Millisecond) / int64(time.Nanosecond))
	sql := "SELECT * FROM " + funcName + "("

	positions := []string{}
	for index := range params {
		positions = append(positions, fmt.Sprintf("$%v", index+1))
	}

	sql += strings.Join(positions, ", ") + ")"

	if alias != "" {
		sql += " AS " + alias
	}

	res := q.unSecurityQuery(dataSource, sql, params, out...)
	if !DisableInfoLog {
		end := time.Now().UnixNano() / (int64(time.Millisecond) / int64(time.Nanosecond))
		skylog.Infof("Function: " + funcName)
		skylog.Infof("Took: " + fmt.Sprintf("%v millisecond(s)", (end-start)))
		skylog.Infof("=============================END FUNC=============================")
	}
	return res
}

func (q *Q) readWithWhere(model interface{}, orderBy, whereCond string, params []interface{}) error {
	modelValue := reflect.ValueOf(model)
	if modelValue.Kind() != reflect.Ptr {
		return errors.New("require a pointer to a struct or an array of a struct")
	} else {
		modelValue = modelValue.Elem()
	}
	tableName := GetTableName(model)

	sqlStr := "SELECT * FROM " + tableName

	if strings.Trim(whereCond, " ") != "" {
		sqlStr += " WHERE " + whereCond
	}

	if strings.Trim(orderBy, "") != "" {
		sqlStr += " ORDER BY " + orderBy
	}

	skyutl.ResetSliceOrStruct(model)
	return q.Query(sqlStr, params, model)
}

func escapeParam(params []interface{}) []interface{} {
	results := []interface{}{}
	for _, value := range params {
		switch v := value.(type) {
		case string:
			results = append(results, strings.ReplaceAll(v, "'", "''"))
		default:
			results = append(results, value)
		}
	}

	return results
}

func (q *Q) batchInsert(dataSource interface{}, ctx context.Context, input interface{}, ignoreFields ...string) error {
	if _, err := checkIsAPointerToArrayOrStruct(input); err != nil {
		return err
	}

	tableName := GetTableName(input)
	ignoreFieldsMap := skyutl.ToMap(true, ignoreFields...)

	cols, _ := getFieldListOfStruct(input, ignoreFieldsMap, true)

	cols = append([]string{"created_by"}, cols...)

	_sql := "INSERT INTO " + tableName + "(" + strings.Join(cols, ", ") + ") values "

	params, data := makeInsertListFields(ctx, input, ignoreFieldsMap, true)
	_sql += strings.Join(params, ", ")
	data = escapeParam(data)
	logSQLAndParam(_sql, data)

	values := pq.Array(data)

	var ok string
	if err := q.callFunc(dataSource, "exec_func", "", []interface{}{_sql, values}, &ok); err != nil {
		return err
	}

	if ok != "ok" {
		return errors.New("Unknown error")
	}

	return nil
}

func (q *Q) batchUpdate(dataSource interface{}, ctx context.Context, model interface{}, condFields []string, ignoreFields ...string) error {
	typeOf := reflect.TypeOf(model)
	if typeOf.Kind() == reflect.Ptr {
		typeOf = typeOf.Elem()
	}

	sqlArray := []string{}
	valuesArray := []interface{}{}
	if typeOf.Kind() == reflect.Slice {
		valueOf := reflect.ValueOf(model)
		if valueOf.Kind() == reflect.Ptr {
			valueOf = valueOf.Elem()
		}

		for i := 0; i < valueOf.Len(); i++ {
			if valueOf.Index(i).Kind() == reflect.Ptr {
				_sql, values := buildUpdateSQL(ctx, valueOf.Index(i).Interface(), condFields, ignoreFields...)
				sqlArray = append(sqlArray, _sql)
				valuesArray = append(valuesArray, values...)
			} else {
				_sql, values := buildUpdateSQL(ctx, valueOf.Index(i).Addr().Interface(), condFields, ignoreFields...)
				sqlArray = append(sqlArray, _sql)
				valuesArray = append(valuesArray, values...)
			}
		}
	} else {
		_sql, values := buildUpdateSQL(ctx, model, condFields, ignoreFields...)
		sqlArray = append(sqlArray, _sql)
		valuesArray = append(valuesArray, values...)
	}

	_sql := strings.Join(sqlArray, ";")
	values := pq.Array(valuesArray)

	var ok string
	if err := q.callFunc(dataSource, "exec_func", "", []interface{}{_sql, values}, &ok); err != nil {
		return err
	}

	if ok != "ok" {
		return errors.New("Unknown error")
	}

	return nil
}

func (q *Q) batchDelete(dataSource interface{}, ctx context.Context, model interface{}, condFields []string) error {
	typeOf := reflect.TypeOf(model)
	if typeOf.Kind() == reflect.Ptr {
		typeOf = typeOf.Elem()
	}

	sqlArray := []string{}
	valuesArray := []interface{}{}
	if typeOf.Kind() == reflect.Slice {
		valueOf := reflect.ValueOf(model)
		if valueOf.Kind() == reflect.Ptr {
			valueOf = valueOf.Elem()
		}

		for i := 0; i < valueOf.Len(); i++ {
			if valueOf.Index(i).Kind() == reflect.Ptr {
				_sql, values := buildDeleteSQL(ctx, valueOf.Index(i).Interface(), condFields)
				sqlArray = append(sqlArray, _sql)
				valuesArray = append(valuesArray, values...)
			} else {
				_sql, values := buildDeleteSQL(ctx, valueOf.Index(i).Addr().Interface(), condFields)
				sqlArray = append(sqlArray, _sql)
				valuesArray = append(valuesArray, values...)
			}
		}
	} else {
		_sql, values := buildDeleteSQL(ctx, model, condFields)
		sqlArray = append(sqlArray, _sql)
		valuesArray = append(valuesArray, values...)
	}

	_sql := strings.Join(sqlArray, ";")
	values := pq.Array(valuesArray)

	var ok string
	if err := q.callFunc(dataSource, "exec_func", "", []interface{}{_sql, values}, &ok); err != nil {
		return err
	}

	if ok != "ok" {
		return errors.New("Unknown error")
	}

	return nil
}

func (q *Q) batchUpsert(dataSource interface{}, ctx context.Context, model interface{}, condFields []string, ignoreFields ...string) error {
	typeOf := reflect.TypeOf(model)
	if typeOf.Kind() == reflect.Ptr {
		typeOf = typeOf.Elem()
	}

	sqlArray := []string{}
	valuesArray := []interface{}{}
	if typeOf.Kind() == reflect.Slice {
		valueOf := reflect.ValueOf(model)
		if valueOf.Kind() == reflect.Ptr {
			valueOf = valueOf.Elem()
		}
		for i := 0; i < valueOf.Len(); i++ {
			if valueOf.Index(i).Kind() == reflect.Ptr {
				v := valueOf.Index(i).Elem()
				field := v.FieldByName("Id")
				if field.IsValid() && field.CanInterface() && field.Interface().(int64) > 0 { // update or delete
					deletedByField := v.FieldByName("DeletedBy")
					deletedBy := int64(0)
					if deletedByField.IsValid() && deletedByField.CanInterface() {
						deletedBy = deletedByField.Interface().(int64)
					}

					if deletedBy > 0 { // delete
						_sql, values := buildDeleteSQL(ctx, v.Addr().Interface(), condFields)
						sqlArray = append(sqlArray, _sql)
						valuesArray = append(valuesArray, values...)
					} else { // update
						_sql, values := buildUpdateSQL(ctx, v.Addr().Interface(), condFields, ignoreFields...)
						sqlArray = append(sqlArray, _sql)
						valuesArray = append(valuesArray, values...)
					}
				} else { // insert
					_sql, values := buildInsertSQL(ctx, v.Addr().Interface(), condFields, ignoreFields...)
					sqlArray = append(sqlArray, _sql)
					valuesArray = append(valuesArray, values...)
				}
			} else {
				field := valueOf.Index(i).FieldByName("Id")
				if field.IsValid() && field.CanInterface() && field.Interface().(int64) > 0 { // update or delete
					deletedBy := valueOf.Index(i).FieldByName("DeletedBy").Interface().(int64)
					if deletedBy > 0 { // delete
						_sql, values := buildDeleteSQL(ctx, valueOf.Index(i).Addr().Interface(), condFields)
						sqlArray = append(sqlArray, _sql)
						valuesArray = append(valuesArray, values...)
					} else { // update
						_sql, values := buildUpdateSQL(ctx, valueOf.Index(i).Addr().Interface(), condFields, ignoreFields...)
						sqlArray = append(sqlArray, _sql)
						valuesArray = append(valuesArray, values...)
					}
				} else { // insert
					_sql, values := buildInsertSQL(ctx, valueOf.Index(i).Addr().Interface(), condFields, ignoreFields...)
					sqlArray = append(sqlArray, _sql)
					valuesArray = append(valuesArray, values...)
				}
			}
		}
	} else {
		_sql, values := buildUpdateSQL(ctx, model, condFields, ignoreFields...)
		sqlArray = append(sqlArray, _sql)
		valuesArray = append(valuesArray, values...)
	}

	_sql := strings.Join(sqlArray, ";")
	values := pq.Array(valuesArray)

	var ok string
	if err := q.callFunc(dataSource, "exec_func", "", []interface{}{_sql, values}, &ok); err != nil {
		return err
	}

	if ok != "ok" {
		return errors.New("Unknown error")
	}

	return nil
}

func buildUpdateSQL(ctx context.Context, model interface{}, condFields []string, ignoreFields ...string) (string, []interface{}) {
	modelValue := reflect.ValueOf(model)
	if err := isPointerToStruct(&modelValue); err != nil {
		return "", nil
	}

	tableName := GetTableName(model)
	ignoreFieldsMap := skyutl.ToMap(true, ignoreFields...)

	_sql := []string{"UPDATE " + tableName + " SET updated_by = %L, updated_at = date_generator(), "}
	updateFieldStr, updateValues := makeBatchUpdateFieldsWithParam(model, ignoreFieldsMap)

	_sql = append(_sql, updateFieldStr)
	values, cond := makeBatchConditionValue(modelValue, condFields...)

	if strings.Trim(cond, " ") != "" {
		_sql = append(_sql, " WHERE "+cond)
	}

	sqlStr := strings.Join(_sql, " ")

	paramValues := append(updateValues, values...)

	userID, _ := skyutl.GetUserID(ctx)

	paramValues = append([]interface{}{userID}, paramValues...)

	paramValues = escapeParam(paramValues)
	return sqlStr, paramValues
}

func buildDeleteSQL(ctx context.Context, model interface{}, condFields []string) (string, []interface{}) {
	modelValue := reflect.ValueOf(model)
	if err := isPointerToStruct(&modelValue); err != nil {
		return "", nil
	}

	tableName := GetTableName(model)

	_sql := []string{"UPDATE " + tableName + " SET deleted_by = %L, deleted_at = date_generator() "}

	values, cond := makeBatchConditionValue(modelValue, condFields...)

	if strings.Trim(cond, " ") != "" {
		_sql = append(_sql, " WHERE "+cond)
	}

	sqlStr := strings.Join(_sql, " ")

	userID, _ := skyutl.GetUserID(ctx)

	paramValues := append([]interface{}{userID}, values...)

	paramValues = escapeParam(paramValues)
	return sqlStr, paramValues
}

func buildInsertSQL(ctx context.Context, input interface{}, condFields []string, ignoreFields ...string) (string, []interface{}) {
	if _, err := checkIsAPointerToArrayOrStruct(input); err != nil {
		return "", nil
	}

	tableName := GetTableName(input)
	ignoreFieldsMap := skyutl.ToMap(true, ignoreFields...)

	cols, _ := getFieldListOfStruct(input, ignoreFieldsMap, false)

	cols = append([]string{"created_by"}, cols...)

	_sql := "INSERT INTO " + tableName + "(" + strings.Join(cols, ", ") + ") values "

	params, data := makeInsertListFields(ctx, input, ignoreFieldsMap, false)
	_sql += strings.Join(params, ", ")

	data = escapeParam(data)
	logSQLAndParam(_sql, data)

	return _sql, data
}

func GetSqlValue(value interface{}) string {
	if value == nil {
		return ""
	}

	switch value.(type) {

	case string, *string:
		return fmt.Sprintf("'%v'", value)
	default:
		return fmt.Sprintf("%v", value)
	}
}

func (q *Q) batchUpsertWithReturn(dataSource interface{}, ctx context.Context, model interface{}, condFields []string, ignoreFields ...string) error {
	valueOf := reflect.ValueOf(model)
	if valueOf.Kind() != reflect.Ptr {
		return errors.New("require a pointer to an array of a struct")
	}

	if valueOf.Kind() == reflect.Ptr {
		valueOf = valueOf.Elem()
	}

	if valueOf.Kind() != reflect.Slice {
		return errors.New("require a pointer to an array of a struct")
	}

	typeOf := reflect.TypeOf(model)
	if typeOf.Kind() == reflect.Ptr {
		typeOf = typeOf.Elem()
	}

	dataType := typeOf

	if typeOf.Kind() == reflect.Slice {
		valueOf := reflect.ValueOf(model)
		if valueOf.Kind() == reflect.Ptr {
			valueOf = valueOf.Elem()
		}

		insertModel := reflect.New(dataType)
		insertCountPtrIndexes := []int{}

		updateModel := reflect.New(dataType)
		updateCountPtrIndexes := []int{}

		deleteModel := reflect.New(dataType)
		deleteCountPtrIndexes := []int{}

		for i := 0; i < valueOf.Len(); i++ {
			v := valueOf.Index(i)
			if v.Kind() == reflect.Ptr {
				v = v.Elem()
			}

			field := v.FieldByName("Id")
			if field.IsValid() && field.CanInterface() && field.Interface().(int64) > 0 { // update or delete
				deletedByField := v.FieldByName("DeletedBy")
				deletedBy := int64(0)
				if deletedByField.IsValid() && deletedByField.CanInterface() {
					deletedBy = deletedByField.Interface().(int64)
				}

				if deletedBy > 0 { // delete
					deleteModel.Elem().Set(reflect.Append(deleteModel.Elem(), valueOf.Index(i)))
					deleteCountPtrIndexes = append(deleteCountPtrIndexes, i)
				} else { // update
					updateModel.Elem().Set(reflect.Append(updateModel.Elem(), valueOf.Index(i)))
					updateCountPtrIndexes = append(updateCountPtrIndexes, i)
				}
			} else { // insert
				insertModel.Elem().Set(reflect.Append(insertModel.Elem(), valueOf.Index(i)))
				insertCountPtrIndexes = append(insertCountPtrIndexes, i)
			}
		}

		if err := q.insertByValue(dataSource, ctx, insertModel, ignoreFields...); err != nil {
			return err
		}

		if err := q.updateManyWithReturnByValue(dataSource, ctx, updateModel, condFields, ignoreFields...); err != nil {
			return err
		}

		if err := q.deleteManyWithReturnByValue(dataSource, ctx, deleteModel, condFields); err != nil {
			return err
		}
		mergeArray(model, insertModel.Interface(), updateModel.Interface(), deleteModel.Interface(), insertCountPtrIndexes, updateCountPtrIndexes, deleteCountPtrIndexes)
	} else {

	}

	return nil
}

func mergeArray(dest, insert, update, delete interface{}, insertIndexes, updateIndexes, deleteIndexes []int) {
	destValue := reflect.ValueOf(dest)
	if destValue.Kind() == reflect.Ptr {
		destValue = destValue.Elem()
	}

	insertValue := reflect.ValueOf(insert)
	if insertValue.Kind() == reflect.Ptr {
		insertValue = insertValue.Elem()
	}
	for i := range insertIndexes {
		destValue.Index(insertIndexes[i]).Set(insertValue.Index(i))
	}

	updateValue := reflect.ValueOf(update)
	if updateValue.Kind() == reflect.Ptr {
		updateValue = updateValue.Elem()
	}
	for i := range updateIndexes {
		if updateValue.Len() > i {
			destValue.Index(updateIndexes[i]).Set(updateValue.Index(i))
		}

	}

	deleteValue := reflect.ValueOf(delete)
	if deleteValue.Kind() == reflect.Ptr {
		deleteValue = deleteValue.Elem()
	}
	for i := range deleteIndexes {
		if deleteValue.Len() > i {
			destValue.Index(deleteIndexes[i]).Set(deleteValue.Index(i))
		}

	}
}