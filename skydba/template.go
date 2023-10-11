package skydba

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"

	"github.com/stoewer/go-strcase"
	"github.com/nkocsea/skylib_skylog/skylog"
	"github.com/nkocsea/skylib_skyutl/report"
	"github.com/nkocsea/skylib_skyutl/skyutl"
)

//MakeUpdate function
func MakeUpdate(ctx context.Context, model interface{}, disabled bool) {
	userID, _ := skyutl.GetUserID(ctx)

	MakeUpdateWithID(userID, model, disabled)
}

//MakeUpdateWithID function
func MakeUpdateWithID(userID int64, model interface{}, disabled bool) {
	now, _ := GetCurrentMillis()
	md := reflect.ValueOf(model).Elem()
	skyutl.SetReflectField(md, md.FieldByName("Disabled"), "Disabled", disabled)
	skyutl.SetReflectField(md, md.FieldByName("UpdatedBy"), "UpdatedBy", userID)
	skyutl.SetReflectField(md, md.FieldByName("UpdatedAt"), "UpdatedAt", now)

}

//MakeInsert function
func MakeInsert(ctx context.Context, model interface{}, disabled bool) {
	userID, _ := skyutl.GetUserID(ctx)
	MakeInsertWithID(userID, model, disabled)
}

//MakeValueInsert function
func MakeValueInsert(ctx context.Context, modelValue reflect.Value, disabled bool) {
	userID, _ := skyutl.GetUserID(ctx)
	MakeValueInsertWithID(userID, modelValue, disabled)

}

//MakeInsertWithID function
func MakeInsertWithID(userID int64, model interface{}, disabled bool) {
	now, _ := GetCurrentMillis()
	md := reflect.ValueOf(model).Elem()
	skyutl.SetReflectField(md, md.FieldByName("CreatedBy"), "CreatedBy", userID)
	skyutl.SetReflectField(md, md.FieldByName("CreatedAt"), "CreatedAt", now)
	skyutl.SetReflectField(md, md.FieldByName("DeletedAt"), "DeletedAt", now)
	skyutl.SetReflectField(md, md.FieldByName("Disabled"), "Disabled", disabled)
	skyutl.SetReflectField(md, md.FieldByName("Version"), "Version", int32(1))
}

//MakeValueInsertWithID function
func MakeValueInsertWithID(userID int64, modelValue reflect.Value, disabled bool) {
	now, _ := GetCurrentMillis()

	skyutl.SetReflectField(modelValue, modelValue.FieldByName("CreatedBy"), "CreatedBy", userID)
	skyutl.SetReflectField(modelValue, modelValue.FieldByName("CreatedAt"), "CreatedAt", now)
	skyutl.SetReflectField(modelValue, modelValue.FieldByName("Disabled"), "Disabled", disabled)
	skyutl.SetReflectField(modelValue, modelValue.FieldByName("Version"), "Version", int32(1))
}

//MakeDelete function
func MakeDelete(ctx context.Context, model interface{}) {
	userID, _ := skyutl.GetUserID(ctx)
	MakeDeleteWithID(userID, model)
}

//MakeDeleteWithID function
func MakeDeleteWithID(userID int64, model interface{}) {
	now, _ := GetCurrentMillis()
	md := reflect.ValueOf(model).Elem()
	skyutl.SetReflectField(md, md.FieldByName("DeletedBy"), "DeletedBy", userID)
	skyutl.SetReflectField(md, md.FieldByName("DeletedAt"), "DeletedAt", now)
}

//IsTextValueDuplicated function
func IsTextValueDuplicated(tableName string, columnName string, value string, id int64) (int32, error) {
	q := DefaultQuery()
	var result int32

	if err := q.Query(`SELECT * FROM is_text_value_duplicated($1, $2, $3, $4) as "isDuplicated"`, []interface{}{tableName, columnName, value, id}, &result); err != nil {
		skylog.Error(err)
		return 0, err
	}
	return result, nil
}

//IsTextValueExisted function
func IsTextValueExisted(tableName string, columnName string, value string) (int32, error) {
	q := DefaultQuery()
	var result int32

	if err := q.Query(`SELECT * FROM is_text_value_existed($1, $2, $3) as "isExisted"`, []interface{}{tableName, columnName, value}, &result); err != nil {
		skylog.Error(err)
		return 0, err
	}
	return result, nil

}

//GetOneByID function
func GetOneByID(tableName string, id int64, outStruct interface{}) error {
	const sql = `SELECT * FROM get_one_by_id($1, $2) as json`
	q := DefaultQuery()
	var jsOut string
	if err := q.Query(sql, []interface{}{tableName, id}, &jsOut); err != nil {
		skylog.Error(err)
		return err
	}

	if strings.HasPrefix(jsOut, "[") && strings.HasSuffix(jsOut, "]") {
		jsOut = strings.TrimSuffix(strings.TrimPrefix(jsOut, "["), "]")
	}

	convertedJS := skyutl.ConvertKeys([]byte(jsOut), strcase.LowerCamelCase)
	if err := json.Unmarshal(convertedJS, outStruct); err != nil {
		skylog.Error(err)
		return err
	}

	return nil
}

//GetOrgByID function
func GetOrgByID(orgInfo *report.OrgInfo) error {
	q := DefaultQuery()
	if err := q.ReadWithID(orgInfo, ""); err != nil {
		skylog.Error(err)
		return err
	}
	return nil
}
