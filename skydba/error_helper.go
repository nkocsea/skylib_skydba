package skydba

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"suntech.com.vn/skylib/skylog.git/skylog"
)

const (
	ERR_MESSAGE_PARAMS = "messageParams"
	ERR_CODE           = "code"
	ERR_MESSAGE        = "message"
	ERR_FIELD          = "field"
)

type LocaleResource struct {
	Category  string
	TypeGroup string
	Key       string
	Value     string
}

func ReflectVal2Slice(val reflect.Value) []interface{} {
	if val.Kind() != reflect.Slice {
		return []interface{}{}
	}

	ret := make([]interface{}, val.Len())

	for i := 0; i < val.Len(); i++ {
		ret[i] = val.Index(i).Interface()
	}

	return ret
}

func ReflectVal2Map(val reflect.Value) map[string]interface{} {
	if val.Kind() != reflect.Map {
		return map[string]interface{}{}
	}

	ret := make(map[string]interface{}, val.Len())

	for _, key := range val.MapKeys() {
		ret[fmt.Sprintf("%v", key)] = val.MapIndex(key)
	}

	return ret
}

// GetEmailSenderInfo function return sender email info from database
func GetLocaleResource(companyId int64, resourceKey string) string {
	container := ""
	typeGroup := ""
	key := ""
	arr := strings.Split(resourceKey, ".")
	if len(arr) == 3 {
		container = strings.Trim(arr[0], " ")
		typeGroup = strings.Trim(arr[1], " ")
		key = strings.Trim(arr[2], " ")
	} else {
		container = "SYS"
		typeGroup = "MSG"
		key = strings.Trim(resourceKey, " ")
	}

	q := DefaultQuery()

	sql := `
		select t1.category, t1.type_group, t1.key, t1.value
		from locale_resource t1 
		where t1.disabled = 0 
		and t1.deleted_at <= 0::bigint 
		and (t1.company_id = $1 or t1.company_id =0::bigint)
		and t1.category = $2
		and t1.type_group = $3
		and t1.key = $4
		order by t1.company_id desc
	`

	var items []*LocaleResource
	if err := q.Query(sql, []interface{}{companyId, container, typeGroup, key}, &items); err != nil {
		return key
	}

	if len(items) <= 0 {
		skylog.Error(fmt.Sprintf("Resource key %v not found", resourceKey))
		return key
	}
	return items[0].Value
}

func GetFieldName(companyId int64, field string, suffix string) string {
	if suffix == "" {
		return GetLocaleResource(companyId, fmt.Sprintf("SYS.FIELD.%v", strings.ToUpper(field)))
	} else {
		return GetLocaleResource(companyId, fmt.Sprintf("SYS.FIELD.%v_%v", strings.ToUpper(suffix), strings.ToUpper(field)))
	}
}

func CreateError(companyId int64, code codes.Code, resourceKey string, payload map[string]any) error {
	msg := GetLocaleResource(companyId, resourceKey)
	if msgParams, ok := payload[ERR_MESSAGE_PARAMS]; ok {
		val := reflect.ValueOf(msgParams)
		if val.Kind() == reflect.Map {
			msg, _ = FormatTemplate("", msg, ReflectVal2Map(val))
		} else if val.Kind() == reflect.Slice {
			msg = fmt.Sprintf(msg, ReflectVal2Slice(val)...)
		} else {
			msg = fmt.Sprintf(msg, msgParams)
		}
	}

	data := map[string]interface{}{
		ERR_CODE:    resourceKey,
		ERR_MESSAGE: msg,
	}
	if field, ok := payload[ERR_FIELD]; ok {
		data[ERR_FIELD] = field
	}

	fullMsg, _ := json.Marshal(data)
	st := status.New(code, string(fullMsg))
	return st.Err()
}

func InvalidError(companyId int64, resourceKey string, payload map[string]any) error {
	return CreateError(companyId, codes.InvalidArgument, resourceKey, payload)
}

func RequiredError(companyId int64, field string, suffix string) error {
	payload := map[string]any{
		ERR_FIELD: field,
	}
	return InvalidError(companyId, "SYS.MSG.REQUIRED_VALUE", payload)
}

func NotFutureDateError(companyId int64, field string, suffix string) error {
	fieldName := GetFieldName(companyId, field, suffix)
	payload := map[string]any{
		ERR_MESSAGE_PARAMS: []string{fieldName},
		ERR_FIELD:          field,
	}
	return InvalidError(companyId, "SYS.MSG.NOT_FUTURE_DATE", payload)
}

func StartEndDateError(companyId int64, field1 string, field2 string, suffix string) error {
	field1Name := GetFieldName(companyId, field1, suffix)
	field2Name := GetFieldName(companyId, field2, suffix)
	payload := map[string]any{
		ERR_MESSAGE_PARAMS: []string{field2Name, field1Name},
		ERR_FIELD:          field2,
	}
	return InvalidError(companyId, "SYS.MSG.START_DATE_END_DATE_ERROR", payload)
}

func PhoneInvalidError(companyId int64, field string, suffix string) error {
	payload := map[string]any{
		ERR_FIELD: field,
	}
	return InvalidError(companyId, "SYS.MSG.PHONE_INVALID", payload)
}

func EmailInvalidError(companyId int64, field string, suffix string) error {
	fieldName := GetFieldName(companyId, field, suffix)
	payload := map[string]any{
		ERR_MESSAGE_PARAMS: []string{fieldName},
		ERR_FIELD:          field,
	}
	return InvalidError(companyId, "SYS.MSG.EMAIL_INVALID", payload)
}

func InternalError(companyId int64, resourceKey string, payload map[string]any) error {
	return CreateError(companyId, codes.Internal, resourceKey, payload)
}

func DataNotFound(companyId int64) error {
	return InternalError(companyId, "SYS.MSG.DATA_NOT_FOUND", map[string]any{})
}
