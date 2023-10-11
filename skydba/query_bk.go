package skydba

// import (
// 	"context"
// 	"database/sql"
// 	"errors"
// 	"reflect"
// 	"strings"

// 	"github.com/elliotchance/orderedmap"
// 	"github.com/jinzhu/copier"
// 	"github.com/nkocsea/skylib_skylog/skylog"
// 	"github.com/nkocsea/skylib_skyutl/skyutl"
// )

// //ReadWithID function
// //Select one record with specific ID and reassign to model
// //
// //model := Model{Id: <primary key value>}
// //if err := q.ReadWithID(&model); err != nil {
// //	fmt.Println(err)
// //}
// func (q *Q) ReadWithID(model interface{}) error {
// 	if !skyutl.IsPtrToStruct(model) {
// 		return errors.New("require a pointer to a struct")
// 	}
// 	tableName, _ := skyutl.GetStructNameInSnakeCase(model)
// 	_sql := "SELECT * FROM " + tableName + " WHERE id = $1"
// 	id := skyutl.GetFieldValueOfStruct(model, "Id")
// 	if !DisableInfoLog {
// 		skylog.Info(_sql, id)
// 	}

// 	return q.Query(_sql, []interface{}{id}, model)
// }

// //Read function
// //Select one record with specific key valye map and reassign to model
// //
// //var model Model
// //cond := orderedmap.NewOrderedMap()
// //cond.Set("field1", <value of field1>)
// //cond.Set("field2", <value of field2>)
// //if err := q.Read(&model, cond); err != nil {
// //	fmt.Println(err)
// //}
// func (q *Q) Read(model interface{}, whereMap *orderedmap.OrderedMap) error {
// 	if whereMap.Len() == 0 {
// 		return errors.New("require a not empty whereMap")
// 	}

// 	if !skyutl.IsPtrToStruct(model) {
// 		return errors.New("require a pointer to a struct")
// 	}
// 	tableName, _ := skyutl.GetStructNameInSnakeCase(model)

// 	whereCond := makeAndCondition(whereMap)

// 	_sql := "SELECT * FROM " + tableName + " WHERE " + whereCond

// 	paramValues := skyutl.GetOrderedMapValues(whereMap)
// 	return q.Query(_sql, paramValues, model)
// }

// //Insert function
// //Insert one or more record, return inserted record(s)
// //input param is a struct or an array of struct
// func (q *Q) Insert(input interface{}, ignoreFields ...string) (interface{}, error) {
// 	return q.insert(nil, q.DB, input, ignoreFields...)
// }

// //TxInsert function
// //Transactional insert one or more record, return inserted record(s)
// //input param is a struct or an array of struct
// func (q *Q) TxInsert(tx *sql.Tx, input interface{}, ignoreFields ...string) (interface{}, error) {
// 	return q.insert(nil, tx, input, ignoreFields...)
// }

// //ContextTxInsert function
// //Transactional insert one or more record which auto set created_by and created_at field, return inserted record(s)
// //input param is a struct or an array of struct
// func (q *Q) ContextTxInsert(ctx context.Context, tx *sql.Tx, input interface{}, ignoreFields ...string) (interface{}, error) {
// 	return q.insert(ctx, tx, input, ignoreFields...)
// }

// //InsContextInsertert function
// //Insert one or more record which auto set created_by and created_at field, return inserted record(s)
// //input param is a struct or an array of struct
// func (q *Q) ContextInsert(ctx context.Context, input interface{}, ignoreFields ...string) (interface{}, error) {
// 	return q.insert(ctx, q.DB, input, ignoreFields...)
// }

// func (q *Q) insert(ctx context.Context, dataSource interface{}, input interface{}, ignoreFields ...string) (interface{}, error) {
// 	tableName, _ := skyutl.GetStructNameInSnakeCase(input)
// 	ignoreFieldsMap := skyutl.ToMap(true, ignoreFields...)

// 	cols, _ := getFieldListOfStruct(input, ignoreFieldsMap)

// 	_sql := []string{"INSERT INTO " + tableName + "(" + strings.Join(cols, ", ") + ") values "}

// 	params, data := makeInsertFields(ctx, input, ignoreFieldsMap)
// 	_sql = append(_sql, strings.Join(params, ", "))

// 	_sql = append(_sql, "RETURNING *")

// 	sqlStr := strings.Join(_sql, " ")

// 	if !DisableInfoLog {
// 		skylog.Info(sqlStr)
// 		skylog.Info(data)
// 	}

// 	var rows *sql.Rows
// 	var err error
// 	if reflect.TypeOf(dataSource).String() == "*sql.Tx" {
// 		rows, err = dataSource.(*sql.Tx).Query(sqlStr, data...)

// 	} else {
// 		rows, err = dataSource.(*sql.DB).Query(sqlStr, data...)
// 	}

// 	if err != nil {
// 		skylog.Error(err)
// 		return nil, err
// 	}
// 	defer rows.Close()
// 	columns, err := rows.Columns()
// 	if err != nil {
// 		skylog.Error(err)
// 		return nil, err
// 	}

// 	count := len(columns)

// 	var out interface{}
// 	copier.CopyWithOption(&out, &input, copier.Option{DeepCopy: true})
// 	skyutl.ResetSliceOrStruct(out)

// 	for rows.Next() {
// 		values := make([]interface{}, count)
// 		valuePtrs := make([]interface{}, count)
// 		for i := range columns {
// 			valuePtrs[i] = &values[i]
// 		}
// 		rows.Scan(valuePtrs...)
// 		fields := []string{}
// 		for i, col := range columns {
// 			val := values[i]

// 			b, ok := val.([]byte)
// 			var v interface{}
// 			if ok {
// 				v = string(b)
// 			} else {
// 				v = val
// 			}

// 			fields = append(fields, col)
// 			values[i] = v
// 		}

// 		fillOut(fields, values, out)
// 	}

// 	return out, nil
// }

// func (q *Q) remove(dataSource interface{}, input interface{}, cond *orderedmap.OrderedMap) (int64, error) {
// 	tableName, _ := skyutl.GetStructNameInSnakeCase(input)

// 	sqlStr := "DELETE FROM " + tableName + " WHERE " + makeAndCondition(cond) + " RETURNING id"

// 	if !DisableInfoLog {
// 		skylog.Info(sqlStr)
// 	}

// 	var rows *sql.Rows
// 	var err error
// 	paramValues := skyutl.GetOrderedMapValues(cond)
// 	if reflect.TypeOf(dataSource).String() == "*sql.Tx" {
// 		rows, err = dataSource.(*sql.Tx).Query(sqlStr, paramValues...)

// 	} else {
// 		rows, err = dataSource.(*sql.DB).Query(sqlStr, paramValues...)
// 	}

// 	if err != nil {
// 		skylog.Error(err)
// 		return 0, err
// 	}

// 	defer rows.Close()

// 	var deleteID int64
// 	for rows.Next() {
// 		if err := rows.Scan(&deleteID); err != nil {
// 			return 0, err
// 		}
// 	}

// 	return deleteID, nil
// }

// //Remove function
// //Permanently delete one record with custom condition, return deleted id
// func (q *Q) Remove(input interface{}, cond *orderedmap.OrderedMap) (int64, error) {
// 	return q.remove(q.DB, input, cond)
// }

// //TxRemove function
// //Transactional permanently delete one record with custom condition, return deleted id
// func (q *Q) TxRemove(tx *sql.Tx, input interface{}, cond *orderedmap.OrderedMap) (int64, error) {
// 	return q.remove(tx, input, cond)
// }

// //RemoveWithID function
// //Permanently delete one record with specific ID, return deleted id
// func (q *Q) RemoveWithID(input interface{}) (int64, error) {
// 	id := skyutl.GetFieldValueOfStruct(input, "Id")
// 	cond := orderedmap.NewOrderedMap()
// 	cond.Set("id", id)
// 	return q.remove(q.DB, input, cond)
// }

// //TxRemoveWithID function
// //Transactional permanently delete one record with specific ID, return deleted id
// func (q *Q) TxRemoveWithID(tx *sql.Tx, input interface{}) (int64, error) {
// 	id := skyutl.GetFieldValueOfStruct(input, "Id")
// 	cond := orderedmap.NewOrderedMap()
// 	cond.Set("id", id)
// 	return q.remove(tx, input, cond)
// }

// //Update function
// func (q *Q) Update(input interface{}, cond *orderedmap.OrderedMap, ignoreFields ...string) (interface{}, error) {
// 	return q.update(q.DB, input, cond, ignoreFields...)
// }

// //UpdateWithID function
// func (q *Q) UpdateWithID(input interface{}, ignoreFields ...string) (interface{}, error) {
// 	if !skyutl.IsPtrToStruct(input) {
// 		err := errors.New("require a pointer to a model struct")
// 		return nil, err
// 	}

// 	id := skyutl.GetFieldValueOfStruct(input, "Id")

// 	cond := orderedmap.NewOrderedMap()
// 	cond.Set("id", id)
// 	return q.update(q.DB, input, cond, ignoreFields...)
// }

// //Upsert function
// func (q *Q) Upsert(input interface{}, cond *orderedmap.OrderedMap, ignoreFields ...string) (interface{}, error) {
// 	return q.upsert(q.DB, input, cond, ignoreFields...)
// }

// //ContextUpsert function
// func (q *Q) ContextUpsert(ctx context.Context, input interface{}, cond *orderedmap.OrderedMap, ignoreFields ...string) (interface{}, error) {
// 	return q.contextUpsert(ctx, q.DB, input, cond, ignoreFields...)
// }

// //TxUpsert function
// func (q *Q) TxUpsert(tx *sql.Tx, input interface{}, cond *orderedmap.OrderedMap, ignoreFields ...string) (interface{}, error) {
// 	return q.upsert(tx, input, cond, ignoreFields...)
// }

// //UpsertWithID function
// func (q *Q) UpsertWithID(input interface{}, ignoreFields ...string) (interface{}, error) {
// 	id := skyutl.GetFieldValueOfStruct(input, "Id")
// 	cond := orderedmap.NewOrderedMap()
// 	cond.Set("id", id)
// 	return q.Upsert(input, cond, ignoreFields...)
// }

// //ContextUpsertWithID function
// func (q *Q) ContextUpsertWithID(ctx context.Context, input interface{}, ignoreFields ...string) (interface{}, error) {
// 	id := skyutl.GetFieldValueOfStruct(input, "Id")
// 	cond := orderedmap.NewOrderedMap()
// 	cond.Set("id", id)
// 	return q.ContextUpsert(ctx, input, cond, ignoreFields...)
// }

// //TxUpsertWithID function
// func (q *Q) TxUpsertWithID(tx *sql.Tx, input interface{}, ignoreFields ...string) (interface{}, error) {
// 	id := skyutl.GetFieldValueOfStruct(input, "Id")
// 	cond := orderedmap.NewOrderedMap()
// 	cond.Set("id", id)
// 	return q.TxUpsert(tx, input, cond, ignoreFields...)
// }

// func (q *Q) upsert(dataSource interface{}, input interface{}, cond *orderedmap.OrderedMap, ignoreFields ...string) (interface{}, error) {
// 	if !skyutl.IsPtrToStruct(input) {
// 		err := errors.New("require a pointer to a model struct")
// 		return nil, err
// 	}

// 	updatedRows, err := q.execUpdate(dataSource, input, cond, ignoreFields...)
// 	if err != nil {
// 		skylog.Error(err)
// 		return nil, err
// 	}

// 	if updatedRows > 0 {
// 		return updatedRows, nil
// 	}
// 	return q.insert(nil, dataSource, input, ignoreFields...)
// }

// func (q *Q) contextUpsert(ctx context.Context, dataSource interface{}, input interface{}, cond *orderedmap.OrderedMap, ignoreFields ...string) (interface{}, error) {
// 	if !skyutl.IsPtrToStruct(input) {
// 		err := errors.New("require a pointer to a model struct")
// 		skylog.Error(err)
// 		return nil, err
// 	}

// 	id, _ := cond.Get("id")
// 	if id != nil && id.(int64) > 0 {
// 		MakeUpdate(ctx, input)
// 	} else {
// 		MakeInsert(ctx, input)
// 	}

// 	updatedRows, err := q.execUpdate(dataSource, input, cond, ignoreFields...)
// 	if err != nil {
// 		skylog.Error(err)
// 		return nil, err
// 	}

// 	if updatedRows > 0 {
// 		return input, nil
// 	}
// 	return q.insert(nil, dataSource, input, ignoreFields...)
// }

// //TxUpdate function
// func (q *Q) TxUpdate(tx *sql.Tx, input interface{}, cond *orderedmap.OrderedMap, ignoreFields ...string) (interface{}, error) {
// 	return q.update(tx, input, cond, ignoreFields...)
// }

// //TxUpdateWithID function
// func (q *Q) TxUpdateWithID(tx *sql.Tx, input interface{}, ignoreFields ...string) (interface{}, error) {
// 	if !skyutl.IsPtrToStruct(input) {
// 		err := errors.New("require a pointer to a model struct")
// 		return nil, err
// 	}

// 	id := skyutl.GetFieldValueOfStruct(input, "Id")
// 	cond := orderedmap.NewOrderedMap()
// 	cond.Set("id", id)
// 	return q.update(tx, input, cond, ignoreFields...)
// }

// func (q *Q) update(dataSource interface{}, input interface{}, cond *orderedmap.OrderedMap, ignoreFields ...string) (interface{}, error) {
// 	if !skyutl.IsStructOrPtrToStruct(input) {
// 		err := errors.New("[input] param must be struct or pointer to struct")
// 		skylog.Error(err)
// 		return nil, err
// 	}

// 	tableName, _ := skyutl.GetStructNameInSnakeCase(input)
// 	ignoreFieldsMap := skyutl.ToMap(true, ignoreFields...)

// 	_sql := []string{"UPDATE " + tableName + " SET "}

// 	updateFieldStr, updateValues := makeUpdateFields(input, ignoreFieldsMap)

// 	_sql = append(_sql, updateFieldStr)
// 	whereFieldStr, condValues := makeWhereCond(cond, len(updateValues))
// 	_sql = append(_sql, " WHERE "+whereFieldStr)

// 	_sql = append(_sql, "RETURNING *")

// 	sqlStr := strings.Join(_sql, " ")

// 	var rows *sql.Rows
// 	var err error

// 	paramValues := append(updateValues, condValues...)

// 	if !DisableInfoLog {
// 		skylog.Info(sqlStr)
// 		skylog.Info(paramValues)
// 	}

// 	if reflect.TypeOf(dataSource).String() == "*sql.Tx" {
// 		rows, err = dataSource.(*sql.Tx).Query(sqlStr, paramValues...)
// 	} else {
// 		rows, err = dataSource.(*sql.DB).Query(sqlStr, paramValues...)
// 	}

// 	if err != nil {
// 		skylog.Error(err)
// 		return nil, err
// 	}
// 	defer rows.Close()
// 	columns, err := rows.Columns()
// 	if err != nil {
// 		skylog.Error(err)
// 		return nil, err
// 	}

// 	count := len(columns)
// 	out := reflect.New(getStructType(input)).Interface()
// 	hasUpdated := false
// 	for rows.Next() {
// 		values := make([]interface{}, count)
// 		valuePtrs := make([]interface{}, count)
// 		for i := range columns {
// 			valuePtrs[i] = &values[i]
// 		}
// 		rows.Scan(valuePtrs...)
// 		fields := []string{}
// 		for i, col := range columns {
// 			val := values[i]

// 			b, ok := val.([]byte)
// 			var v interface{}
// 			if ok {
// 				v = string(b)
// 			} else {
// 				v = val
// 			}

// 			fields = append(fields, col)
// 			values[i] = v
// 		}
// 		fillOut(fields, values, out)
// 		hasUpdated = true
// 	}
// 	if hasUpdated {
// 		return out, nil
// 	}
// 	return nil, nil
// }

// //ExecUpdate function
// func (q *Q) ExecUpdate(input interface{}, cond *orderedmap.OrderedMap, ignoreFields ...string) (interface{}, error) {
// 	return q.execUpdate(q.DB, input, cond, ignoreFields...)
// }

// //TxExecUpdate function
// func (q *Q) TxExecUpdate(tx *sql.Tx, input interface{}, cond *orderedmap.OrderedMap, ignoreFields ...string) (interface{}, error) {
// 	return q.execUpdate(tx, input, cond, ignoreFields...)
// }

// func (q *Q) execUpdate(dataSource interface{}, input interface{}, cond *orderedmap.OrderedMap, ignoreFields ...string) (int64, error) {
// 	if !skyutl.IsStructOrPtrToStruct(input) {
// 		return 0, errors.New("[input] param must be struct or pointer to struct")
// 	}

// 	tableName, _ := skyutl.GetStructNameInSnakeCase(input)
// 	ignoreFieldsMap := skyutl.ToMap(true, ignoreFields...)

// 	_sql := []string{"UPDATE " + tableName + " SET "}

// 	updateFieldStr, updateValues := makeUpdateFields(input, ignoreFieldsMap)

// 	_sql = append(_sql, updateFieldStr)
// 	whereFieldStr, condValues := makeWhereCond(cond, len(updateValues))
// 	_sql = append(_sql, " WHERE "+whereFieldStr)

// 	_sql = append(_sql, "RETURNING *")

// 	sqlStr := strings.Join(_sql, " ")

// 	var result sql.Result
// 	var err error

// 	paramValues := append(updateValues, condValues...)

// 	if !DisableInfoLog {
// 		skylog.Info(sqlStr)
// 		skylog.Info(paramValues)
// 	}

// 	if reflect.TypeOf(dataSource).String() == "*sql.Tx" {
// 		result, err = dataSource.(*sql.Tx).Exec(sqlStr, paramValues...)
// 	} else {
// 		result, err = dataSource.(*sql.DB).Exec(sqlStr, paramValues...)
// 	}

// 	if err != nil {
// 		skylog.Error(err)
// 		return 0, err
// 	}

// 	rowsAffected, _ := result.RowsAffected()

// 	return rowsAffected, nil
// }
