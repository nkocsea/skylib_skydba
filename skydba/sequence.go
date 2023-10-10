package skydba

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
	"strconv"
	"suntech.com.vn/skylib/skyutl.git/skyutl"
	"reflect"
	"suntech.com.vn/skylib/skylog.git/skylog"

	"github.com/elliotchance/orderedmap"
)

type sequence struct {
	Id        int64
	CompanyId int64
	BranchId  int64
	Name      string
	Value     int64
	Prefix    string
	Format    string
	UpdatedAt int64
}

// Function NextCode
// code, err := q.NextCode(context.Background(), "ITEM", "XN", skydba.NULL_DATE, "0601", "0000000", "", "")
// 	if err != nil {
// 		fmt.Println(err)
// 	}
// 	fmt.Println(code) // => XN21080000001
func (q *Q) NextCode(ctx context.Context, name, prefix string, sysdate int64, dateFormat, seqFormat, separator, suffix string) (string, error) {
	return q.nextCode(q.DB, ctx, name, prefix, sysdate, dateFormat, seqFormat, separator, suffix, int64(0), int64(0))
}

func (q *Q) NextCodeB(ctx context.Context, name, prefix string, sysdate int64, dateFormat, seqFormat, separator, suffix string) (string, error) {
	return q.nextCode(q.DB, ctx, name, prefix, sysdate, dateFormat, seqFormat, separator, suffix, int64(0), int64(-1))
}

func (q *Q) NextCodeC(ctx context.Context, name, prefix string, sysdate int64, dateFormat, seqFormat, separator, suffix string) (string, error) {
	return q.nextCode(q.DB, ctx, name, prefix, sysdate, dateFormat, seqFormat, separator, suffix, int64(-1), int64(-1))
}

func (q *Q) TxNextCode(tx *sql.Tx, ctx context.Context, name, prefix string, sysdate int64, dateFormat, seqFormat, separator, suffix string) (string, error) {
	return q.nextCode(tx, ctx, name, prefix, sysdate, dateFormat, seqFormat, separator, suffix, int64(0), int64(0))
}

func (q *Q) TxNextCodeB(tx *sql.Tx, ctx context.Context, name, prefix string, sysdate int64, dateFormat, seqFormat, separator, suffix string) (string, error) {
	return q.nextCode(tx, ctx, name, prefix, sysdate, dateFormat, seqFormat, separator, suffix, int64(0), int64(-1))
}

func (q *Q) TxNextCodeC(tx *sql.Tx, ctx context.Context, name, prefix string, sysdate int64, dateFormat, seqFormat, separator, suffix string) (string, error) {
	return q.nextCode(tx, ctx, name, prefix, sysdate, dateFormat, seqFormat, separator, suffix, int64(-1), int64(-1))
}

// seq, err := q.NextSeq(context.Background(), "ITEM", skydba.NULL_DATE, "0601", "0000000")
// 	if err != nil {
// 		fmt.Println(err)
// 	}
// 	fmt.Println(seq) // => 21080000001
func (q *Q) NextSeq(ctx context.Context, name string, sysdate int64, dateFormat, seqFormat string) (string, error) {
	return q.nextSeq(q.DB, ctx, name, sysdate, dateFormat, seqFormat, int64(0), int64(0))
}

func (q *Q) NextSeqB(ctx context.Context, name string, sysdate int64, dateFormat, seqFormat string) (string, error) {
	return q.nextSeq(q.DB, ctx, name, sysdate, dateFormat, seqFormat, int64(0), int64(-1))
}

func (q *Q) NextSeqC(ctx context.Context, name string, sysdate int64, dateFormat, seqFormat string) (string, error) {
	return q.nextSeq(q.DB, ctx, name, sysdate, dateFormat, seqFormat, int64(-1), int64(-1))
}

func (q *Q) TxNextSeq(tx *sql.Tx, ctx context.Context, name string, sysdate int64, dateFormat, seqFormat string) (string, error) {
	return q.nextSeq(tx, ctx, name, sysdate, dateFormat, seqFormat, int64(0), int64(0))
}

func (q *Q) TxNextSeqB(tx *sql.Tx, ctx context.Context, name string, sysdate int64, dateFormat, seqFormat string) (string, error) {
	return q.nextSeq(tx, ctx, name, sysdate, dateFormat, seqFormat, int64(0), int64(-1))
}

func (q *Q) TxNextSeqC(tx *sql.Tx, ctx context.Context, name string, sysdate int64, dateFormat, seqFormat string) (string, error) {
	return q.nextSeq(tx, ctx, name, sysdate, dateFormat, seqFormat, int64(-1), int64(-1))
}

func (q *Q) nextSeq(dataSource interface{}, ctx context.Context, name string, sysdate int64, dateFormat, seqFormat string, companyID, branchID int64) (string, error) {
	return q.nextCode(dataSource, ctx, name, "", sysdate, dateFormat, seqFormat, "", "", companyID, branchID)
}

func (q *Q) nextCode(dataSource interface{}, ctx context.Context, name, prefix string, sysdate int64, dateFormat, seqFormat, separator, suffix string, companyID, branchID int64) (string, error) {
	options := map[string]interface{}{}

	if prefix != "" {
		options["PREFIX"] = prefix
	}

	if sysdate != NULL_DATE {
		options["DATE_VALUE_IN_MILI"] = sysdate
	}

	if dateFormat != "" {
		options["DATE_FORMAT"] = dateFormat
	}

	if seqFormat != "" {
		options["NUM_FORMAT"] = seqFormat
	}

	if separator != "" {
		options["SEPARATOR"] = separator
	}

	if suffix != "" {
		options["SUFFIX"] = suffix
	}

	if companyID != 0 {
		options["COMPANY_ID"] = companyID
	}

	if branchID != 0 {
		options["BRANCH_ID"] = branchID
	}

	return q.nextCodeWithOptions(dataSource, ctx, name, options)
}

func convertMilisecondToTime(milliseconds int64) time.Time {
	return time.Unix(0, milliseconds*int64(time.Millisecond))
}

//function NextCodeWithOptions
// options := map[string]interface{}{
// 	"NUM_FORMAT":          "0000000",            //default: ""
// 	"PREFIX":              "XN",                 //default: ""
//	"SUFFIX":			   "",                   //default: ""
// 	"SEPARATOR":           "-",                  //default: ""
// 	"DATE_FORMAT":         "0601",               //2006-01-02 -> Java: yyyy-MM-dd, default: ""
// 	"DATE_VALUE_IN_MILI":  int64(1627948230422), //default: NULL_DATE (-9999999999999)
// 	"SAVE_TO_DB":          true,                 //default: true
// 	"COMPANY_ID":          int64(1),             //default: 0
//	"BRANCH_ID":           int64(2),             //default: 0
// }
// result, err := q.NextCodeWithOptions(context.Background(), "ITEM", options)
func (q *Q) NextCodeWithOptions(ctx context.Context, name string, options map[string]interface{}) (string, error) {
	return q.nextCodeWithOptions(q.DB, ctx, name, options)
}

func (q *Q) TxNextCodeWithOptions(tx *sql.Tx, ctx context.Context, name string, options map[string]interface{}) (string, error) {
	return q.nextCodeWithOptions(tx, ctx, name, options)
}

func (q *Q) nextCodeWithOptions(dataSource interface{}, ctx context.Context, name string, options map[string]interface{}) (string, error) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	_, companyID, branchID, _, err := skyutl.GetLoginInfo(ctx)
	if err != nil {
		skylog.Error(err)
		return "", err
	}

	if companyID == 0 {
		return "", skyutl.Error400("SYS.MSG.REQUIRE_COMPANY_ID", "", nil)
	}

	if branchID == 0 {
		return "", skyutl.Error400("SYS.MSG.REQUIRE_BRANCH_ID", "", nil)
	}

	if strings.Trim(name, " ") == "" {
		return "", skyutl.Error400("SYS.MSG.REQUIRE_SEQ_NAME", "", nil)
	}
	seq := sequence{}
	cond := orderedmap.NewOrderedMap()

	separator := ""
	saveToDB := true
	dateValueInMili := int64(NULL_DATE)
	dateFormat := ""
	prefix := ""
	suffix := ""
	numFormat := ""

	for key, value := range options {
		if key == "PREFIX" {
			prefix = strings.Trim(value.(string), " ")
		}

		if key == "SUFFIX" {
			suffix = value.(string)
		}

		if key == "NUM_FORMAT" {
			numFormat = strings.Trim(value.(string), " ")
		}

		if key == "DATE_FORMAT" {
			dateFormat = strings.Trim(value.(string), " ")
		}

		if key == "DATE_VALUE_IN_MILI" {
			dateValueInMili = value.(int64)
		}

		if key == "SEPARATOR" {
			separator = value.(string)
		}

		if key == "SAVE_TO_DB" {
			saveToDB = value.(bool)
		}

		if key == "COMPANY_ID" {
			_companyID := value.(int64)
			if _companyID != 0 {
				companyID = _companyID
			}
		}

		if key == "BRANCH_ID" {
			_branchID := value.(int64)

			if _branchID != 0 {
				branchID = _branchID
			}
		}
	}

	dateValueStr := ""
	now, err := GetCurrentMillis()
	if err != nil {
		skylog.Error(err)
		return "", err
	}

	if dateFormat != "" {
		if dateValueInMili == NULL_DATE {
			dateValueInMili = now
		}
		if strings.Contains(dateFormat, "w") {
			_, numOfWeek := convertMilisecondToTime(dateValueInMili).ISOWeek()
			if strings.Contains(dateFormat, "0w") {
				dateValueStr = convertMilisecondToTime(dateValueInMili).Format(strings.Replace(dateFormat, "0w", "", -1)) + fmt.Sprintf("%02d", numOfWeek)
			} else {
				dateValueStr = convertMilisecondToTime(dateValueInMili).Format(strings.Replace(dateFormat, "w", "", -1)) + fmt.Sprintf("%v", numOfWeek)
			}

		} else {
			dateValueStr = convertMilisecondToTime(dateValueInMili).Format(dateFormat)
		}

	}

	nowTime := convertMilisecondToTime(dateValueInMili)

	if dateFormat != "" {
		if strings.Contains(dateFormat, "w") {
			_, numOfWeek := nowTime.ISOWeek()
			if strings.Contains(dateFormat, "0w") {
				dateFormat = strings.Replace(dateFormat, "0w", "", -1)
				if prefix != "" {
					cond.Set("name", fmt.Sprintf("%v.%v.%v.%02d", name, prefix, dateFormat, numOfWeek))
				} else {
					cond.Set("name", fmt.Sprintf("%v.%v.%02d", name, dateFormat, numOfWeek))
				}
			} else {
				dateFormat = strings.Replace(dateFormat, "w", "", -1)
				if prefix != "" {
					cond.Set("name", fmt.Sprintf("%v.%v.%v.%v", name, prefix, dateFormat, numOfWeek))
				} else {
					cond.Set("name", fmt.Sprintf("%v.%v.%v", name, dateFormat, numOfWeek))
				}
			}
		} else {
			if prefix != "" {
				cond.Set("name", fmt.Sprintf("%v.%v.%v", name, prefix, dateFormat))
			} else {
				cond.Set("name", fmt.Sprintf("%v.%v", name, dateFormat))
			}
		}
	} else {
		if prefix != "" {
			cond.Set("name", fmt.Sprintf("%v.%v", name, prefix))
		} else {
			cond.Set("name", name)
		}

	}

	cond.Set("company_id", companyID)
	cond.Set("branch_id", branchID)

	if err := q.ReadWithCond(&seq, "", cond); err != nil {
		skylog.Error(err)
		return "", err
	}

	seq.Format = numFormat
	seq.Prefix = prefix
	if seq.Id == 0 {
		seq.CompanyId = companyID
		seq.BranchId = branchID
		_name, found := cond.Get("name")
		if found {
			seq.Name = _name.(string)
		} else {
			seq.Name = name
		}

		seq.Value = 1
	} else {
		seq.Value++
	}

	if saveToDB {
		if err := q.UpsertWithCond(ctx, &seq, cond); err != nil {
			skylog.Error(err)
			return "", err
		}
	}

	_format := seq.Format + fmt.Sprint(seq.Value)
	if len(_format) <= len(fmt.Sprint(seq.Value)) || len(fmt.Sprint(seq.Value)) > len(seq.Format) {
		return seq.Prefix + separator + dateValueStr + fmt.Sprint(seq.Value) + suffix, nil
	} else {
		return seq.Prefix + separator + dateValueStr + fillPrefixZero(seq.Value, seq.Format) + suffix, nil
	}
}

// version 2
//function nextCodeWithOptionsList
// options := map[string]interface{}{
// 	"NUM_FORMAT":          "0000000",            //default: ""
// 	"PREFIX":              "XN",                 //default: ""
//	"SUFFIX":			   "",                   //default: ""
// 	"SEPARATOR":           "-",                  //default: ""
// 	"DATE_FORMAT":         "0601",               //2006-01-02 -> Java: yyyy-MM-dd, default: ""
// 	"DATE_VALUE_IN_MILI":  int64(1627948230422), //default: NULL_DATE (-9999999999999)
// 	"SAVE_TO_DB":          true,                 //default: true
// 	"COMPANY_ID":          int64(1),             //default: 0
//	"BRANCH_ID":           int64(2),             //default: 0
//  "NUMBER_OF_CODE"       int                   //default: 1
// }
// result, err := q.NextCodeListWithOptions(context.Background(), "ITEM", options)

func (q *Q) NextCodeListWithOptions(ctx context.Context, name string, options map[string]interface{}) ([]string, error) {
	return q.nextCodeListWithOptions(q.DB, ctx, name, options)
}

func (q *Q) TxNextCodeListWithOptions(tx *sql.Tx, ctx context.Context, name string, options map[string]interface{}) ([]string, error) {
	return q.nextCodeListWithOptions(tx, ctx, name, options)
}

func (q *Q) nextCodeListWithOptions(dataSource interface{}, ctx context.Context, name string, options map[string]interface{}) ([]string, error) {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	var err error
	// _, companyID, branchID, _, err := skyutl.GetLoginInfo(ctx)
	companyID := int64(1)
	branchID := int64(2)

	if err != nil {
		skylog.Error(err)
		return nil, err
	}

	if companyID == 0 {
		return nil, skyutl.Error400("SYS.MSG.REQUIRE_COMPANY_ID", "", nil)
	}

	if branchID == 0 {
		return nil, skyutl.Error400("SYS.MSG.REQUIRE_BRANCH_ID", "", nil)
	}

	if strings.Trim(name, " ") == "" {
		return nil, skyutl.Error400("SYS.MSG.REQUIRE_SEQ_NAME", "", nil)
	}
	seq := sequence{}
	
	separator := ""
	saveToDB := true
	dateValueInMili := int64(NULL_DATE)
	dateFormat := ""
	prefix := ""
	suffix := ""
	numFormat := ""
	numberOfCode := 1

	for key, value := range options {
		if key == "PREFIX" {
			prefix = strings.Trim(value.(string), " ")
		}

		if key == "SUFFIX" {
			suffix = value.(string)
		}

		if key == "NUM_FORMAT" {
			numFormat = strings.Trim(value.(string), " ")
		}

		if key == "DATE_FORMAT" {
			dateFormat = strings.Trim(value.(string), " ")
		}

		if key == "DATE_VALUE_IN_MILI" {
			dateValueInMili = value.(int64)
		}

		if key == "SEPARATOR" {
			separator = value.(string)
		}

		if key == "SAVE_TO_DB" {
			saveToDB = value.(bool)
		}

		if key == "COMPANY_ID" {
			_companyID := value.(int64)
			if _companyID != 0 {
				companyID = _companyID
			}
		}

		if key == "BRANCH_ID" {
			_branchID := value.(int64)

			if _branchID != 0 {
				branchID = _branchID
			}
		}

		if key == "NUMBER_OF_CODE" {
			_numberOfCode := value.(int)

			if _numberOfCode > 0 {
				numberOfCode = _numberOfCode
			}
		}
	}

	dateValueStr := ""
	now, err := GetCurrentMillis()
	if err != nil {
		skylog.Error(err)
		return nil, err
	}

	if dateFormat != "" {
		if dateValueInMili == NULL_DATE {
			dateValueInMili = now
		}
		if strings.Contains(dateFormat, "w") {
			_, numOfWeek := convertMilisecondToTime(dateValueInMili).ISOWeek()
			if strings.Contains(dateFormat, "0w") {
				dateValueStr = convertMilisecondToTime(dateValueInMili).Format(strings.Replace(dateFormat, "0w", "", -1)) + fmt.Sprintf("%02d", numOfWeek)
			} else {
				dateValueStr = convertMilisecondToTime(dateValueInMili).Format(strings.Replace(dateFormat, "w", "", -1)) + fmt.Sprintf("%v", numOfWeek)
			}
		} else {
			dateValueStr = convertMilisecondToTime(dateValueInMili).Format(dateFormat)
		}

	}

	nowTime := convertMilisecondToTime(dateValueInMili)

	if dateFormat != "" {
		if strings.Contains(dateFormat, "w") {
			_, numOfWeek := nowTime.ISOWeek()
			if strings.Contains(dateFormat, "0w") {
				dateFormat = strings.Replace(dateFormat, "0w", "", -1)
				if prefix != "" {
					seq.Name = fmt.Sprintf("%v.%v.%v.%02d", name, prefix, dateFormat, numOfWeek)
				} else {
					seq.Name = fmt.Sprintf("%v.%v.%02d", name, dateFormat, numOfWeek)
				}
			} else {
				dateFormat = strings.Replace(dateFormat, "w", "", -1)
				if prefix != "" {
					seq.Name = fmt.Sprintf("%v.%v.%v.%v", name, prefix, dateFormat, numOfWeek)
				} else {
					seq.Name = fmt.Sprintf("%v.%v.%v", name, dateFormat, numOfWeek)
				}
			}
		} else {
			if prefix != "" {
				seq.Name = fmt.Sprintf("%v.%v.%v", name, prefix, dateFormat)
			} else {
				seq.Name = fmt.Sprintf("%v.%v", name, dateFormat)
			}
		}
	} else {
		if prefix != "" {
			seq.Name = fmt.Sprintf("%v.%v", name, prefix)
		} else {
			seq.Name = name
		}

	}

	seq.Format = numFormat
	seq.Prefix = prefix
	seq.CompanyId = companyID
	seq.BranchId = branchID

	_sql := `SELECT * FROM update_sequence_with_lock($1, $2, $3, $4, $5, $6, $7)`

	var ids string

	param := []interface{}{seq.Name, seq.CompanyId, seq.BranchId, seq.Format, seq.Prefix, numberOfCode, saveToDB}
	if reflect.TypeOf(dataSource).String() == "*sql.Tx" {
		if err := q.TxQuery(dataSource.(*sql.Tx), _sql, param, &ids); err != nil {
			return nil, err
		}
	} else {
		if err := q.Query(_sql, param, &ids); err != nil {
			return nil, err
		}
	}


	idStrings := strings.Split(ids, ",")
	res := []string{}
	for i := range idStrings {
		if len(idStrings[i]) > 0 {
			_format := seq.Format + idStrings[i]
			
			if len(_format) <= len(idStrings[i]) || len(idStrings[i]) > len(seq.Format) {
				res = append(res, seq.Prefix + separator + dateValueStr + idStrings[i]+ suffix)
				fmt.Println("aaa ", _format)
			} else {
				res = append(res, seq.Prefix + separator + dateValueStr + fillPrefixZeroStr(idStrings[i], seq.Format) + suffix)
				fmt.Println("bbb ", _format)
			}
		}
	}

	return res, nil
}

func fillPrefixZero(number int64, format string) string {
	_format := format + fmt.Sprint(number)

	return _format[len(_format)-len(format):]
}

func fillPrefixZeroStr(numberAsStr string, format string) string {
	_format := format + numberAsStr

	return _format[len(_format)-len(format):]
}

func GenerateID (num int, q *Q) []int64 {
	return generateID( q.DB, num, q)
}

func TxGenerateID (tx *sql.Tx, num int, q *Q) []int64 {
	return generateID(tx, num, q)
}

func generateID (dataSource interface{}, num int, q *Q) []int64 {
	_sql := `SELECT * FROM generate_id_list($1)`

	var ids string
	
	if reflect.TypeOf(dataSource).String() == "*sql.Tx" {
		if err := q.TxQuery(dataSource.(*sql.Tx), _sql, []interface{}{num}, &ids); err != nil {
			return nil
		}
	} else {
		if err := q.Query(_sql, []interface{}{num}, &ids); err != nil {
			return nil
		}
	}


	idStrings := strings.Split(ids, ",")
	res := []int64{}

	for i := range idStrings {
		i64, err := strconv.ParseInt(strings.Trim(idStrings[i], " "), 10, 64)
		if err == nil {
			res = append(res, i64)
		}
	}

	return res
}