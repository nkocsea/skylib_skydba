package main

import (
	"context"
	"fmt"

	// "time"
	_ "github.com/lib/pq"
	// "encoding/json"
	"suntech.com.vn/skylib/skydba.git/skydba"
	// "suntech.com.vn/skylib/skyutl.git/report"
	// "github.com/elliotchance/orderedmap"
)

type RefreshToken struct {
	Id        int64   `json:"id"`
	Token     *string `json:"token" orm:"null"`
	AccountId int64   `json:"accountId" orm:"null"`
	CreatedAt *int64  `json:"createdAt" orm:"null"`
}

// Role struct
type RoleRole struct {
	TableName string `default:"role"`
	Id        int64  `json:"id"`
	OrgId     int64  `json:"orgId" orm:"null"`
	Code      string `json:"code" orm:"null"`
	Name      string `json:"name" orm:"null"`
	Sort      int32  `json:"sort" orm:"null"`
	Disabled  bool   `json:"disabled" orm:"null"`
	CreatedBy int64  `json:"createdBy" orm:"null"`
	CreatedAt int64  `json:"createdAt" orm:"null"`
	UpdatedBy int64  `json:"updatedBy" orm:"null"`
	UpdatedAt int64  `json:"updatedAt" orm:"null"`
	DeletedBy int64  `json:"deletedBy" orm:"null"`
	Version   int32  `json:"version" orm:"null"`
}

type customType struct {
	Id          int64
	Name        string
	Value       string
	Score       string
	Description string
}
type TestStruct struct {
	PersonalCategoryId   int64
	PersonalCategoryName string
	DetailData           []customType
}

type Role struct {
	Id int64
	// SystemTableName string `protobuf:"bytes,9999,opt,name=system_table_name,json=system_table_name,table_name=roleaa,proto3" json:"system_table_name,omitempty"`
	// SystemTableName   string  `protobuf:"bytes,9999,opt,name=system_table_name,json=system_table_name,table_name=emr_vital,proto3" json:"system_table_name,omitempty"`
	Name      string
	Code      string
	Sort      int32
	UpdatedBy int64
	DeletedBy int64
	DeletedAt int64
}

type ReportTemplate struct {
	Id         int64
	Name       string
	Background []byte
}

type Org struct {
	Id        int64
	Name      string
	IconData  []byte `json:"icon_data"`
	DeletedBy int64
	UpdatedBy int64
}

func main() {
	skydba.Init("", "Main Data Source", "postgres", "103.119.86.118", "skyonev3", "skyonev3", "skyonev3", 19999, 2, 2)
	// q := skydba.DefaultQuery()

	// org := Org{Name: "abcd", DeletedBy: 111}
	// if err := q.Upsert(context.Background(), &org, []string{"id"}); err != nil {
	// 	fmt.Println(err)
	// }

	// fmt.Println(org)

	// res := [][]uint8{}
	// if err := q.CallFunc("test123", "", nil, &res); err != nil {
	// 	fmt.Println(err)
	// }

	// fmt.Println(string(res[0]))

	// r := ReportTemplate{Id: 2788411727765571763}

	// fmt.Println(q.ReadWithID(&r, ""))
	// fmt.Println(r)
	// role := []*Role{}

	// report.GeneratePdf("", int64(1), q, nil, nil, nil, nil)
	// report.DetectImageType([]byte{1, 2})

	// roleList := []*Role{}
	// s1 := ""
	// for i := 1; i < 60; i++ {
	// 	role := Role{
	// 		Code: fmt.Sprintf("Code %v", i),
	// 		Name: fmt.Sprintf("Name %v", i),
	// 		Sort: int32(i),
	// 	}
	// 	s1 += fmt.Sprintf("%v", i)
	// 	roleList = append(roleList, &role)
	// }

	// role := Role {
	// 		Name: "xxxxxxxxx",
	// 		Code: "yyyxxxx",
	// 	}

	// skydba.DisableInfoLog = true
	// start := time.Now().UnixNano() / (int64(time.Millisecond)/int64(time.Nanosecond))
	// fmt.Println(skydba.GenerateID(1000, q))

	// end := time.Now().UnixNano() / (int64(time.Millisecond)/int64(time.Nanosecond))
	// fmt.Println("Took: " + fmt.Sprintf("%v millisecond(s)", (end - start)))
	// q.Test()
	// fmt.Println(role)

	// tx, _ := skydba.DefaultBeginTx()
	// fmt.Println(q.BatchUpsertWithReturn(context.Background(), &roleList, []string{"id"}))
	// fmt.Println(q.BatchUpsertWithReturn(context.Background(), &roleList, []string{"id"}))
	// tx.Rollback()
	// tx.Commit()

	// s2 := ""
	// for _, row := range roleList {
	// 	s2 += fmt.Sprintf("%v", row.Sort)
	// }

	// fmt.Println(s1 == s2)
	// fmt.Println(roleList[1000])

	// orgInfo := &report.OrgInfo{Id: 200021}
	// skydba.GetOrgByID(orgInfo)
	// fmt.Println(orgInfo)

	// t := &report.HtmlReportTemplate{}

	// q.Query("select id, name, xlsx_template from html_report_template", []interface{}{}, t)
	// fmt.Println(t.Id, t.Name, t.XlsxTemplate)

	// sql := `SELECT work_lab_sample_waiting($1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
	// 	$11, $12, $13, $14, $15, $16, $17, $18)`
	// var cursorName string
	// params := []interface{}{
	// 	1,
	// 	100021,
	// 	"",
	// 	0,
	// 	0,
	// 	0,
	// 	"lab211",
	// 	6,
	// 	9,
	// 	1637254800000,
	// 	1642525199421,
	// 	1637254800000,
	// 	1642525199421,
	// 	"%%",
	// 	0,
	// 	"",
	// 	0,
	// 	20,
	// }

	// q.Query(sql, params, &cursorName)

	// fmt.Println(cursorName)

	// fetchQ := `fetch all in "` + cursorName + `"`

	// fmt.Println(skyutl.GetFieldValueOfStruct(role, "TableName"))

	// func (q *Q) NextSeqWithOptions(ctx context.Context, name string, options map[string]interface{}) (string, error) {

	// options := map[string]interface{}{
	// 	"NUM_FORMAT":         "0000000",   //default: ""
	// 	"PREFIX":             "XN",        //default: ""
	// 	"SUFFIX":             "xx",        //default: ""
	// 	"SEPARATOR":          "-",         //default: ""
	// 	"DATE_FORMAT":        "060w",      //2006-01-02 -> Java: yyyy-MM-dd, default: ""
	// 	"DATE_VALUE_IN_MILI": int64(1111), //default: NULL_DATE (-9999999999999)
	// 	"SAVE_TO_DB":         true,        //default: true
	// 	"COMPANY_ID":         int64(-1),   //default: 0
	// 	"BRANCH_ID":          int64(-1),   //default: 0
	// }
	// NextSeq(ctx context.Context, name, prefix string, sysdate int64, dateFormat, seqFormat, separator, suffix string)
	// NextSeq(ctx context.Context, name string, sysdate int64, dateFormat, seqFormat string) (string, error)
	// options := map[string]interface{}{
	// 	"NUM_FORMAT":          "0000000",            //default: ""
	// 	"PREFIX":              "XN",                 //default: ""
	// 	"SUFFIX":			   "",                   //default: ""
	// 	"SEPARATOR":           "-",                  //default: ""
	// 	"DATE_FORMAT":         "0601",               //2006-01-02 -> Java: yyyy-MM-dd, default: ""
	// 	"DATE_VALUE_IN_MILI":  int64(1627948230422), //default: NULL_DATE (-9999999999999)
	// 	"SAVE_TO_DB":          true,                 //default: true
	// 	"COMPANY_ID":          int64(1),             //default: 0
	// 	"BRANCH_ID":           int64(2),             //default: 0
	// 	"NUMBER_OF_CODE":       1,                   //default: 1
	// }

	// codeList, err := q.NextCodeListWithOptions(context.Background(), "ITEM", options)

	// if err != nil {
	// 	fmt.Println(err)
	// }

	// fmt.Println(codeList) // => 21080000001

	// const _sql = "SELECT * FROM find_recruitment_request($1)"
	// params := []interface{}{1}
	// rows, err := q.DB.Query(_sql, params...)
	// if err != nil {
	// 	skylog.Error(err)
	// 	return
	// }
	// defer rows.Close()

	// results := []TestStruct{}
	// for rows.Next() {
	// 	var personalCategoryId int64
	// 	var personalCategoryName string
	// 	var detailData []customType

	// 	if err := rows.Scan(&personalCategoryId, &personalCategoryName, pq.Array(&detailData)); err != nil {
	// 		skylog.Error(err)
	// 		break
	// 	}

	// 	results = append(results, TestStruct{PersonalCategoryId: personalCategoryId, PersonalCategoryName: personalCategoryName, DetailData: detailData})
	// }

	// fmt.Println(results)

	// partnerCache := skydba.NewPartnerCache("localhost", 6379, "", 0)

	// partnerCache.Set("keyaaa", &skydba.Partner{
	// 	Id:       int64(1),
	// 	LastName: "abc",
	// })

	// fmt.Println(partnerCache.Get("keyaaa"))

	err := skydba.SendNotify(context.Background(), []int64{1}, "Test message 01", "Body message 01")
	if err != nil {
		fmt.Printf("err: %v\n", err)
	}
	fmt.Printf("\"SUCCESS\": %v\n", "SUCCESS")
}
