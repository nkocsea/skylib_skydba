package skydba

import (
	"database/sql"
)

type SimpleAccountInfo struct {
	Id          int64  `json:"id"`
	PartnerId   int64  `json:"partnerId"`
	PartnerCode string `json:"code"`
	Username    string `json:"username"`
	Title       string `json:"title"`
	Name        string `json:"name"`
	FirstName   string `json:"firstName"`
	MiddleName  string `json:"middleName"`
	LastName    string `json:"lastName"`
}

type SimpleDeviceInfo struct {
	Id        int64  `json:"id"`
	AccountId int64  `json:"accountId"`
	DeviceId  string `json:"deviceId"`
	Token     string `json:"ioken"`
}

func GetAvailableAccountById(tx *sql.Tx, id int64) (*SimpleAccountInfo, error) {
	_sql := `
		select t1.id
			, t2.id as partner_id
			, t2.code as partner_code
			, t1.username
			, t2.title
			, t2.name
			, t2.first_name
			, t2.middle_name
			, t2.last_name
		from account t1
		left join partner t2 on (t2.account_id = t1.id and t2.disabled = 0 and t2.deleted_by <= 0::bigint)
		where t1.id = $1
		and t1.disabled = 0
		and t1.deleted_by <= 0::bigint
	`

	var items []*SimpleAccountInfo
	var err error

	q := DefaultQuery()
	if tx != nil {
		err = q.TxQuery(tx, _sql, []interface{}{id}, &items)
	} else {
		err = q.Query(_sql, []interface{}{id}, &items)
	}
	if err != nil {
		return nil, err
	}
	if len(items) > 0 && items[0].Id > 0 {
		return items[0], nil
	}
	return nil, nil
}

func GetAvailableDeviceByAccountId(tx *sql.Tx, accountId int64) ([]*SimpleDeviceInfo, error) {
	_sql := `
		select t1.id
			, t1.account_id
			, t1.device_id
			, t1."token"
		from device_token t1
		where t1.account_id = $1
		and t1.disabled = 0
		and t1.deleted_by <= 0::bigint
	`

	var items []*SimpleDeviceInfo
	var err error

	q := DefaultQuery()
	if tx != nil {
		err = q.TxQuery(tx, _sql, []interface{}{accountId}, &items)
	} else {
		err = q.Query(_sql, []interface{}{accountId}, &items)
	}
	if err != nil {
		return items, err
	}
	return items, nil
}
