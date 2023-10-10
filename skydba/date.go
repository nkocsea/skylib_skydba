package skydba

const (
	NULL_DATE = -9999999999999
	MIN_DATE  = -8888888888888
	MAX_DATE  = 99999999999999
)

//GetCurrentMillis function return current date time from database in milliseconds
func GetCurrentMillis() (int64, error) {
	q := DefaultQuery()

	sql := `SELECT * FROM date_generator() as date`

	var date int64
	if err := q.Query(sql, []interface{}{}, &date); err != nil {
		return -1, err
	}

	return date, nil
}
