package skydba

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"suntech.com.vn/skylib/skylog.git/skylog"
)

//MainDB main db connection
var MainDB *sql.DB

//ConnectDB function
func ConnectDB(appName, dataSourceDesc, driver, host, dbname, username, password string, port, timeout, reconnect int32) *sql.DB {
	connectionStr := fmt.Sprintf("host=%v application_name=%v dbname=%v user=%v password=%v port=%v connect_timeout=%v sslmode=disable",
		host, appName, dbname, username, password, port, timeout)
	DB, err := sql.Open(driver, connectionStr)
	DB.SetMaxOpenConns(256)
	if err != nil {
		skylog.Error(err)
		skylog.Info(fmt.Sprintf("Reconnect %v in %v seconds", dataSourceDesc, reconnect))
		time.Sleep(time.Duration(reconnect) * time.Second)
		return ConnectDB(appName, dataSourceDesc, driver, host, dbname, username, password, port, timeout, reconnect)
	}

	if err := DB.Ping(); err != nil {
		skylog.Error(err)
		skylog.Info(fmt.Sprintf("Reconnect %v in %v seconds", dataSourceDesc, reconnect))
		time.Sleep(time.Duration(reconnect) * time.Second)
		return ConnectDB(appName, dataSourceDesc, driver, host, dbname, username, password, port, timeout, reconnect)
	}

	return DB
}

//Init function
func Init(appName, dataSourceDesc string, driver, host, dbname, username, password string, port, timeout, reconnect int32) {
	MainDB = ConnectDB(appName, dataSourceDesc, driver, host, dbname, username, password, port, timeout, reconnect)
}

//BeginTx function
func BeginTx(db sql.DB) (*sql.Tx, error) {
	return db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelSerializable})
}

//DefaultBeginTx function
func DefaultBeginTx() (*sql.Tx, error) {
	return MainDB.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelSerializable})
}
