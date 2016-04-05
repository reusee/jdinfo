package main

import "github.com/jmoiron/sqlx"
import _ "github.com/go-sql-driver/mysql"

var db *sqlx.DB

func init() {
	var err error
	db, err = sqlx.Connect("mysql", "root:ffffff@tcp(127.0.0.1:3306)/jd?parseTime=true&autocommit=true")
	ce(err, "connect to db")
	initSchema()
}

func initSchema() {
	db.MustExec(`
	CREATE TABLE IF NOT EXISTS items (
		sku BIGINT PRIMARY KEY,
		shop_id INT NOT NULL,
		category INT NOT NULL
	)
	ROW_FORMAT=COMPRESSED
	`)

	db.MustExec(`
	CREATE TABLE IF NOT EXISTS shops (
		shop_id INT PRIMARY KEY,
		location CHAR(32),
		name CHAR(128),
		INDEX location (location)
	)
	ROW_FORMAT=COMPRESSED
	`)

	db.MustExec(`
	CREATE TABLE IF NOT EXISTS ranks (
		sku BIGINT NOT NULL,
		date CHAR(8) NOT NULL,
		rank INT NOT NULL,
		UNIQUE INDEX sku_date (sku, date),
		INDEX date (date)
	)
	ROW_FORMAT=COMPRESSED
	`)

}
