package main

import "github.com/jmoiron/sqlx"
import _ "github.com/lib/pq"

var db *sqlx.DB

func init() {
	var err error
	db, err = sqlx.Connect("postgres", "user=reus dbname=jd sslmode=disable")
	ce(err, "connect to db")
	initSchema()
}

func initSchema() {
	db.MustExec(`
	CREATE TABLE IF NOT EXISTS items (
		sku BIGINT PRIMARY KEY,
		shop_id INT NOT NULL,
		category INT NOT NULL,
		title TEXT
	)
	`)

	db.MustExec(`
	CREATE TABLE IF NOT EXISTS shops (
		shop_id INT PRIMARY KEY,
		location TEXT,
		name TEXT
	)
	`)
	db.MustExec(`
	CREATE INDEX IF NOT EXISTS location ON shops (location)
	`)

	db.MustExec(`
	CREATE TABLE IF NOT EXISTS infos (
		sku BIGINT NOT NULL,
		date TEXT NOT NULL,
		category INT NOT NULL,
		rank INT NOT NULL,
		sales INT NOT NULL,
		price DECIMAL(10, 2) NOT NULL
	)
	`)
	db.MustExec(`CREATE UNIQUE INDEX IF NOT EXISTS sku_date_cat 
		ON infos
		(sku, date, category)`)
	db.MustExec(`CREATE INDEX IF NOT EXISTS date ON infos (date)`)
	db.MustExec(`CREATE INDEX IF NOT EXISTS sales ON infos (sales)`)
	db.MustExec(`CREATE INDEX IF NOT EXISTS category ON infos (category)`)

	db.MustExec(`
	CREATE TABLE IF NOT EXISTS images (
		sku BIGINT,
		url_id INT
	)
	`)
	db.MustExec(`CREATE UNIQUE INDEX IF NOT EXISTS sku_image ON images
		(sku, url_id)`)
	db.MustExec(`CREATE INDEX IF NOT EXISTS url_id ON images (url_id)`)

	db.MustExec(`
	CREATE TABLE IF NOT EXISTS urls (
		url_id SERIAL PRIMARY KEY,
		url TEXT NOT NULL,
		sha512 BYTEA
	)
	`)
	db.MustExec(`CREATE UNIQUE INDEX IF NOT EXISTS url ON urls (url)`)
	db.MustExec(`CREATE INDEX IF NOT EXISTS sha512 ON urls (sha512)`)

}
