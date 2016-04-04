package main

import (
	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

func withTx(db *sqlx.DB, fn func(tx *sqlx.Tx) error) error {
begin:
	tx := db.MustBegin()
	err := fn(tx)
	if err != nil {
		tx.Rollback()
		if e, ok := err.(*Err); ok {
			err = e.Origin()
		}
		if err, ok := err.(*mysql.MySQLError); ok && (err.Number == 1213 || err.Number == 1205) { // restart
			goto begin
		}
		return err
	}
	err = tx.Commit()
	if err != nil {
		tx.Rollback()
		if e, ok := err.(*Err); ok {
			err = e.Origin()
		}
		if err, ok := err.(*mysql.MySQLError); ok && (err.Number == 1213 || err.Number == 1205) { // restart
			goto begin
		}
		return err
	}
	return nil
}
