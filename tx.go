package main

import (
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
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
		if err, ok := err.(*pq.Error); ok && err.Code.Name() == "deadlock_detected" {
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
		if err, ok := err.(*pq.Error); ok && err.Code.Name() == "deadlock_detected" {
			goto begin
		}
		return err
	}
	return nil
}

/*
func withMysqlTx(db *sqlx.DB, fn func(tx *sqlx.Tx) error) error {
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
*/
