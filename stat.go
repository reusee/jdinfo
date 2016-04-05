//+build no

package main

import (
	"os"

	"github.com/jmoiron/sqlx"
)

import _ "github.com/go-sql-driver/mysql"

import "fmt"

var db *sqlx.DB

func init() {
	var err error
	db, err = sqlx.Connect("mysql", "root:ffffff@tcp(127.0.0.1:3306)/jd?parseTime=true&autocommit=true")
	ce(err, "connect to db")
}

func main() {
	date1 := os.Args[1]
	date2 := os.Args[2]
	category := os.Args[3]

	type Entry struct {
		Sku    int64
		Rank   int
		ShopId int `db:"shop_id"`
	}
	getRanks := func(date string, category string) map[int64]*Entry {
		rows, err := db.Queryx(`SELECT a.sku, rank, shop_id FROM ranks a
		LEFT JOIN items b
		ON a.sku = b.sku
		WHERE category = ?
		AND date = ?
		`, category, date)
		ce(err, "query")
		ranks := make(map[int64]*Entry)
		for rows.Next() {
			entry := new(Entry)
			ce(rows.StructScan(entry), "scan")
			ranks[entry.Sku] = entry
		}
		ce(rows.Err(), "rows err")
		return ranks
	}
	ranks1 := getRanks(date1, category)
	if len(ranks1) == 0 {
		panic(me(nil, "invalid date1 %s", date1))
	}
	ranks2 := getRanks(date2, category)
	if len(ranks2) == 0 {
		panic(me(nil, "invalid date1 %s", date1))
	}

	for sku, entry2 := range ranks2 {
		entry1, ok := ranks1[sku]
		if ok && entry2.Rank > entry1.Rank {
			fmt.Printf("cur %-10d prev %-10d delta %-10d shop %-10d sku %-15d\n",
				entry2.Rank, entry1.Rank, entry2.Rank-entry1.Rank, entry2.ShopId, sku)
		}
	}

}

type Err struct {
	Pkg  string
	Info string
	Prev error
}

func (e *Err) Error() string {
	if e.Prev == nil {
		return fmt.Sprintf("%s: %s", e.Pkg, e.Info)
	}
	return fmt.Sprintf("%s: %s\n%v", e.Pkg, e.Info, e.Prev)
}

func (e *Err) Origin() error {
	var ret error = e
	for err, ok := ret.(*Err); ok && err.Prev != nil; err, ok = ret.(*Err) {
		ret = err.Prev
	}
	return ret
}

func me(err error, format string, args ...interface{}) *Err {
	if len(args) > 0 {
		return &Err{
			Pkg:  `jdinfostat`,
			Info: fmt.Sprintf(format, args...),
			Prev: err,
		}
	}
	return &Err{
		Pkg:  `jdinfostat`,
		Info: format,
		Prev: err,
	}
}

func ce(err error, format string, args ...interface{}) {
	if err != nil {
		panic(me(err, format, args...))
	}
}

func ct(err *error) {
	if p := recover(); p != nil {
		if e, ok := p.(error); ok {
			*err = e
		} else {
			panic(p)
		}
	}
}
