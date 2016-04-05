//+build no

package main

import (
	"os"
	"sort"

	"github.com/jmoiron/sqlx"
)

import _ "github.com/go-sql-driver/mysql"

import "fmt"

import "math/rand"

var db *sqlx.DB

func init() {
	var err error
	db, err = sqlx.Connect("mysql", "root:ffffff@tcp(127.0.0.1:3306)/jd?parseTime=true&autocommit=true")
	ce(err, "connect to db")
}

type Entry struct {
	Sku      int64
	Rank     int
	ShopId   int    `db:"shop_id"`
	ShopName string `db:"shop_name"`
}

func main() {
	prevDate := os.Args[1]
	curDate := os.Args[2]
	category := os.Args[3]

	getRanks := func(date string, category string) map[int64]*Entry {
		rows, err := db.Queryx(`SELECT a.sku, rank, b.shop_id, c.name as shop_name FROM ranks a
		LEFT JOIN items b
		ON a.sku = b.sku
		LEFT JOIN shops c
		ON b.shop_id = c.shop_id
		WHERE category = ?
		AND date = ?
		AND location = "广东  广州市"
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
	prevRanks := getRanks(prevDate, category)
	if len(prevRanks) == 0 {
		panic(me(nil, "invalid prev date %s", prevDate))
	}
	curRanks := getRanks(curDate, category)
	if len(curRanks) == 0 {
		panic(me(nil, "invalid cur date %s", curDate))
	}

	pairs := EntryPair(make([][2]*Entry, 0))
	for sku, curEntry := range curRanks {
		prevEntry, ok := prevRanks[sku]
		if ok && curEntry.Rank < prevEntry.Rank && curEntry.Rank <= 1800 && prevEntry.Rank-curEntry.Rank > 300 {
			pairs = append(pairs, [2]*Entry{
				prevEntry, curEntry,
			})
		}
	}
	pairs.Sort(func(a, b [2]*Entry) bool {
		return (a[0].Rank - a[1].Rank) < (b[0].Rank - b[1].Rank)
	})

	fmt.Printf("%d entries\n", len(pairs))
	for i, pair := range pairs {
		prevEntry, curEntry := pair[0], pair[1]
		delta := prevEntry.Rank - curEntry.Rank
		fmt.Printf("%-4d: cur %3d页%2d位 prev %3d页%2d位 delta %3d页%2d位 shop %-6d %s\n", i,
			curEntry.Rank/60+1, curEntry.Rank%60+1,
			prevEntry.Rank/60+1, prevEntry.Rank%60+1,
			delta/60+1, delta%60+1,
			curEntry.ShopId, curEntry.ShopName)
		fmt.Printf("http://item.jd.com/%d.html\n", curEntry.Sku)
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

type EntryPair [][2]*Entry

func (s EntryPair) Reduce(initial interface{}, fn func(value interface{}, elem [2]*Entry) interface{}) (ret interface{}) {
	ret = initial
	for _, elem := range s {
		ret = fn(ret, elem)
	}
	return
}

func (s EntryPair) Map(fn func([2]*Entry) [2]*Entry) (ret EntryPair) {
	for _, elem := range s {
		ret = append(ret, fn(elem))
	}
	return
}

func (s EntryPair) Filter(filter func([2]*Entry) bool) (ret EntryPair) {
	for _, elem := range s {
		if filter(elem) {
			ret = append(ret, elem)
		}
	}
	return
}

func (s EntryPair) All(predict func([2]*Entry) bool) (ret bool) {
	ret = true
	for _, elem := range s {
		ret = predict(elem) && ret
	}
	return
}

func (s EntryPair) Any(predict func([2]*Entry) bool) (ret bool) {
	for _, elem := range s {
		ret = predict(elem) || ret
	}
	return
}

func (s EntryPair) Each(fn func(e [2]*Entry)) {
	for _, elem := range s {
		fn(elem)
	}
}

func (s EntryPair) Shuffle() {
	for i := len(s) - 1; i >= 1; i-- {
		j := rand.Intn(i + 1)
		s[i], s[j] = s[j], s[i]
	}
}

func (s EntryPair) Sort(cmp func(a, b [2]*Entry) bool) {
	sorter := sliceSorter{
		l: len(s),
		less: func(i, j int) bool {
			return cmp(s[i], s[j])
		},
		swap: func(i, j int) {
			s[i], s[j] = s[j], s[i]
		},
	}
	_ = sorter.Len
	_ = sorter.Less
	_ = sorter.Swap
	sort.Sort(sorter)
}

type sliceSorter struct {
	l    int
	less func(i, j int) bool
	swap func(i, j int)
}

func (t sliceSorter) Len() int {
	return t.l
}

func (t sliceSorter) Less(i, j int) bool {
	return t.less(i, j)
}

func (t sliceSorter) Swap(i, j int) {
	t.swap(i, j)
}

func (s EntryPair) Clone() (ret EntryPair) {
	ret = make([][2]*Entry, len(s))
	copy(ret, s)
	return
}
