//+build ignore

package main

import (
	"os"
	"sort"
	"strings"

	"github.com/jmoiron/sqlx"
)

import _ "github.com/lib/pq"

import "fmt"

import "math/rand"

var db *sqlx.DB

func init() {
	var err error
	db, err = sqlx.Connect("postgres", "user=reus dbname=jd sslmode=disable")
	ce(err, "connect to db")
}

type Info struct {
	Sku      int64
	Rank     int
	ShopId   int    `db:"shop_id"`
	ShopName string `db:"shop_name"`
}

var categories = map[string]int{
	"衬衫":    1354,
	"T恤":    1355,
	"针织衫":   1356,
	"雪纺衫":   9713,
	"牛仔裤":   9715,
	"休闲裤":   9717,
	"连衣裙":   9719,
	"半身裙":   9720,
	"短裤":    11991,
	"吊带，背心": 11988,
}

const location = "广东  广州市"
const maxSales = 50
const maxCurPage = 50
const minRankDelta = 30

func main() {
	prevDate := os.Args[1]
	curDate := os.Args[2]
	if len(os.Args) > 3 {
		statKeywords(prevDate, curDate, os.Args[3:])
	} else {
		for catName := range categories {
			statCategory(prevDate, curDate, catName)
		}
	}
}

func statKeywords(prevDate, curDate string, keywords []string) {
	getInfosByKeywords := func(date string, keywords []string) map[int64]*Info {
		keywordConditions := []string{}
		for _, keyword := range keywords {
			keywordConditions = append(keywordConditions, `AND title LIKE '%`+keyword+`%'`)
		}
		rows, err := db.Queryx(`SELECT a.sku, rank, b.shop_id, c.name as shop_name FROM infos a
			LEFT JOIN items b
			ON a.sku = b.sku
			LEFT JOIN shops c
			ON b.shop_id = c.shop_id

			WHERE 
			date = $1
			AND sales < $2
			AND location = $3

			`+strings.Join(keywordConditions, ""),
			date,
			maxSales,
			location,
		)
		ce(err, "query")
		infos := make(map[int64]*Info)
		for rows.Next() {
			info := new(Info)
			ce(rows.StructScan(info), "scan")
			infos[info.Sku] = info
		}
		ce(rows.Err(), "rows err")
		return infos
	}

	prevInfos := getInfosByKeywords(prevDate, keywords)
	if len(prevInfos) == 0 {
		panic(me(nil, "invalid prev date %s", prevDate))
	}
	curInfos := getInfosByKeywords(curDate, keywords)
	if len(curInfos) == 0 {
		panic(me(nil, "invalid cur date %s", curDate))
	}

	pickupItems(prevInfos, curInfos)
}

func statCategory(prevDate, curDate string, catName string) {
	category := categories[catName]
	fmt.Printf("=== category %s %s to %s location %s max sales %d ===\n",
		catName,
		prevDate,
		curDate,
		location,
		maxSales,
	)

	getInfosByCategory := func(date string, category int) map[int64]*Info {
		rows, err := db.Queryx(`SELECT a.sku, rank, b.shop_id, c.name as shop_name FROM infos a
			LEFT JOIN items b
			ON a.sku = b.sku
			LEFT JOIN shops c
			ON b.shop_id = c.shop_id

			WHERE a.category = $1
			AND date = $2
			AND sales < $3
			AND location = $4

			`,
			category,
			date,
			maxSales,
			location,
		)
		ce(err, "query")
		infos := make(map[int64]*Info)
		for rows.Next() {
			info := new(Info)
			ce(rows.StructScan(info), "scan")
			infos[info.Sku] = info
		}
		ce(rows.Err(), "rows err")
		return infos
	}

	prevInfos := getInfosByCategory(prevDate, category)
	if len(prevInfos) == 0 {
		panic(me(nil, "invalid prev date %s", prevDate))
	}
	curInfos := getInfosByCategory(curDate, category)
	if len(curInfos) == 0 {
		panic(me(nil, "invalid cur date %s", curDate))
	}

	pickupItems(prevInfos, curInfos)
}

func pickupItems(prevInfos, curInfos map[int64]*Info) {
	pairs := InfoPair(make([][2]*Info, 0))
	for sku, curInfo := range curInfos {
		prevInfo, ok := prevInfos[sku]
		if ok && curInfo.Rank < prevInfo.Rank && curInfo.Rank <= maxCurPage*60 && prevInfo.Rank-curInfo.Rank > minRankDelta {
			pairs = append(pairs, [2]*Info{
				prevInfo, curInfo,
			})
		}
	}
	pairs.Sort(func(a, b [2]*Info) bool {
		return (a[0].Rank - a[1].Rank) > (b[0].Rank - b[1].Rank)
	})

	for i, pair := range pairs {
		prevInfo, curInfo := pair[0], pair[1]
		delta := prevInfo.Rank - curInfo.Rank
		fmt.Printf("%-4d: %-37s %3d页%2d位 <- %3d页%2d位 升 %3d页%2d位 %s\n",
			i+1,
			fmt.Sprintf("http://item.jd.com/%d.html", curInfo.Sku),
			curInfo.Rank/60+1, curInfo.Rank%60+1,
			prevInfo.Rank/60+1, prevInfo.Rank%60+1,
			delta/60+1, delta%60+1,
			curInfo.ShopName)
	}

	fmt.Print("\n")
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

type InfoPair [][2]*Info

func (s InfoPair) Reduce(initial interface{}, fn func(value interface{}, elem [2]*Info) interface{}) (ret interface{}) {
	ret = initial
	for _, elem := range s {
		ret = fn(ret, elem)
	}
	return
}

func (s InfoPair) Map(fn func([2]*Info) [2]*Info) (ret InfoPair) {
	for _, elem := range s {
		ret = append(ret, fn(elem))
	}
	return
}

func (s InfoPair) Filter(filter func([2]*Info) bool) (ret InfoPair) {
	for _, elem := range s {
		if filter(elem) {
			ret = append(ret, elem)
		}
	}
	return
}

func (s InfoPair) All(predict func([2]*Info) bool) (ret bool) {
	ret = true
	for _, elem := range s {
		ret = predict(elem) && ret
	}
	return
}

func (s InfoPair) Any(predict func([2]*Info) bool) (ret bool) {
	for _, elem := range s {
		ret = predict(elem) || ret
	}
	return
}

func (s InfoPair) Each(fn func(e [2]*Info)) {
	for _, elem := range s {
		fn(elem)
	}
}

func (s InfoPair) Shuffle() {
	for i := len(s) - 1; i >= 1; i-- {
		j := rand.Intn(i + 1)
		s[i], s[j] = s[j], s[i]
	}
}

func (s InfoPair) Sort(cmp func(a, b [2]*Info) bool) {
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

func (s InfoPair) Clone() (ret InfoPair) {
	ret = make([][2]*Info, len(s))
	copy(ret, s)
	return
}
