package main

import (
	"crypto/sha512"
	"io"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"
	"regexp"
	"sort"

	"strings"
	"sync"

	"github.com/fatih/color"
	"github.com/jmoiron/sqlx"
	"github.com/reusee/proxy"
)

import _ "github.com/lib/pq"

import "fmt"

import "math/rand"

var vvicDb, db *sqlx.DB

var clientSet = proxy.NewClientSet()

func init() {
	var err error
	db, err = sqlx.Connect("postgres", "user=reus dbname=jd sslmode=disable")
	ce(err, "connect to db")
	vvicDb, err = sqlx.Connect("postgres", "user=reus dbname=vvic sslmode=disable")
	ce(err, "connect to vvic db")

	go http.ListenAndServe(":27777", nil)
}

type Info struct {
	Sku      int64
	Rank     int
	ShopId   int    `db:"shop_id"`
	ShopName string `db:"shop_name"`
}

type Category struct {
	Name string
	Id   int
}

var categories = []Category{
	{"休闲裤", 9717},
	{"衬衫", 1354},
	{"T恤", 1355},
	{"针织衫", 1356},
	{"雪纺衫", 9713},
	{"牛仔裤", 9715},
	{"连衣裙", 9719},
	{"半身裙", 9720},
	{"短裤", 11991},
	{"吊带，背心", 11988},
}

const location = "广东  广州市"

const maxSales = 50

const maxCurPage = 20

const minRankDelta = 10

const minPrice = 80

func main() {
	cmd := os.Args[1]
	switch cmd {
	case "delta":
		prevDate := os.Args[2]
		curDate := os.Args[3]
		if len(os.Args) > 4 {
			statDeltaByKeywords(prevDate, curDate, os.Args[4:])
		} else {
			for _, category := range categories {
				statDeltaByCategory(prevDate, curDate, category)
			}
		}
	case "best":
		date := os.Args[2]
		if len(os.Args) > 3 {
			statBestByKeywords(date, os.Args[3:])
		} else {
			for _, category := range categories {
				statBestByCategory(date, category)
			}
		}
	default:
		panic("no such command")
	}
}

func statBestByKeywords(date string, keywords []string) {
	keywordConditions := []string{}
	for _, keyword := range keywords {
		keywordConditions = append(keywordConditions, `AND title LIKE '%`+keyword+`%'`)
	}
	rows, err := db.Queryx(`SELECT a.sku FROM infos a
		LEFT JOIN items b
		ON a.sku = b.sku
		LEFT JOIN shops c
		ON b.shop_id = c.shop_id

		WHERE 
		date = $1
		AND location = $2
		`+strings.Join(keywordConditions, "")+`
		ORDER BY rank ASC
		LIMIT 1024
		`,
		date,
		location,
	)
	ce(err, "query")
	total := 0
	for rows.Next() {
		info := new(Info)
		ce(rows.StructScan(info), "scan")
		fmt.Printf("http://item.jd.com/%d.html\n", info.Sku)
		n, err := printVvicItem(info.Sku)
		ce(err, "print vvic item")
		if n > 0 {
			total++
		}
		if total > 50 {
			rows.Close()
			break
		}
	}
	ce(rows.Err(), "rows err")
}

func statBestByCategory(date string, category Category) {
	fmt.Printf("=== category %s %s location %s ===\n",
		category.Name,
		date,
		location,
	)

	rows, err := db.Queryx(`SELECT a.sku FROM infos a
		LEFT JOIN items b
		ON a.sku = b.sku
		LEFT JOIN shops c
		ON b.shop_id = c.shop_id

		WHERE a.category = $1
		AND date = $2
		AND location = $3

		ORDER BY rank ASC
		LIMIT 1024
			`,
		category.Id,
		date,
		location,
	)
	ce(err, "query")
	total := 0
	for rows.Next() {
		info := new(Info)
		ce(rows.StructScan(info), "scan")
		fmt.Printf("http://item.jd.com/%d.html\n", info.Sku)
		n, err := printVvicItem(info.Sku)
		ce(err, "print vvic item")
		if n > 0 {
			total++
		}
		if total > 50 {
			rows.Close()
			break
		}
	}
	ce(rows.Err(), "rows err")
}

func statDeltaByKeywords(prevDate, curDate string, keywords []string) {
	getInfosByKeywords := func(date string, keywords []string) map[int64]*Info {
		keywordConditions := []string{}
		for _, keyword := range keywords {
			keywordConditions = append(keywordConditions, `AND title LIKE '%`+keyword+`%'`)
		}
		rows, err := db.Queryx(`SELECT a.sku, rank, b.shop_id FROM infos a
			LEFT JOIN items b
			ON a.sku = b.sku
			LEFT JOIN shops c
			ON b.shop_id = c.shop_id

			WHERE 
			date = $1
			AND sales < $2
			AND location = $3

			AND (price >= $4 OR price <= 0) -- 可能有些价格采集不到

			`+strings.Join(keywordConditions, ""),
			date,
			maxSales,
			location,
			minPrice,
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

func statDeltaByCategory(prevDate, curDate string, category Category) {
	fmt.Printf("=== category %s %s to %s location %s max sales %d ===\n",
		category.Name,
		prevDate,
		curDate,
		location,
		maxSales,
	)

	getInfosByCategory := func(date string, categoryId int) map[int64]*Info {
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
			categoryId,
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

	prevInfos := getInfosByCategory(prevDate, category.Id)
	if len(prevInfos) == 0 {
		panic(me(nil, "invalid prev date %s", prevDate))
	}
	curInfos := getInfosByCategory(curDate, category.Id)
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
			delta/60, delta%60,
			curInfo.ShopName)

		_, err := printVvicItem(curInfo.Sku)
		ce(err, "print vvic item")
	}

	fmt.Print("\n")
}

func printVvicItem(sku int64) (n int, err error) {
	defer ct(&err)
	// find vvic items
	var hashes [][]byte
	err = db.Select(&hashes, `SELECT sha512_16k FROM images
			WHERE sku = $1`,
		sku)
	ce(err, "select hashes")
	if len(hashes) < 2 {
		err := collectImages(sku)
		ce(err, "collect images")
		err = db.Select(&hashes, `SELECT sha512_16k FROM images
			WHERE sku = $1`,
			sku)
		ce(err, "select hashes")
	}

	stat := make(map[int]int)
	for _, hash := range hashes {
		var vvicIds []int
		err = vvicDb.Select(&vvicIds, `SELECT a.good_id FROM goods a
				LEFT JOIN images b
				ON a.good_id = b.good_id
				LEFT JOIN urls c
				ON b.url_id = c.url_id
				WHERE sha512_16k = $1
				AND status > 0
				`,
			hash,
		)
		ce(err, "select vvic ids")
		for _, id := range vvicIds {
			stat[id]++
		}
	}
	ids := Ints([]int{})
	for id := range stat {
		ids = append(ids, id)
	}
	ids.Sort(func(a, b int) bool {
		return stat[a] > stat[b]
	})
	for i := 0; i < 8 && i < len(ids); i++ {
		color.Green("http://www.vvic.com/item.html?id=%d\n", ids[i])
		n++
	}
	return
}

var descUrlPattern = regexp.MustCompile(`desc: '([^']+)'`)

var imageUrlPattern = regexp.MustCompile(`//img[0-9]*\.360buyimg\.com[^\\)]+`)

func collectImages(sku int64) (err error) {
	defer ct(&err)
	// collect images and hash
	itemPageUrl := fmt.Sprintf("http://item.jd.com/%d.html", sku)
	var content []byte
	getContent(&content, itemPageUrl)
	descUrlMatches := descUrlPattern.FindSubmatch(content)
	if len(descUrlMatches) == 0 {
		ce(nil, "desc url not found %d", sku)
	}
	descUrl := "http:" + string(descUrlMatches[1])
	color.Cyan("collect images %s\n", descUrl)
	var desc []byte
	getContent(&desc, descUrl)
	images := imageUrlPattern.FindAll(desc, -1)
	tx := db.MustBegin()
	wg := new(sync.WaitGroup)
	wg.Add(len(images))
	sem := make(chan bool, 8)
	for _, imageUrlBs := range images {
		imageUrl := "http:" + string(imageUrlBs)
		fmt.Printf("%s\n", imageUrl)
		sem <- true
		go func() {
			defer func() {
				<-sem
				wg.Done()
			}()
			var sum []byte
			tooSmall := false
			clientSet.Do(func(client *http.Client) proxy.ClientState {
				resp, err := client.Get(imageUrl)
				if err != nil {
					return proxy.Bad
				}
				defer resp.Body.Close()
				h := sha512.New()
				n, err := io.CopyN(h, resp.Body, 16384)
				if err == io.EOF {
					if n < 16384 {
						tooSmall = true
						return proxy.Good
					}
					err = nil
				}
				if err != nil {
					return proxy.Bad
				}
				sum = h.Sum(nil)
				return proxy.Good
			})
			if tooSmall {
				return
			}
			_, err = tx.Exec(`INSERT INTO images (sku, url, sha512_16k) VALUES ($1, $2, $3)
					ON CONFLICT (sku, url) DO NOTHING`,
				sku,
				imageUrl,
				sum,
			)
			ce(err, "insert image")
		}()
	}
	wg.Wait()
	ce(tx.Commit(), "commit")
	return
}

func getContent(ret *[]byte, url string) {
	clientSet.Do(func(client *http.Client) proxy.ClientState {
		resp, err := client.Get(url)
		if err != nil {
			return proxy.Bad
		}
		defer resp.Body.Close()
		content, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return proxy.Bad
		}
		*ret = content
		return proxy.Good
	})
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

type Ints []int

func (s Ints) Reduce(initial interface{}, fn func(value interface{}, elem int) interface{}) (ret interface{}) {
	ret = initial
	for _, elem := range s {
		ret = fn(ret, elem)
	}
	return
}

func (s Ints) Map(fn func(int) int) (ret Ints) {
	for _, elem := range s {
		ret = append(ret, fn(elem))
	}
	return
}

func (s Ints) Filter(filter func(int) bool) (ret Ints) {
	for _, elem := range s {
		if filter(elem) {
			ret = append(ret, elem)
		}
	}
	return
}

func (s Ints) All(predict func(int) bool) (ret bool) {
	ret = true
	for _, elem := range s {
		ret = predict(elem) && ret
	}
	return
}

func (s Ints) Any(predict func(int) bool) (ret bool) {
	for _, elem := range s {
		ret = predict(elem) || ret
	}
	return
}

func (s Ints) Each(fn func(e int)) {
	for _, elem := range s {
		fn(elem)
	}
}

func (s Ints) Shuffle() {
	for i := len(s) - 1; i >= 1; i-- {
		j := rand.Intn(i + 1)
		s[i], s[j] = s[j], s[i]
	}
}

func (s Ints) Sort(cmp func(a, b int) bool) {
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

func (s Ints) Clone() (ret Ints) {
	ret = make([]int, len(s))
	copy(ret, s)
	return
}
