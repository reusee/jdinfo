//TODO 记录评论数，避免太过劣势的单品
package main

import "github.com/PuerkitoBio/goquery"
import "fmt"
import "net/http"
import "time"
import "sync"
import "sync/atomic"
import "github.com/jmoiron/sqlx"
import "strconv"

var pt = fmt.Printf

var categories = []int{
	1354,  // 衬衫
	1355,  // T恤
	1356,  // 针织衫
	3983,  // 羽绒服
	9705,  // 棉服
	9706,  // 毛呢大衣
	9707,  // 真皮皮衣
	9708,  // 风衣
	9710,  // 卫衣
	9711,  // 小西装
	9712,  // 短外套
	9713,  // 雪纺衫
	9714,  // 马甲
	9715,  // 牛仔裤
	9716,  // 打底裤
	9717,  // 休闲裤
	9718,  // 正装裤
	9719,  // 连衣裙
	9720,  // 半身裙
	9721,  // 中老年女装
	9722,  // 大码女装
	9723,  // 婚纱
	11985, // 打底衫
	11986, // 旗袍，唐装
	11987, // 加绒裤
	11988, // 吊带，背心
	11989, // 羊绒衫
	11991, // 短裤
	11993, // 皮草
	11996, // 礼服
	11998, // 仿皮皮衣
	11999, // 羊毛衫
}

var date = time.Now().Format("20060102")

func main() {
	collectCategoryPages()
	collectShopLocations()
}

type Info struct {
	Sku      int64
	Category int
	Rank     int
	Sales    int
}

func collectCategoryPages() {
	infosChan := make(chan Info)
	infosMap := make(map[int64]Info)
	go func() {
		for info := range infosChan {
			infosMap[info.Sku] = info
		}
	}()

	maxPage := 300
	wg := new(sync.WaitGroup)
	wg.Add(len(categories) * maxPage)
	sem := make(chan bool, 8)
	for _, category := range categories {
		for page := 1; page <= maxPage; page++ {
			category := category
			page := page
			sem <- true
			go func() {
				defer func() {
					<-sem
					wg.Done()
				}()
				retry := 5
			collect:
				if err := collectCategoryPage(category, page, infosChan); err != nil {
					if retry > 0 {
						retry--
						time.Sleep(time.Second)
						goto collect
					}
					ce(err, "collect %v %v", category, page)
				}
			}()
		}
	}
	wg.Wait()
	pt("all pages collected\n")
	time.Sleep(time.Second)

	// delete old data
	db.MustExec(`DELETE FROM infos WHERE date = $1`, date)
	// update infos
	c := 0
	tx := db.MustBegin()
	for sku, info := range infosMap {
		_, err := tx.Exec(`INSERT INTO infos (sku, date, rank, sales, category) 
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (sku, date, category) DO UPDATE SET 
			rank = $3, sales = $4`,
			sku,
			date,
			info.Rank,
			info.Sales,
			info.Category)
		ce(err, "insert rank")
		c++
		if c%2048 == 0 {
			ce(tx.Commit(), "commit")
			tx = db.MustBegin()
		}
	}
	ce(tx.Commit(), "commit")
	pt("infos updated\n")
}

const itemsPerPage = 60

var pageCount int64

func collectCategoryPage(category int, page int, infosChan chan Info) (err error) {
	defer ct(&err)
	pt("%-10d %-10d %-10d\n", atomic.AddInt64(&pageCount, 1), category, page)
	pageUrl := fmt.Sprintf("http://list.jd.com/list.html?cat=1315,1343,%d&page=%d&sort=sort_totalsales15_desc",
		category, page)
	resp, err := http.Get(pageUrl)
	ce(err, "get page %s", pageUrl)
	doc, err := goquery.NewDocumentFromResponse(resp)
	ce(err, "doc from resp %s", pageUrl)
	itemSes := doc.Find("div#plist div.j-sku-item")
	ce(withTx(db, func(tx *sqlx.Tx) (err error) {
		defer ct(&err)
		itemSes.Each(func(i int, se *goquery.Selection) {
			skuStr, ok := se.Attr("data-sku")
			if !ok {
				panic(me(nil, "no sku %s", pageUrl))
			}
			sku, err := strconv.ParseInt(skuStr, 10, 64)
			ce(err, "parse sku")
			shopId, ok := se.Attr("jdzy_shop_id")
			if !ok {
				panic(me(nil, "no shop id %s", pageUrl))
			}
			if shopId == "0" { // 自营的
				return
			}
			title := se.Find("div.p-name em").Text()
			if len(title) == 0 {
				panic(me(nil, "no title %s", pageUrl))
			}
			salesStr := se.Find("div.p-commit a").Text()
			if len(salesStr) == 0 {
				panic(me(nil, "no sales %s", pageUrl))
			}
			sales, err := strconv.Atoi(salesStr)
			ce(err, "parse sales %s", salesStr)
			_, err = tx.Exec(`INSERT INTO shops (shop_id) VALUES ($1)
				ON CONFLICT (shop_id) DO NOTHING`,
				shopId,
			)
			ce(err, "insert shop")
			_, err = tx.Exec(`INSERT INTO items (sku, category, shop_id, title) VALUES ($1, $2, $3, $4)
				ON CONFLICT (sku) DO UPDATE SET category = $2, shop_id = $3`,
				sku, category, shopId, title,
			)
			ce(err, "insert item")
			infosChan <- Info{
				Sku:      sku,
				Rank:     itemsPerPage*(page-1) + i,
				Sales:    sales,
				Category: category,
			}
		})
		return
	}), "with tx")
	return
}

func collectShopLocations() {
	var ids []int
	err := db.Select(&ids, `SELECT shop_id FROM shops
		WHERE location IS NULL OR location = '' OR name IS NULL OR name = ''`)
	ce(err, "select shop ids without location")
	wg := new(sync.WaitGroup)
	wg.Add(len(ids))
	sem := make(chan bool, 4)
	for _, id := range ids {
		id := id
		sem <- true
		go func() {
			defer func() {
				<-sem
				wg.Done()
			}()
			shopPageUrl := fmt.Sprintf("http://mall.jd.com/shopLevel-%d.html", id)
			resp, err := http.Get(shopPageUrl)
			ce(err, "get shop page")
			doc, err := goquery.NewDocumentFromResponse(resp)
			ce(err, "get doc")
			var location, name string
			doc.Find("span.label").Each(func(_ int, se *goquery.Selection) {
				if se.Text() == "所在地：" {
					se = se.SiblingsFiltered("span.value")
					location = se.Text()
				} else if se.Text() == "公司名称：" {
					se = se.SiblingsFiltered("span.value")
					name = se.Text()
				}
			})
			_, err = db.Exec(`UPDATE shops SET location = $1, name = $2
					WHERE shop_id = $3`,
				location, name, id)
			ce(err, "update shops")
			pt("%15d %s %s\n", id, location, name)
		}()
	}
	wg.Wait()
}
