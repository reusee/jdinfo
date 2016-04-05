package main

import "github.com/PuerkitoBio/goquery"
import "fmt"
import "net/http"
import "time"
import "github.com/jmoiron/sqlx"
import "sync"
import "sync/atomic"

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
	db.MustExec(`DELETE FROM ranks WHERE date = ?`, date)
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
				collectCategoryPage(category, page)
			}()
		}
	}
	wg.Wait()
}

const itemsPerPage = 60

var pageCount int64

func collectCategoryPage(category int, page int) {
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
			sku, ok := se.Attr("data-sku")
			if !ok {
				panic(me(nil, "no sku %s", pageUrl))
			}
			shopId, ok := se.Attr("jdzy_shop_id")
			if !ok {
				panic(me(nil, "no shop id %s", pageUrl))
			}
			if shopId == "0" { // 自营的
				return
			}
			_, err := tx.Exec(`INSERT INTO shops (shop_id) VALUES (?)
				ON DUPLICATE KEY UPDATE shop_id=shop_id`,
				shopId,
			)
			ce(err, "insert shop")
			_, err = tx.Exec(`INSERT INTO items (sku, category, shop_id) VALUES (?, ?, ?)
				ON DUPLICATE KEY UPDATE sku=sku, category = ?, shop_id = ?`,
				sku, category, shopId, category, shopId,
			)
			ce(err, "insert item")
			_, err = tx.Exec(`INSERT INTO ranks (sku, date, rank) VALUES (?, ?, ?)
				ON DUPLICATE KEY UPDATE rank=rank`,
				sku,
				date,
				itemsPerPage*(page-1)+i)
			ce(err, "insert rank")
		})
		return
	}), "with tx")
}
