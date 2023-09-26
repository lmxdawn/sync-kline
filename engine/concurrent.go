package engine

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"github.com/shopspring/decimal"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"io"
	"strings"
	"sync-kline/config"
	"time"
)

type Worker interface {
	Close() error
	Start()
	WriteMessage(msg []byte)
	SubscribeTradeDetail(symbol string)
	HistoryKline(symbol string, period string) ([]*KLine, error)
	ReadTradeDetailCh() *TradeDetailCh
}

type KLine struct {
	Time   int64  `json:"time"`   // 时间
	Open   string `json:"open"`   // 开盘
	Close  string `json:"close"`  // 收盘
	Low    string `json:"low"`    // 最低
	High   string `json:"high"`   // 最高
	Amount string `json:"amount"` // 数量
	Vol    string `json:"vol"`    // 成交额
	Count  int    `json:"count"`  // 成交数量
}

type TradeDetailCh struct {
	Symbol string
	Time   int64
	Amount decimal.Decimal
	Price  decimal.Decimal
}

type ConCurrentEngine struct {
	worker Worker
	Db     *mongo.Database
	config *config.EngineConfig
}

var (
	periodMap = map[string]string{
		"1min":  "1min",
		"5min":  "5min",
		"15min": "15min",
		"30min": "30min",
		"60min": "1hour",
		"1hour": "1hour",
		"4hour": "4hour",
		"1day":  "1day",
		"1mon":  "1mon",
		"1week": "1week",
		"1year": "1year",
		"1m":    "1min",
		"5m":    "5min",
		"15m":   "15min",
		"30m":   "30min",
		"1h":    "1hour",
		"4h":    "4hour",
		"1d":    "1day",
		"1M":    "1mon",
		"1w":    "1week",
	}
	timeMap = map[string]int64{
		"1min":  60,
		"5min":  5 * 60,
		"15min": 15 * 60,
		"30min": 30 * 60,
		"1hour": 60 * 60,
		"4hour": 4 * 60 * 60,
		"1day":  24 * 60 * 60,
		"1week": 7 * 24 * 60 * 60,
		"1mon":  0,
		"1year": 0,
	}
	timeSubMap = map[string]int64{
		"1day":  -8 * 60 * 60,         // 一天的零点需要减去8小时，因为时间戳从8点开始算的
		"1week": 4*24*60*60 - 8*60*60, // 每周的周一需要加上 4天，去掉8小时，因为时间戳是从周四开始的
	}
)

var decimal0 = decimal.NewFromInt(0)

// Start 启动
func (c *ConCurrentEngine) Start() {

	defer c.worker.Close()

	go c.loop()

	select {}
}

// loop 循环监听
func (c *ConCurrentEngine) loop() {

	go c.worker.Start()

	// 循环读取
	for {
		tradeDetailCh := c.worker.ReadTradeDetailCh()

		for period := range timeMap {
			c.KLineCreate("", tradeDetailCh.Symbol, tradeDetailCh.Time, period, tradeDetailCh.Price, tradeDetailCh.Amount)
		}

		//fmt.Println("推送", tradeDetailCh)
	}

}

func (c *ConCurrentEngine) saveHistory(symbol string, period string) {

	kLines, err := c.worker.HistoryKline(symbol, period)
	if err != nil {
		return
	}

	data := make([]interface{}, len(kLines))
	for i, kLine := range kLines {
		data[i] = kLine
	}
	_, err = c.Db.Collection(klineGetCollectionName(symbol, period)).InsertMany(context.TODO(), data)
	if err != nil {
		fmt.Println(err)
		return
	}
}

func (c *ConCurrentEngine) KLineDatabase(name string) *mongo.Database {

	return c.Db
}

func (c *ConCurrentEngine) KLineCreateAll(name string, pair string, ts int64, price decimal.Decimal, amount decimal.Decimal) {

	for period := range timeMap {
		c.KLineCreate(name, pair, ts, period, price, amount)
	}

}

func (c *ConCurrentEngine) KLineCreate(name string, pair string, ts int64, period string, price decimal.Decimal, amount decimal.Decimal) {

	currentTime, _ := klineCreateDateTime(ts, periodMap[period], 0, 1)

	filter := bson.M{"time": currentTime}
	isAdd := false
	findOne := c.KLineDatabase(name).Collection(klineGetCollectionName(pair, period)).FindOne(context.TODO(), filter)
	var kLine KLine
	if findOne.Err() != nil {
		isAdd = true
		kLine = KLine{
			Time:   0,
			Open:   decimal0.String(),
			Close:  decimal0.String(),
			Low:    decimal0.String(),
			High:   decimal0.String(),
			Amount: decimal0.String(),
			Vol:    decimal0.String(),
			Count:  0,
		}
	} else {
		err := findOne.Decode(&kLine)
		if err != nil {
			return
		}
	}

	kLine.Time = currentTime
	open, _ := decimal.NewFromString(kLine.Open)
	if open.Cmp(decimal0) <= 0 {
		kLine.Open = price.String()
	}
	kLine.Close = price.String()
	low, _ := decimal.NewFromString(kLine.Low)
	if low.Cmp(decimal0) <= 0 {
		kLine.Low = price.String()
	} else {
		kLine.Low = decimal.Min(low, price).String()
	}
	high, _ := decimal.NewFromString(kLine.High)
	if high.Cmp(decimal0) <= 0 {
		kLine.High = price.String()
	} else {
		kLine.High = decimal.Max(high, price).String()
	}

	amountOld, _ := decimal.NewFromString(kLine.Amount)
	volOld, _ := decimal.NewFromString(kLine.Vol)
	kLine.Amount = amountOld.Add(amount).String()
	kLine.Vol = volOld.Add(amount.Mul(price)).String()
	kLine.Count += 1

	if isAdd {
		_, err := c.KLineDatabase(name).Collection(klineGetCollectionName(pair, period)).InsertOne(context.TODO(), kLine)
		if err != nil {
			fmt.Println(err)
			return
		}
	} else {
		update := bson.M{"$set": kLine}
		_, err := c.KLineDatabase(name).Collection(klineGetCollectionName(pair, period)).UpdateOne(context.TODO(), filter, update)
		if err != nil {
			fmt.Println(err)
			return
		}
	}

}

func (c *ConCurrentEngine) KlinePeriod() []string {

	var periods []string

	for period := range timeMap {
		periods = append(periods, period)
	}

	return periods
}

func (c *ConCurrentEngine) KlineHistory(name string, pair string, period string, lastTime int64) ([]*KLine, error) {

	var kLines []*KLine

	filter := bson.M{}

	if lastTime > 0 {
		filter = bson.M{"time": bson.M{"$lt": lastTime}}
	}

	// 定义分页参数
	page := 1       // 当前页数
	pageSize := 200 // 每页条数
	skip := (page - 1) * pageSize

	findOptions := options.Find()
	findOptions.SetSkip(int64(skip))
	findOptions.SetLimit(int64(pageSize))
	findOptions.SetSort(bson.M{"time": -1}) // 时间降序

	cur, err := c.KLineDatabase(name).Collection(klineGetCollectionName(pair, period)).Find(context.Background(), filter, findOptions)
	if err != nil {
		return nil, err
	}
	defer cur.Close(context.Background())
	err = cur.All(context.Background(), &kLines)
	if err != nil {
		return nil, err
	}

	return kLines, nil
}

func klineGetCollectionName(pair string, period string) string {
	//fmt.Println("名称", period, periodMap[period])
	return strings.ToLower(pair) + "_" + periodMap[period]
}

func klineCreateDateTime(ts int64, period string, currentTime int64, limit int) (int64, int64) {

	prevTime := int64(0)
	timeValue, ok := timeMap[period]
	if !ok {
		return currentTime, prevTime
	}

	// 月份的处理方式不一样
	if "1mon" == period {
		d := time.Unix(ts, 0)
		d = time.Date(d.Year(), d.Month(), 1, 0, 0, 0, 0, d.Location())
		currentTime = d.Unix()
		// 设置上一个时间
		d.AddDate(0, -limit, 0)
		prevTime = d.Unix()

	} else if "1year" == period {
		d := time.Unix(ts, 0)
		d = time.Date(d.Year(), 1, 1, 0, 0, 0, 0, d.Location())
		currentTime = d.Unix()
		// 设置上一个时间
		d.AddDate(-limit, 0, 0)
		prevTime = d.Unix()

	} else {

		if currentTime <= 0 {
			currentTime = ts
			if timeValue != 0 {
				currentTime -= ts % timeValue
			}
		}
		if timeSubValue, ok := timeSubMap[period]; ok {
			currentTime = currentTime + timeSubValue
		}
		prevTime = currentTime - (int64(limit) * timeValue)
	}

	return currentTime, prevTime
}

// NewEngine 创建ETH
func NewEngine(db *mongo.Database, config *config.EngineConfig) (*ConCurrentEngine, error) {

	var worker Worker
	var err error
	switch config.Platform {
	case "huobi":
		worker, err = NewHuoBiWorker(config)
		if err != nil {
			return nil, err
		}
	}

	c := &ConCurrentEngine{
		worker: worker,
		Db:     db,
		config: config,
	}

	// 获取历史数据
	for _, symbol := range c.config.Symbols {
		for _, period := range c.config.Periods {
			fmt.Printf("正在获取%s交易对：%s -- %s 的历史记录\n", c.config.Platform, symbol, period)
			findOne := db.Collection(klineGetCollectionName(symbol, period)).FindOne(context.TODO(), nil)
			if findOne.Err() != nil {
				fmt.Println("请求", findOne.Err())
				c.saveHistory(symbol, period)
			}
		}
	}

	return c, nil
}

func GZIPDe(in []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(in))
	if err != nil {
		var out []byte
		return out, err
	}
	defer reader.Close()
	return io.ReadAll(reader)
}
