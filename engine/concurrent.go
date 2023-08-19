package engine

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"github.com/shopspring/decimal"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"io"
	"math"
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
	Time   int64
	Open   float64
	Close  float64
	Low    float64
	High   float64
	Amount float64
	Vol    float64
	Count  int
}

type TradeDetailCh struct {
	Symbol string
	Time   int64
	Amount float64
	Price  float64
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
			c.createKLine(tradeDetailCh.Time, tradeDetailCh.Symbol, period, tradeDetailCh.Price, tradeDetailCh.Amount)
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
	_, err = c.Db.Collection(getCollectionName(symbol, period)).InsertMany(context.TODO(), data)
	if err != nil {
		fmt.Println(err)
		return
	}
}

func (c *ConCurrentEngine) createKLine(ts int64, symbol string, period string, price float64, amount float64) {

	currentTime, _ := createDateTime(ts, periodMap[period], 0, 1)

	filter := bson.M{"time": currentTime}
	isAdd := false
	findOne := c.Db.Collection(getCollectionName(symbol, period)).FindOne(context.TODO(), filter)
	var kLine KLine
	if findOne.Err() != nil {
		isAdd = true
		kLine = KLine{
			Time:   0,
			Open:   0,
			Close:  0,
			Low:    0,
			High:   0,
			Amount: 0,
			Vol:    0,
			Count:  0,
		}
	} else {
		err := findOne.Decode(&kLine)
		if err != nil {
			return
		}
	}

	kLine.Time = currentTime
	if kLine.Open <= 0 {
		kLine.Open = price
	}
	kLine.Close = price
	if kLine.Low <= 0 {
		kLine.Low = price
	} else {
		kLine.Low = math.Min(kLine.Low, price)
	}
	if kLine.High <= 0 {
		kLine.High = price
	} else {
		kLine.High = math.Max(kLine.High, price)
	}

	amountD := decimal.NewFromFloat(amount)
	priceD := decimal.NewFromFloat(price)

	kLine.Amount = decimal.NewFromFloat(kLine.Amount).Add(amountD).InexactFloat64()
	kLine.Vol = decimal.NewFromFloat(kLine.Vol).Add(amountD.Mul(priceD)).InexactFloat64()
	kLine.Count += 1

	if isAdd {
		_, err := c.Db.Collection(getCollectionName(symbol, period)).InsertOne(context.TODO(), kLine)
		if err != nil {
			return
		}
	} else {
		update := bson.M{"$set": kLine}
		_, err := c.Db.Collection(getCollectionName(symbol, period)).UpdateOne(context.TODO(), filter, update)
		if err != nil {
			return
		}
	}

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
			findOne := db.Collection(getCollectionName(symbol, period)).FindOne(context.TODO(), nil)
			if findOne.Err() != nil {
				fmt.Println("请求", findOne.Err())
				c.saveHistory(symbol, period)
			}
		}
	}

	return c, nil
}

func getCollectionName(symbol string, period string) string {
	//fmt.Println("名称", period, periodMap[period])
	return strings.ToLower(symbol) + "_" + periodMap[period]
}

func createDateTime(ts int64, period string, currentTime int64, limit int) (int64, int64) {

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

func GZIPDe(in []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(in))
	if err != nil {
		var out []byte
		return out, err
	}
	defer reader.Close()
	return io.ReadAll(reader)
}
