package engine

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/mongo"
	"io/ioutil"
	"sync-kline/config"
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
	Time   int64
	Amount float64
	Price  float64
}

type ConCurrentEngine struct {
	worker Worker
	Db     *mongo.Database
	config *config.EngineConfig
}

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
		fmt.Println("推送", tradeDetailCh)
	}

}

func (c *ConCurrentEngine) saveHistory(symbol string, period string) {

	kLines, err := c.worker.HistoryKline(symbol, period)
	if err != nil {
		return
	}

	fmt.Println(len(kLines))
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
			fmt.Printf("正在获取%s交易对：%s -- %s 的历史记录", c.config.Platform, symbol, period)
			findOne := db.Collection(getCollectionName(symbol, period)).FindOne(context.TODO(), nil)
			if findOne == nil {
				fmt.Println("请求")
				c.saveHistory(symbol, period)
			}
		}
	}

	return c, nil
}

func getCollectionName(symbol string, period string) string {
	return symbol + "_" + period
}

func GZIPDe(in []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(in))
	if err != nil {
		var out []byte
		return out, err
	}
	defer reader.Close()
	return ioutil.ReadAll(reader)
}
