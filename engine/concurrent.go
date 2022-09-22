package engine

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"go.mongodb.org/mongo-driver/mongo"
	"io/ioutil"
	"sync-kline/config"
)

type Worker interface {
	Close() error
	ReadMessage()
	WriteMessage(msg []byte)
	SubscribeKline(symbol string, period string)
	HistoryKline(symbol string, period string) ([]*KLine, error)
	ReadKlineCh() *KLineCh
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

type KLineCh struct {
	Symbol string
	Period string
	KLine
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

	// 监听ws
	go c.worker.ReadMessage()

	// 订阅
	for _, symbol := range c.config.Symbols {
		for _, period := range c.config.Periods {
			go c.worker.SubscribeKline(symbol, period)
			go c.saveHistory(symbol, period)
		}
	}

	// 循环读取
	for {
		kLineCh := c.worker.ReadKlineCh()
		fmt.Println("推送", kLineCh)
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
		worker, err = NewHuoBiWorker(config.ProxyUrl, config.WsUrl, config.HttpUrl)
		if err != nil {
			return nil, err
		}
	}

	return &ConCurrentEngine{
		worker: worker,
		Db:     db,
		config: config,
	}, nil
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
