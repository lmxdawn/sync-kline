package engine

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/websocket"
	"github.com/mitchellh/mapstructure"
	"github.com/shopspring/decimal"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync-kline/client"
	"sync-kline/config"
	"time"
)

type HuoBiWorker struct {
	conn          *websocket.Conn
	reconnection  int
	httpClient    *client.Client
	symbols       []string
	tradeDetailCh chan *TradeDetailCh
}

type HuoBiWsMessageRes struct {
	Ping int64                  `json:"ping"`
	Ch   string                 `json:"ch"`
	Ts   int64                  `json:"ts"`
	Tick map[string]interface{} `json:"tick"`
}

type HuoBiHttpRes struct {
	Ch     string      `json:"ch"`
	Ts     int64       `json:"ts"`
	Status string      `json:"status"`
	Data   interface{} `json:"data"`
}

type HuoBiKlineRes struct {
	Id     int64   `mapstructure:"id"`
	Open   float64 `mapstructure:"open"`
	Close  float64 `mapstructure:"close"`
	Low    float64 `mapstructure:"low"`
	High   float64 `mapstructure:"high"`
	Amount float64 `mapstructure:"amount"`
	Vol    float64 `mapstructure:"vol"`
	Count  int     `mapstructure:"count"`
}

type HuoBiTradeDetailRes struct {
	Id   int64 `mapstructure:"id"`
	Ts   int64 `mapstructure:"ts"`
	Data []struct {
		Id        float64 `mapstructure:"id"`
		Ts        int64   `mapstructure:"ts"`
		TradeId   int64   `mapstructure:"tradeId"`
		Amount    float64 `mapstructure:"amount"`
		Price     float64 `mapstructure:"price"`
		Direction string  `mapstructure:"direction"`
	} `mapstructure:"data"`
}

func (w *HuoBiWorker) Close() error {
	return w.conn.Close()
}

func (w *HuoBiWorker) Start() {

	go w.readMessage()

	for _, symbol := range w.symbols {
		go w.SubscribeTradeDetail(symbol)
	}

}

func (w *HuoBiWorker) readMessage() {
	for {
		_, message, err := w.conn.ReadMessage()
		if err != nil {
			log.Println("read:", err)
			if w.reconnection >= 10 {
				return
			}
			fmt.Println("正在尝试重连")
			w.Start()
			return
		}

		bytes, err := GZIPDe(message)
		if err != nil {
			continue
		}
		//log.Printf("recv: %s", string(bytes))

		var res HuoBiWsMessageRes
		err = json.Unmarshal(bytes, &res)
		if err != nil {
			continue
		}

		if res.Ping > 0 {
			req := make(map[string]interface{})
			req["pong"] = time.Now().UnixMilli()
			marshal, err := json.Marshal(req)
			if err != nil {
				continue
			}
			w.WriteMessage(marshal)
			continue
		}

		if strings.Contains(res.Ch, "trade.detail") {
			w.formatTradeDetail(&res)
		}

	}
}

func (w *HuoBiWorker) formatTradeDetail(res *HuoBiWsMessageRes) {

	ch := strings.Split(res.Ch, ".")

	var tick HuoBiTradeDetailRes
	if err := mapstructure.Decode(res.Tick, &tick); err != nil {
		fmt.Println("解析失败", err)
		return
	}

	for _, item := range tick.Data {
		w.tradeDetailCh <- &TradeDetailCh{
			Symbol: ch[1],
			Time:   tick.Ts / 1000,
			Amount: decimal.NewFromFloat(item.Amount),
			Price:  decimal.NewFromFloat(item.Price),
		}
	}
}

func (w *HuoBiWorker) WriteMessage(msg []byte) {

	err := w.conn.WriteMessage(websocket.TextMessage, msg)
	if err != nil {
		log.Println("write:", err)
		return
	}

}

func (w *HuoBiWorker) SubscribeTradeDetail(symbol string) {

	req := make(map[string]interface{})
	req["sub"] = fmt.Sprintf("market.%s.trade.detail", symbol)
	req["id"] = strconv.FormatInt(time.Now().Unix(), 10)

	marshal, err := json.Marshal(req)
	if err != nil {
		return
	}
	w.WriteMessage(marshal)

}

func (w *HuoBiWorker) HistoryKline(symbol string, period string) ([]*KLine, error) {

	params := url.Values{}
	params["symbol"] = []string{symbol}
	params["period"] = []string{period}
	params["size"] = []string{"2000"}

	path := "/market/history/kline"

	var res HuoBiHttpRes

	err := w.httpClient.Get(path, params, &res)
	if err != nil {
		return nil, err
	}

	var data []*HuoBiKlineRes
	if err := mapstructure.Decode(res.Data, &data); err != nil {
		fmt.Println("解析失败", err)
		return nil, err
	}

	var klines []*KLine
	for _, item := range data {
		klines = append(klines, &KLine{
			Time:   item.Id,
			Open:   decimal.NewFromFloat(item.Open).String(),
			Close:  decimal.NewFromFloat(item.Close).String(),
			Low:    decimal.NewFromFloat(item.Low).String(),
			High:   decimal.NewFromFloat(item.High).String(),
			Amount: decimal.NewFromFloat(item.Amount).String(),
			Vol:    decimal.NewFromFloat(item.Vol).String(),
			Count:  item.Count,
		})
	}

	return klines, nil
}

func (w *HuoBiWorker) ReadTradeDetailCh() *TradeDetailCh {
	return <-w.tradeDetailCh
}

func NewHuoBiWorker(config *config.EngineConfig) (*HuoBiWorker, error) {

	var proxy func(r *http.Request) (*url.URL, error)
	if len(config.ProxyUrl) > 0 {
		uProxy, _ := url.Parse(config.ProxyUrl)
		proxy = http.ProxyURL(uProxy)
	}

	dialer := websocket.Dialer{Proxy: proxy}
	conn, _, err := dialer.Dial(config.WsUrl, nil)
	if err != nil {
		fmt.Println("dial:", err)
		return nil, err
	}

	httpClient := client.NewClient(config.HttpUrl, proxy)

	return &HuoBiWorker{
		conn:          conn,
		httpClient:    httpClient,
		symbols:       config.Symbols,
		tradeDetailCh: make(chan *TradeDetailCh),
	}, nil
}
