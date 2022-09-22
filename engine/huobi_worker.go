package engine

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/websocket"
	"github.com/mitchellh/mapstructure"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync-kline/client"
	"time"
)

type HuoBiWorker struct {
	conn       *websocket.Conn
	httpClient *client.Client
	kLineCh    chan *KLineCh
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

func (w *HuoBiWorker) Close() error {
	return w.conn.Close()
}

func (w *HuoBiWorker) ReadMessage() {
	for {
		_, message, err := w.conn.ReadMessage()
		if err != nil {
			log.Println("read:", err)
			return
		}

		bytes, err := GZIPDe(message)
		if err != nil {
			return
		}
		log.Printf("recv: %s", string(bytes))

		var res HuoBiWsMessageRes
		err = json.Unmarshal(bytes, &res)
		if err != nil {
			return
		}

		if res.Ping > 0 {
			req := make(map[string]interface{})
			req["pong"] = time.Now().UnixMilli()
			marshal, err := json.Marshal(req)
			if err != nil {
				return
			}
			w.WriteMessage(marshal)
			return
		}

		if strings.Contains(res.Ch, "kline") {
			w.formatKline(&res)
		}

	}
}

func (w *HuoBiWorker) formatKline(res *HuoBiWsMessageRes) {

	ch := strings.Split(res.Ch, ".")

	var tick HuoBiKlineRes
	if err := mapstructure.Decode(res.Tick, &tick); err != nil {
		fmt.Println("解析失败", err)
		return
	}
	w.kLineCh <- &KLineCh{
		Symbol: ch[1],
		Period: ch[3],
		KLine: KLine{
			Time:   tick.Id,
			Open:   tick.Open,
			Close:  tick.Close,
			Low:    tick.Low,
			High:   tick.High,
			Amount: tick.Amount,
			Vol:    tick.Vol,
			Count:  tick.Count,
		},
	}
}

func (w *HuoBiWorker) WriteMessage(msg []byte) {

	err := w.conn.WriteMessage(websocket.TextMessage, msg)
	if err != nil {
		log.Println("write:", err)
		return
	}

}

func (w *HuoBiWorker) SubscribeKline(symbol string, period string) {

	req := make(map[string]interface{})
	req["sub"] = fmt.Sprintf("market.%s.kline.%s", symbol, period)
	req["id"] = strconv.FormatInt(time.Now().Unix(), 10)

	marshal, err := json.Marshal(req)
	if err != nil {
		return
	}
	fmt.Println(string(marshal))
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
			Open:   item.Open,
			Close:  item.Close,
			Low:    item.Low,
			High:   item.High,
			Amount: item.Amount,
			Vol:    item.Vol,
			Count:  item.Count,
		})
	}

	return klines, nil
}

func (w *HuoBiWorker) ReadKlineCh() *KLineCh {
	return <-w.kLineCh
}

func NewHuoBiWorker(proxyUrl string, wsUrl string, httpUrl string) (*HuoBiWorker, error) {

	var proxy func(r *http.Request) (*url.URL, error)
	if len(proxyUrl) > 0 {
		uProxy, _ := url.Parse(proxyUrl)
		proxy = http.ProxyURL(uProxy)
	}

	dialer := websocket.Dialer{Proxy: proxy}
	conn, _, err := dialer.Dial(wsUrl, nil)
	if err != nil {
		fmt.Println("dial:", err)
		return nil, err
	}

	httpClient := client.NewClient(httpUrl, proxy)

	return &HuoBiWorker{
		conn:       conn,
		httpClient: httpClient,
		kLineCh:    make(chan *KLineCh),
	}, nil
}
