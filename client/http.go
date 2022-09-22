package client

import (
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"
)

type Client struct {
	client *http.Client
	url    string
}

// NewClient 创建
func NewClient(url string, proxy func(r *http.Request) (*url.URL, error)) *Client {
	return &Client{
		client: &http.Client{
			Timeout: time.Millisecond * time.Duration(10*1000),
			Transport: &http.Transport{
				Proxy: proxy,
			},
		},
		url: url,
	}
}

// Get 请求
func (c *Client) Get(path string, params url.Values, res interface{}) error {

	return c.request(path, http.MethodGet, nil, params, nil, res)

}

// Post 请求
func (c *Client) Post(path string, data map[string]interface{}, res interface{}) error {
	return c.request(path, http.MethodPost, nil, nil, data, res)
}

func (c *Client) request(path string, method string, header http.Header, params url.Values, data map[string]interface{}, res interface{}) error {

	urlStr := c.url + path

	var reqBody io.Reader
	if data != nil {
		bytesData, err := json.Marshal(data)
		if err != nil {
			return err
		}
		reqBody = bytes.NewReader(bytesData)
	}

	if params != nil {
		Url, err := url.Parse(urlStr)
		if err != nil {
			return err
		}
		//如果参数中有中文参数,这个方法会进行URLEncode
		Url.RawQuery = params.Encode()
		urlStr = Url.String()
	}

	req, err := http.NewRequest(method, urlStr, reqBody)
	if header != nil {
		req.Header = header
	}
	if err != nil {
		// handle error
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.client.Do(req)
	if err != nil {
		// handle error
		return err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		// handle error
		return err
	}

	err = json.Unmarshal(body, res)
	if err != nil {
		return err
	}

	return nil
}
