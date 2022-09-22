package server

type ListPageReq struct {
	Page  int `form:"page" binding:"required,gte=1"`          // 页数
	Limit int `form:"limit" binding:"required,gte=1,lte=200"` // 每页返回多少
}

type CreateWalletReq struct {
	Appid       string `json:"appid" binding:"required"`        // 应用ID
	Sign        string `json:"sign" binding:"required"`         // 签名
	NetworkName string `json:"network_name" binding:"required"` // 网络名称
	CoinSymbol  string `json:"coin_symbol" binding:"required"`  // 币种符号
	MemberId    string `json:"member_id" binding:"required"`    // 用户ID
	CallUrl     string `json:"call_url" binding:"required"`     // 回调url
}
