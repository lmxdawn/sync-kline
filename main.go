package main

import (
	"sync-kline/cmd"
)

// 是否加载文档
var isSwag bool

func main() {

	cmd.Run(isSwag)

}
