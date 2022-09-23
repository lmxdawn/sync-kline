package server

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"sync-kline/config"
	"sync-kline/engine"
	"sync-kline/mongo"
)

// Start 启动服务
func Start(isSwag bool, configPath string) {

	conf, err := config.NewConfig(configPath)

	if err != nil {
		panic("Failed to load configuration")
	}

	db, err := mongo.NewTrade(conf.Mongo.Uri)
	if err != nil {
		panic(err)
	}

	var engines []*engine.ConCurrentEngine
	for _, engineConfig := range conf.Engines {
		eng, err := engine.NewEngine(db, &engineConfig)
		if err != nil {
			panic(fmt.Sprintf("eth run err：%v", err))
		}
		engines = append(engines, eng)
	}

	// 启动监听器
	for _, currentEngine := range engines {
		go currentEngine.Start()
	}

	if isSwag {
		gin.SetMode(gin.DebugMode)
	} else {
		gin.SetMode(gin.ReleaseMode)
	}
	server := gin.Default()

	// 中间件
	server.Use(gin.Logger())
	server.Use(gin.Recovery())
	server.Use(Cors())
	server.Use(SetDB(db))

	server.POST("/kline", KLine)

	fmt.Println("start success")

	err = server.Run(fmt.Sprintf(":%v", conf.App.Port))
	if err != nil {
		panic("start error")
	}

}
