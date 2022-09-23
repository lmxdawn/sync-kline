package mongo

import (
	"context"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func NewClient(uri string) (*mongo.Client, error) {
	var err error
	clientOptions := options.Client().ApplyURI(uri)

	// 连接到MongoDB
	mgoCli, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		return nil, err
	}
	// 检查连接
	err = mgoCli.Ping(context.TODO(), nil)
	if err != nil {
		return nil, err
	}
	return mgoCli, nil
}

func NewTrade(uri string) (*mongo.Database, error) {
	client, err := NewClient(uri)
	if err != nil {
		return nil, err
	}

	db := client.Database("trade")

	return db, nil
}
