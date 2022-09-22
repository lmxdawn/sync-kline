package server

import (
	"github.com/gin-gonic/gin"
)

func KLine(c *gin.Context) {

	var q CreateWalletReq

	if err := c.ShouldBindJSON(&q); err != nil {
		HandleValidatorError(c, err)
		return
	}

	res := CreateWalletRes{}

	APIResponse(c, nil, res)
}
