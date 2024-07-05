package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

func HealthCheck(c *gin.Context) {
	res := map[string]interface{}{
		"data": "chameleon generator server is up and running",
	}
	c.JSON(http.StatusOK, res)
}
