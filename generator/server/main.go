package server

import (
	"context"
	"fmt"

	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"

	"github.com/timeplus-io/chameleon/generator/handlers"
	"github.com/timeplus-io/chameleon/generator/log"

	_ "github.com/timeplus-io/chameleon/generator/docs"
)

// @title Chameleon Generator
// @version 1.0
// @description This is timeplus data generator api server.

// @contact.email gang@timeplus.io

// @BasePath /api/v1beta1

var Version = "development"
var Commit = ""
var BuildTime = ""

func InfoMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Set("Version", Version)
		c.Set("Commit", Commit)
		c.Set("BuildTime", BuildTime)
		c.Next()
	}
}

func CORSMiddleware() gin.HandlerFunc {
	allowedOrigin := viper.GetString("allow-origin")

	return func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", allowedOrigin)
		c.Header("Access-Control-Allow-Credentials", "true")
		c.Header("Access-Control-Allow-Headers", "Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization, accept, origin, Cache-Control, X-Requested-With")
		c.Header("Access-Control-Allow-Methods", "POST, HEAD,PATCH, DELETE, OPTIONS, GET, PUT")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}

		c.Next()
	}
}

func Run(_ *cobra.Command, _ []string) error {
	server := startServer()
	shutdown(server)
	return nil
}

func startServer() *http.Server {
	router := gin.New()

	router.Use(log.LoggerHandler(), gin.Recovery())
	router.Use(InfoMiddleware())
	router.Use(CORSMiddleware())

	// Routes
	router.GET("/health", handlers.HealthCheck)

	address := viper.GetString("server-addr")
	port := viper.GetInt("server-port")
	schema := "http"
	url := ginSwagger.URL(fmt.Sprintf("%s://%s:%d/swagger/doc.json", schema, address, port)) // The url pointing to API definition
	router.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler, url))

	srv := &http.Server{
		Addr:    fmt.Sprintf("%s:%d", address, port),
		Handler: router,
	}

	log.Logger().Infof("listen on %s %d ", address, port)
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.Logger().Infof("listen: %s\n", err)
		}
	}()

	return srv
}

func shutdown(server *http.Server) {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit
	log.Logger().Println("Shutdown Server ...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if server != nil {
		if err := server.Shutdown(ctx); err != nil {
			log.Logger().Fatal("Server Shutdown:", err)
		}
	}

	// TODO: other clean ups can be done here
	log.Logger().Println("Server exiting")
}
