package main

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"regexp"

	"github.com/go-redis/redis"
	"github.com/gorilla/mux"
	"github.com/jinzhu/gorm"

	"net/http"

	_ "github.com/jinzhu/gorm/dialects/postgres"
)

// urls -> URL database structure
type urls struct {
	gorm.Model
	Tinyurl string `gorm:"unique;not null"`
	Longurl string
}

// Response ->  response Object
type Response struct {
	Code int
	Msg  string
}

// PostgresClient -> Provides a connection to the postgres database server
func PostgresClient() *gorm.DB {
	dbClient, err := gorm.Open("postgres", "host=127.0.0.1 port=5432 user=rhythm  dbname=tiny_scale_go password=rhythm sslmode=disable")
	// dbClient, err := gorm.Open("postgres", "'':''@(localhost)/tiny_scale_go?charset=utf8&parseTime=True&loc=Local&sslmode=disable")
	if err != nil {
		fmt.Println(err)
	}
	return dbClient
}

// RedisClient -> Provides a connection to the Redis server
func RedisClient() *redis.Client {
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	return client
}

// StoreTinyURL -> puts the urls into cache and DB
func StoreTinyURL(dbURLData urls, longURL string, tinyURL string, dbClient *gorm.DB, redisClient *redis.Client) {
	dbClient.Create(&dbURLData)
	redisClient.HSet("urls", tinyURL, longURL)
}

// GenerateHashAndInsert -> Genarates a unique tiny URL and inserts it to DB
func GenerateHashAndInsert(longURL string, startIndex int, dbClient *gorm.DB, redisClient *redis.Client) string {
	byteURLData := []byte(longURL)
	hashedURLData := fmt.Sprintf("%x", md5.Sum(byteURLData))
	tinyURLRegex, err := regexp.Compile("[/+]")
	if err != nil {
		return "Unable to generate tiny URL"
	}
	tinyURLData := tinyURLRegex.ReplaceAllString(base64.URLEncoding.EncodeToString([]byte(hashedURLData)), "_")
	if len(tinyURLData) < (startIndex + 6) {
		return "Unable to generate tiny URL"
	}
	tinyURL := tinyURLData[startIndex : startIndex+6]
	var dbURLData urls
	dbClient.Where("tinyurl = ?", tinyURL).Find(&dbURLData)
	if dbURLData.Tinyurl == "" {
		fmt.Println(dbURLData, "in not found")
		go StoreTinyURL(urls{Tinyurl: tinyURL, Longurl: longURL}, longURL, tinyURL, dbClient, redisClient)
		return tinyURL
	} else if (dbURLData.Tinyurl == tinyURL) && (dbURLData.Longurl == longURL) {
		fmt.Println(dbURLData, "in found and equal")
		return tinyURL
	} else {
		return GenerateHashAndInsert(longURL, startIndex+1, dbClient, redisClient)
	}
}

// IndexHandler -> Handles requests coming to / route
func IndexHandler(res http.ResponseWriter, req *http.Request) {
	io.WriteString(res, "Welcome!\n")
}

// GetTinyHandler -> Generates tiny URL and returns it
func GetTinyHandler(res http.ResponseWriter, req *http.Request, dbClient *gorm.DB, redisClient *redis.Client) {
	requestParams, err := req.URL.Query()["longUrl"]
	if !err || len(requestParams[0]) < 1 {
		res.WriteHeader(400)
		json.NewEncoder(res).Encode(Response{400, "missing tinyURL parameter"})
	} else {
		longURL := requestParams[0]
		tinyURL := GenerateHashAndInsert(longURL, 0, dbClient, redisClient)

		res.WriteHeader(http.StatusCreated)
		json.NewEncoder(res).Encode(Response{201, tinyURL})
	}
}

// GetLongHandler -> Fetches long URL and returns it
func GetLongHandler(res http.ResponseWriter, req *http.Request, dbClient *gorm.DB, redisClient *redis.Client) {
	requestParams, err := req.URL.Query()["tinyUrl"]
	if !err || len(requestParams[0]) < 1 {
		res.WriteHeader(400)
		json.NewEncoder(res).Encode(Response{400, "missing longURL parameter"})
	} else {
		tinyURL := requestParams[0]
		redisSearchResult := redisClient.HGet("urls", tinyURL)
		if redisSearchResult.Val() != "" {
			json.NewEncoder(res).Encode(Response{200, redisSearchResult.Val()})
		} else {
			var url urls
			dbClient.Where("tinyurl = ?", tinyURL).Select("longurl").Find(&url)
			if url.Longurl != "" {
				redisClient.HSet("urls", tinyURL, url.Longurl)

				res.WriteHeader(http.StatusCreated)
				json.NewEncoder(res).Encode(Response{201, url.Longurl})
			} else {
				res.WriteHeader(404)
				json.NewEncoder(res).Encode(Response{400, "invalid parameter"})
			}
		}
	}
}

func main() {
	// redis client
	redisClient := RedisClient()

	// ping redis DB
	pong, err := redisClient.Ping().Result()
	fmt.Println("Redis ping", pong, err)

	// Postgres client
	dbClient := PostgresClient()
	defer dbClient.Close()

	// Automatically migrate the schema, to keep the schema update to date.
	dbClient.AutoMigrate(&urls{})

	// mux router instance
	router := mux.NewRouter()

	// home endpoint
	router.HandleFunc("/", IndexHandler)

	// 'tiny/' is get method endpoint returns tiny url
	router.HandleFunc("/tiny/", func(w http.ResponseWriter, r *http.Request) {
		GetTinyHandler(w, r, dbClient, redisClient)
	}).Methods("GET")

	// 'long/' is get method endpoint returns long url
	router.HandleFunc("/long/", func(w http.ResponseWriter, r *http.Request) {
		GetLongHandler(w, r, dbClient, redisClient)
	}).Methods("GET")

	log.Fatal(http.ListenAndServe(":8080", router))
}
