package main

import (
	"database/sql"
	"fmt"
	"net/http"

	_ "github.com/go-sql-driver/mysql"
)

var (
	DSN = "root:1234@tcp(localhost:3306)/golang?charset=utf8&interpolateParams=true"
)

func main() {
	db, err := sql.Open("mysql", DSN)
	if err != nil {
		panic(err)
	}

	err = db.Ping()
	if err != nil {
		panic(err)
	}

	handler, err := NewDbExplorer(db)
	if err != nil {
		panic(err)
	}

	fmt.Println("starting server at :8082")
	http.ListenAndServe(":8082", handler)
}
