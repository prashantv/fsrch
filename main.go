package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"strings"

	// Side-Effect Import the qlbridge sql driver
	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/datasource"
	_ "github.com/araddon/qlbridge/qlbdriver"
)

func main() {
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		log.Fatalf("Please provide a valid select query")
	}
	sqlText := args[0]

	registerFunctions()
	fsSource := &FilesystemSource{}
	datasource.Register("fs", fsSource)

	db, err := sql.Open("qlbridge", "fs://.")
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()

	rows, err := db.Query(sqlText)
	if err != nil {
		u.Errorf("could not execute query: %v", err)
		return
	}
	defer rows.Close()
	cols, _ := rows.Columns()

	// this is just stupid hijinx for getting pointers for unknown len columns
	readCols := make([]interface{}, len(cols))
	writeCols := make([]string, len(cols))
	for i := range writeCols {
		readCols[i] = &writeCols[i]
	}
	fmt.Println(strings.Join(cols, ","))
	for rows.Next() {
		rows.Scan(readCols...)
		fmt.Println(strings.Join(writeCols, ","))
	}
	fmt.Println("")
}
