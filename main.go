package main

import (
	"database/sql"
	"flag"
	"fmt"
	"path/filepath"
	"strings"

	// Side-Effect Import the qlbridge sql driver
	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/expr/builtins"
	_ "github.com/araddon/qlbridge/qlbdriver"
	"github.com/araddon/qlbridge/value"
)

var (
	sqlText          string
	flagCsvDelimiter = ","
	logging          = "info"
)

func init() {
	flag.StringVar(&logging, "logging", "fatal", "logging [ debug,info ]")
	flag.StringVar(&sqlText, "sql", "", "QL ish query multi-node such as [select user_id, yy(reg_date) from stdio];")
	flag.Parse()

	u.SetupLogging(logging)
	u.SetColorOutput()
}

func main() {
	if sqlText == "" {
		u.Errorf("You must provide a valid select query in argument:    --sql=\"select ...\"")
		return
	}

	// load all of our built-in functions
	builtins.LoadAllBuiltins()
	expr.FuncAdd("abs", absPath)
	expr.FuncAdd("ext", extPath)

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

func absPath(ctx expr.EvalContext, path value.Value) (value.StringValue, bool) {
	pathStr := path.ToString()
	if pathStr == "" {
		return value.NewStringValue(""), false
	}

	fullPath, err := filepath.Abs(pathStr)
	if err != nil {
		// TODO: Log an error?
		return value.NewStringValue(""), false
	}

	return value.NewStringValue(fullPath), true
}

func extPath(ctx expr.EvalContext, path value.Value) (value.StringValue, bool) {
	return value.NewStringValue(filepath.Ext(path.ToString())), true
}
