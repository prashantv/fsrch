package main

import (
	"io/ioutil"
	"path/filepath"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/expr/builtins"
	"github.com/araddon/qlbridge/value"
	"github.com/prashantv/fsrch/human"
)

func registerFunctions() {
	builtins.LoadAllBuiltins()
	expr.FuncAdd("abs", absPath)
	expr.FuncAdd("ext", extPath)
	expr.FuncAdd("contents", contentsPath)

	// Human formatting functions.
	expr.FuncAdd("human.size", humanSize)
}

func absPath(ctx expr.EvalContext, path value.StringValue) (value.StringValue, bool) {
	pathStr := path.Val()
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

func extPath(ctx expr.EvalContext, path value.StringValue) (value.StringValue, bool) {
	return value.NewStringValue(filepath.Ext(path.Val())), true
}

func contentsPath(ctx expr.EvalContext, path value.StringValue) (value.ByteSliceValue, bool) {
	contents, err := ioutil.ReadFile(path.Val())
	if err != nil {
		return value.NewByteSliceValue(nil), false
	}

	return value.NewByteSliceValue(contents), true
}

func humanSize(ctx expr.EvalContext, size value.IntValue) (value.StringValue, bool) {
	return value.NewStringValue(human.Size(size.Int())), true
}
