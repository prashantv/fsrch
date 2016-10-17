package main

import (
	"database/sql/driver"
	"os"
	"path/filepath"
	"sync"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/value"
)

const _tableName = "fs"

type pathAndInfo struct {
	path string
	info os.FileInfo
}

var (
	// Make sure that our source satisfied SourceAll.
	_ schema.SourceAll = (*FilesystemSource)(nil)

	// Initialize the schema that the FilesytemSource returns.
	_tableSchema, _columns, _columnsMap = initSchemaColumns()
)

func initSchemaColumns() (*schema.Table, []string, map[string]int) {
	table := schema.NewTable(_tableName)
	colsMap := make(map[string]int, len(_columnsDesc))
	cols := make([]string, len(_columnsDesc))
	for i, f := range _columnsDesc {
		cols[i] = f.name
		colsMap[f.name] = i
		table.AddField(schema.NewFieldBase(f.name, f.t, 64, f.desc))
	}
	return table, cols, colsMap
}

// FilesystemSource implements a source that traverses through the filesystem.
// There is a single table, "fs" available in this source.
type FilesystemSource struct{}

// Tables returns the list of tables that can be used by this source, just "fs".
func (fs *FilesystemSource) Tables() []string {
	return []string{_tableName}
}

// Open returns a connection to iterate over this filesystem.
func (fs *FilesystemSource) Open(source string) (schema.Conn, error) {
	return newConn("."), nil
}

// Close doesn't do anything.
func (fs *FilesystemSource) Close() error { return nil }

// Table returns the schema for the returned table.
func (fs *FilesystemSource) Table(table string) (*schema.Table, error) {
	if table != _tableName {
		return nil, schema.ErrNotFound
	}

	return _tableSchema, nil
}

type fsConn struct {
	info  chan pathAndInfo
	rowCt uint64

	quit    chan struct{}
	running sync.WaitGroup
	walkErr error
}

func newConn(root string) schema.ConnScanner {
	c := &fsConn{
		info: make(chan pathAndInfo, 10),
		quit: make(chan struct{}),
	}
	c.running.Add(1)
	go c.start(root)
	return c
}

func (c *fsConn) start(root string) {
	defer c.running.Done()
	defer close(c.info)

	c.walkErr = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		select {
		case c.info <- pathAndInfo{path, info}:
		case <-c.quit:
			return filepath.SkipDir
		}

		return nil
	})
}

func (c *fsConn) Close() error {
	close(c.quit)
	c.running.Wait()
	return c.walkErr
}

var cols = map[string]int{
	"name": 0,
	"path": 1,
	"size": 2,
}

var _columnsDesc = []struct {
	name string
	t    value.ValueType
	desc string
}{
	{"name", value.StringType, "The base name of the object"},
	{"relPath", value.StringType, "Path of the object, relative to the search root"},
	{"size", value.IntType, "Size of the file (0 for directories)"},
	{"isDir", value.BoolType, "Whether the object is a directory"},
	{"isFile", value.BoolType, "Whether the object is a file"},
}

func (c *fsConn) Next() schema.Message {
	pi, ok := <-c.info
	if !ok {
		return nil
	}
	c.rowCt++

	vals := []driver.Value{
		pi.info.Name(),
		pi.path,
		pi.info.Size(),
		pi.info.IsDir(),
		!pi.info.IsDir(),
	}
	return datasource.NewSqlDriverMessageMap(c.rowCt, vals, _columnsMap)
}

func (c *fsConn) Columns() []string {
	return _columns
}
