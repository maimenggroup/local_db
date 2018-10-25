package ai_rocks

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/tecbot/gorocksdb"
	"io"
	"os"
	"time"
)

const (
	RUNDIR    = "run"
	BAKDIR    = "bak"
	UPDATEDIR = "update"
	TMPDIR    = "tmp"
)

type AiRocksTable struct {
	Name        string
	Path        string
	Option      *gorocksdb.Options
	WriteOption *gorocksdb.WriteOptions
	ReadOption  *gorocksdb.ReadOptions
	Db          *gorocksdb.DB
}

func LoadRocksdbFromFile(db *gorocksdb.DB, option *gorocksdb.WriteOptions, file, split string) error {
	fi, err := os.Open(file)
	if err != nil {
		return err
	}
	defer fi.Close()

	br := bufio.NewReader(fi)
	for {
		a, _, err := br.ReadLine()
		if err == io.EOF {
			break
		}

		vals := bytes.Split(a, []byte(split))
		if len(vals) < 2 {
			continue
		}
		if err := db.Put(option, vals[0], vals[1]); err != nil {
			return err
		}
	}
	return nil
}

func (tb *AiRocksTable) Init(name, path string, option *gorocksdb.Options, woption *gorocksdb.WriteOptions, roption *gorocksdb.ReadOptions) error {
	tb.Name = name
	tb.Path = path
	tb.Option = option
	tb.WriteOption = woption
	tb.ReadOption = roption
	db, err := gorocksdb.OpenDb(tb.Option, fmt.Sprintf("%s/%s/%s", tb.Path, tb.Name, RUNDIR))
	if err != nil {
		return err
	}
	tb.Db = db
	return nil
}

func (tb *AiRocksTable) Create(subPath, file, split string) (*gorocksdb.DB, error) {
	db, err := gorocksdb.OpenDb(tb.Option, fmt.Sprintf("%s/%s/%s", tb.Path, tb.Name, subPath))
	if err != nil {
		return nil, err
	}
	if err := LoadRocksdbFromFile(db, tb.WriteOption, file, split); err != nil {
		return nil, err
	}
	return db, nil
}

// 增量更新
func (tb *AiRocksTable) IncUpdate(file, split string) error {
	return LoadRocksdbFromFile(tb.Db, tb.WriteOption, file, split)
}

// 全量更新
func (tb *AiRocksTable) FullUpdate(file, split string) error {
	// 创建两个一样的新数据库，一个供读，一个供切换使用
	tmpDb, err := tb.Create(TMPDIR, file, split)
	if err != nil {
		return err
	}
	tmpDb.Close()

	db, err := tb.Create(UPDATEDIR, file, split)
	if err != nil {
		return err
	}
	// 交换后关闭之前的db
	tb.Db, db = db, tb.Db
	db.Close()

	curTime := time.Now().Format("2006-01-02/15/04")
	bakDir := fmt.Sprintf("%s/%s/%s/%s", tb.Path, tb.Name, BAKDIR, curTime)
	runDir := fmt.Sprintf("%s/%s/%s", tb.Path, tb.Name, RUNDIR)
	tmpDir := fmt.Sprintf("%s/%s/%s", tb.Path, tb.Name, TMPDIR)
	// 先创建备份目录
	if err := os.MkdirAll(bakDir, 0777); err != nil {
		return err
	}
	// 备份失败尝试删除目录
	if err := os.Rename(runDir, bakDir); err != nil {
		os.Remove(runDir)
	}
	// 不管怎样，更新新的数据目录
	if err := os.Rename(tmpDir, runDir); err != nil {
		return err
	}
	// 打开更新了数据的目录
	newDb, err := gorocksdb.OpenDb(tb.Option, fmt.Sprintf("%s/%s/%s", tb.Path, tb.Name, RUNDIR))
	if err != nil {
		return err
	}
	// 交换后关闭之前的db
	tb.Db, newDb = newDb, tb.Db
	newDb.Close()

	return nil
}

func (tb *AiRocksTable) Get(key []byte) ([]byte, error) {
	return tb.Db.GetBytes(tb.ReadOption, key)
}

func (tb *AiRocksTable) Put(key, value []byte) error {
	return tb.Db.Put(tb.WriteOption, key, value)
}

func (tb *AiRocksTable) Delete(key []byte) error {
	return tb.Db.Delete(tb.WriteOption, key)
}

func (tb *AiRocksTable) GetString(key string) (string, error) {
	val, err := tb.Db.GetBytes(tb.ReadOption, []byte(key))
	if err != nil {
		return "", err
	} else {
		return string(val), nil
	}
}

func (tb *AiRocksTable) PutString(key, value string) error {
	return tb.Db.Put(tb.WriteOption, []byte(key), []byte(value))
}

func (tb *AiRocksTable) DeleteString(key string) error {
	return tb.Db.Delete(tb.WriteOption, []byte(key))
}

func (tb *AiRocksTable) Close() {
	tb.Db.Close()
}
