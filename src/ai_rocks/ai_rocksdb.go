package ai_rocks

import (
	"fmt"
	"github.com/tecbot/gorocksdb"
)

type AiRocksDbs struct {
	dbPath      string
	option      *gorocksdb.Options
	writeOption *gorocksdb.WriteOptions
	readOption  *gorocksdb.ReadOptions
	dbs         map[string]*AiRocksTable
}

type AiRocksDbsError struct {
	message string
}

func (e *AiRocksDbsError) Error() string {
	return e.message
}

func (alb *AiRocksDbs) Init(dbpath string, tables []string) error {
	alb.dbPath = dbpath
	alb.dbs = map[string]*AiRocksTable{}
	alb.option = gorocksdb.NewDefaultOptions()
	alb.option.SetCreateIfMissing(true)
	alb.option.SetCompression(gorocksdb.ZLibCompression)
	alb.writeOption = gorocksdb.NewDefaultWriteOptions()
	alb.readOption = gorocksdb.NewDefaultReadOptions()
	for _, table := range tables {
		if _, ok := alb.dbs[table]; ok {
			continue
		}
		var rt *AiRocksTable
		rt = &AiRocksTable{}
		if err := rt.Init(table, alb.dbPath, alb.option, alb.writeOption, alb.readOption); err != nil {
			return err
		}
		alb.dbs[table] = rt
	}
	return nil
}

func (alb *AiRocksDbs) Close() {
	for _, db := range alb.dbs {
		db.Close()
	}
}

func (alb *AiRocksDbs) Get(tab string, key []byte) ([]byte, error) {
	ropt := gorocksdb.NewDefaultReadOptions()
	if db, ok := alb.dbs[tab]; ok {
		return db.Db.GetBytes(ropt, key)
	}
	return []byte{}, &AiRocksDbsError{message: fmt.Sprintf("table[%s] not exists", tab)}
}

func (alb *AiRocksDbs) Put(tab string, key []byte, val []byte) error {
	wopt := gorocksdb.NewDefaultWriteOptions()
	if db, ok := alb.dbs[tab]; ok {
		return db.Db.Put(wopt, key, val)
	}
	return &AiRocksDbsError{message: fmt.Sprintf("table[%s] not exists", tab)}
}

func (alb *AiRocksDbs) Del(tab string, key []byte) error {
	wopt := gorocksdb.NewDefaultWriteOptions()
	if db, ok := alb.dbs[tab]; ok {
		return db.Db.Delete(wopt, key)
	}
	return &AiRocksDbsError{message: fmt.Sprintf("table[%s] not exists", tab)}
}

func (alb *AiRocksDbs) GetString(tab string, key string) (string, error) {
	ropt := gorocksdb.NewDefaultReadOptions()
	if db, ok := alb.dbs[tab]; ok {
		val, err := db.Db.GetBytes(ropt, []byte(key))
		return string(val), err
	}
	return "", &AiRocksDbsError{message: fmt.Sprintf("table[%s] not exists", tab)}
}

func (alb *AiRocksDbs) PutString(tab string, key string, val string) error {
	wopt := gorocksdb.NewDefaultWriteOptions()
	if db, ok := alb.dbs[tab]; ok {
		return db.Db.Put(wopt, []byte(key), []byte(val))
	}
	return &AiRocksDbsError{message: fmt.Sprintf("table[%s] not exists", tab)}
}

func (alb *AiRocksDbs) DelString(tab string, key string) error {
	wopt := gorocksdb.NewDefaultWriteOptions()
	if db, ok := alb.dbs[tab]; ok {
		return db.Db.Delete(wopt, []byte(key))
	}
	return &AiRocksDbsError{message: fmt.Sprintf("table[%s] not exists", tab)}
}

func (alb *AiRocksDbs) IncUpdate(tab string, file, split string) error {
	if db, ok := alb.dbs[tab]; ok {
		return db.IncUpdate(file, split)
	}
	return &AiRocksDbsError{message: fmt.Sprintf("table[%s] not exists", tab)}
}

func (alb *AiRocksDbs) FullUpdate(tab string, file, split string) error {
	if db, ok := alb.dbs[tab]; ok {
		return db.FullUpdate(file, split)
	}
	return &AiRocksDbsError{message: fmt.Sprintf("table[%s] not exists", tab)}
}
