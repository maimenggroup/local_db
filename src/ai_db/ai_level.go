package ai_db

import (
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"sync"
)

const (
	BATCHNUM int = 100
)

type AiLevel struct {
	db    *leveldb.DB
	mutex *sync.RWMutex
}

type AiLevelError struct {
	message string
}

func (e *AiLevelError) Error() string {
	return e.message
}

func (al *AiLevel) Init(dbpath string) error {
	db, err := leveldb.OpenFile(dbpath, nil)
	if err != nil {
		return err
	}
	al.db = db
	al.mutex = &sync.RWMutex{}

	return nil
}

func (al *AiLevel) Close() error {
	return al.db.Close()
}

func (al *AiLevel) Scan(run func(key []byte, val []byte)) {
	al.mutex.Lock()
	defer al.mutex.Unlock()
	it := al.db.NewIterator(nil, nil)
	for it.Next() {
		run(it.Key(), it.Value())
	}
}

func (al *AiLevel) Get(key []byte) ([]byte, error) {
	al.mutex.RLock()
	defer al.mutex.RUnlock()
	return al.db.Get(key, nil)
}

func (al *AiLevel) Put(key []byte, val []byte) error {
	al.mutex.Lock()
	defer al.mutex.Unlock()
	return al.db.Put(key, val, nil)
}

func (al *AiLevel) Del(key []byte) error {
	al.mutex.Lock()
	defer al.mutex.Unlock()
	return al.db.Delete(key, nil)
}

func (al *AiLevel) PutBatch(keys *[][]byte, vals *[][]byte) error {
	if len(*keys) != len(*vals) {
		return &AiLevelError{message: fmt.Sprintf("len(keys)[%d] != len(vals)[%d]", len(*keys), len(*vals))}
	}
	for i := 0; i < len(*keys); i++ {
		batch := &leveldb.Batch{}
		for j := 0; i < len(*keys) && j < BATCHNUM; j++ {
			batch.Put((*keys)[i], (*vals)[i])
			i++
		}
		al.mutex.Lock()
		err := al.db.Write(batch, nil)
		al.mutex.Unlock()
		if err != nil {
			return err
		}
	}

	return nil
}

func (al *AiLevel) DelBatch(keys *[][]byte) error {
	for i := 0; i < len(*keys); i++ {
		batch := &leveldb.Batch{}
		for j := 0; i < len(*keys) && j < BATCHNUM; j++ {
			batch.Delete((*keys)[i])
			i++
		}
		al.mutex.Lock()
		err := al.db.Write(batch, nil)
		al.mutex.Unlock()
		if err != nil {
			return err
		}
	}

	return nil
}

func (al *AiLevel) GetString(key string) (string, error) {
	al.mutex.RLock()
	defer al.mutex.RUnlock()
	val, err := al.db.Get([]byte(key), nil)
	return string(val), err
}

func (al *AiLevel) PutString(key string, val string) error {
	al.mutex.Lock()
	defer al.mutex.Unlock()
	return al.db.Put([]byte(key), []byte(val), nil)
}

func (al *AiLevel) DelString(key string) error {
	al.mutex.Lock()
	defer al.mutex.Unlock()
	return al.db.Delete([]byte(key), nil)
}

func (al *AiLevel) PutStringBatch(keys *[]string, vals *[]string) error {
	if len(*keys) != len(*vals) {
		return &AiLevelError{message: fmt.Sprintf("len(keys)[%d] != len(vals)[%d]", len(*keys), len(*vals))}
	}
	for i := 0; i < len(*keys); i++ {
		batch := &leveldb.Batch{}
		for j := 0; i < len(*keys) && j < BATCHNUM; j++ {
			batch.Put([]byte((*keys)[i]), []byte((*vals)[i]))
			i++
		}
		al.mutex.Lock()
		err := al.db.Write(batch, nil)
		al.mutex.Unlock()
		if err != nil {
			return err
		}
	}

	return nil
}

func (al *AiLevel) DelStringBatch(keys *[]string) error {
	for i := 0; i < len(*keys); i++ {
		batch := &leveldb.Batch{}
		for j := 0; i < len(*keys) && j < BATCHNUM; j++ {
			batch.Delete([]byte((*keys)[i]))
			i++
		}
		al.mutex.Lock()
		err := al.db.Write(batch, nil)
		al.mutex.Unlock()
		if err != nil {
			return err
		}
	}

	return nil
}
