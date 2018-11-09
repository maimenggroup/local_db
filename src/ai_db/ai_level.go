package ai_db

import (
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	BATCHNUM = 100
)

const (
	RUNDIR    = "run"
	BAKDIR    = "bak"
	UPDATEDIR = "update"
	TMPDIR    = "tmp"
)

type AiLevel struct {
	path  string
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
	al.path = dbpath
	db, err := leveldb.OpenFile(fmt.Sprintf("%s/%s", al.path, RUNDIR), nil)
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

func (al *AiLevel) GetFloat64(key string) (float64, error) {
	al.mutex.RLock()
	defer al.mutex.RUnlock()
	val, err := al.db.Get([]byte(key), nil)
	if err != nil {
		return 0.0, err
	}
	valF, err := strconv.ParseFloat(string(val), 64)
	if err != nil {
		return 0.0, err
	}
	return valF, err
}

func (al *AiLevel) PutFloat64(key string, val float64) error {
	al.mutex.Lock()
	defer al.mutex.Unlock()
	valS := strconv.FormatFloat(val, 'f', -1, 64)
	return al.db.Put([]byte(key), []byte(valS), nil)
}

func (al *AiLevel) IncrFloat64(key string, inc float64) error {
	al.mutex.Lock()
	defer al.mutex.Unlock()
	val, err := al.db.Get([]byte(key), nil)
	if err != nil {
		val = []byte("0.0")
	}
	valF, err := strconv.ParseFloat(string(val), 64)
	if err != nil {
		return err
	}
	sum := valF + inc
	fmt.Println(sum)
	valS := strconv.FormatFloat(sum, 'f', -1, 64)
	fmt.Println(valS)
	return al.db.Put([]byte(key), []byte(valS), nil)
}

func (al *AiLevel) GetInt64(key string) (int64, error) {
	al.mutex.RLock()
	defer al.mutex.RUnlock()
	val, err := al.db.Get([]byte(key), nil)
	if err != nil {
		return 0, err
	}
	valI, err := strconv.ParseInt(string(val), 10, 64)
	if err != nil {
		return 0, err
	}
	return valI, err
}

func (al *AiLevel) PutInt64(key string, val int64) error {
	al.mutex.Lock()
	defer al.mutex.Unlock()
	valS := strconv.FormatInt(val, 10)
	return al.db.Put([]byte(key), []byte(valS), nil)
}

func (al *AiLevel) IncrInt64(key string, inc int64) error {
	al.mutex.Lock()
	defer al.mutex.Unlock()
	val, err := al.db.Get([]byte(key), nil)
	if err != nil {
		val = []byte("0")
	}
	valI, err := strconv.ParseInt(string(val), 10, 64)
	if err != nil {
		return err
	}
	sum := valI + inc
	valS := strconv.FormatInt(sum, 10)
	return al.db.Put([]byte(key), []byte(valS), nil)
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

func (al *AiLevel) LoadFromFile(file, split string) error {
	keys, values, err := ReadFile(file, split)
	if err != nil {
		return err
	}
	return al.PutBatch(keys, values)
}

// 略显冗余，但是减少了加载文件可能带来的时间消耗，减少了写锁的时间
func (al *AiLevel) ReCreateFromFile(file, split string) error {
	curHour := time.Now().Format("2006-01-02/15/")
	bakDir := fmt.Sprintf("%s/%s/%s", al.path, BAKDIR, curHour)
	runDir := fmt.Sprintf("%s/%s", al.path, RUNDIR)
	tmpDir := fmt.Sprintf("%s/%s", al.path, TMPDIR)
	updDir := fmt.Sprintf("%s/%s", al.path, UPDATEDIR)

	// 先创建备份目录
	if err := os.MkdirAll(bakDir, 0777); err != nil {
		return err
	}

	updDb, err := CreateLevelDb(updDir, file, split)
	if err != nil {
		return err
	}
	al.mutex.Lock()
	al.db, updDb = updDb, al.db
	al.mutex.Unlock()
	updDb.Close()

	// 创建一个临时的db
	tmpDb, err := CreateLevelDb(tmpDir, file, split)
	if err != nil {
		return err
	}
	tmpDb.Close()

	// 备份，失败尝试删除目录 rename一个不存在的路径
	curSec := time.Now().Format("0405")
	if err := os.Rename(runDir, bakDir+curSec); err != nil {
		// Remove只能删除文件和空目录，要用RemoveAll
		os.RemoveAll(runDir)
	}
	// 不管怎样，更新新的数据目录
	if err := os.Rename(tmpDir, runDir); err != nil {
		return err
	}

	db, err := leveldb.OpenFile(runDir, nil)
	if err != nil {
		return err
	}

	al.mutex.Lock()
	al.db, db = db, al.db
	al.mutex.Unlock()
	db.Close()

	return nil
}
