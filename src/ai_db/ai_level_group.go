package ai_db

import (
	"fmt"
	"path"
)

type AiLevelGroupDb struct {
	dbs map[string]*AiLevel
}

type AiLevelDbError struct {
	message string
}

func (e *AiLevelDbError) Error() string {
	return e.message
}

func (alb *AiLevelGroupDb) Init(dbpath string, tables []string) error {
	alb.dbs = map[string]*AiLevel{}
	for _, table := range tables {
		if _, ok := alb.dbs[table]; ok {
			continue
		}
		rl := &AiLevel{}
		if err := rl.Init(path.Join(dbpath, table)); err != nil {
			return err
		}
		alb.dbs[table] = rl
	}

	return nil
}

func (alb *AiLevelGroupDb) Close() error {
	var err error
	for _, db := range alb.dbs {
		err = db.Close()
	}
	return err
}

func (alb *AiLevelGroupDb) Get(tab string, key []byte) ([]byte, error) {
	if db, ok := alb.dbs[tab]; ok {
		return db.Get(key)
	}
	return []byte{}, &AiLevelDbError{message: fmt.Sprintf("table[%s] not exists", tab)}
}

func (alb *AiLevelGroupDb) Put(tab string, key []byte, val []byte) error {
	if db, ok := alb.dbs[tab]; ok {
		return db.Put(key, val)
	}
	return &AiLevelDbError{message: fmt.Sprintf("table[%s] not exists", tab)}
}

func (alb *AiLevelGroupDb) Del(tab string, key []byte) error {
	if db, ok := alb.dbs[tab]; ok {
		return db.Del(key)
	}
	return &AiLevelDbError{message: fmt.Sprintf("table[%s] not exists", tab)}
}

func (alb *AiLevelGroupDb) PutBatch(tab string, keys *[][]byte, vals *[][]byte) error {
	if db, ok := alb.dbs[tab]; ok {
		return db.PutBatch(keys, vals)
	}
	return &AiLevelDbError{message: fmt.Sprintf("table[%s] not exists", tab)}
}

func (alb *AiLevelGroupDb) DelBatch(tab string, keys *[][]byte) error {
	if db, ok := alb.dbs[tab]; ok {
		return db.DelBatch(keys)
	}
	return &AiLevelDbError{message: fmt.Sprintf("table[%s] not exists", tab)}
}

func (alb *AiLevelGroupDb) GetString(tab string, key string) (string, error) {
	if db, ok := alb.dbs[tab]; ok {
		return db.GetString(key)
	}
	return "", &AiLevelDbError{message: fmt.Sprintf("table[%s] not exists", tab)}
}

func (alb *AiLevelGroupDb) PutString(tab string, key string, val string) error {
	if db, ok := alb.dbs[tab]; ok {
		return db.PutString(key, val)
	}
	return &AiLevelDbError{message: fmt.Sprintf("table[%s] not exists", tab)}
}

func (alb *AiLevelGroupDb) DelString(tab string, key string) error {
	if db, ok := alb.dbs[tab]; ok {
		return db.DelString(key)
	}
	return &AiLevelDbError{message: fmt.Sprintf("table[%s] not exists", tab)}
}

func (alb *AiLevelGroupDb) PutStringBatch(tab string, keys *[]string, vals *[]string) error {
	if db, ok := alb.dbs[tab]; ok {
		return db.PutStringBatch(keys, vals)
	}
	return &AiLevelDbError{message: fmt.Sprintf("table[%s] not exists", tab)}
}

func (alb *AiLevelGroupDb) DelStringBatch(tab string, keys *[]string) error {
	if db, ok := alb.dbs[tab]; ok {
		return db.DelStringBatch(keys)
	}
	return &AiLevelDbError{message: fmt.Sprintf("table[%s] not exists", tab)}
}

func (alb *AiLevelGroupDb) LoadFromFile(tab, file, split string) error {
	if db, ok := alb.dbs[tab]; ok {
		return db.LoadFromFile(file, split)
	}
	return &AiLevelDbError{message: fmt.Sprintf("table[%s] not exists", tab)}
}

func (alb *AiLevelGroupDb) ReCreateFromFile(tab, file, split string) error {
	if db, ok := alb.dbs[tab]; ok {
		return db.ReCreateFromFile(file, split)
	}
	return &AiLevelDbError{message: fmt.Sprintf("table[%s] not exists", tab)}
}