package ai_db

import (
	"fmt"
	"path"
)

type AiLocalDb struct {
	dbs map[string]*AiLevel
}

type AiLocalDbError struct {
	message string
}

func (e *AiLocalDbError) Error() string {
	return e.message
}

func (alb *AiLocalDb) Init(dbpath string, tables []string) error {
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

func (alb *AiLocalDb) Close() error {
	var err error
	for _, db := range alb.dbs {
		err = db.Close()
	}
	return err
}

func (alb *AiLocalDb) Get(tab string, key []byte) ([]byte, error) {
	if db, ok := alb.dbs[tab]; ok {
		return db.Get(key)
	}
	return []byte{}, &AiLocalDbError{message: fmt.Sprintf("table[%s] not exists", tab)}
}

func (alb *AiLocalDb) Put(tab string, key []byte, val []byte) error {
	if db, ok := alb.dbs[tab]; ok {
		return db.Put(key, val)
	}
	return &AiLocalDbError{message: fmt.Sprintf("table[%s] not exists", tab)}
}

func (alb *AiLocalDb) Del(tab string, key []byte) error {
	if db, ok := alb.dbs[tab]; ok {
		return db.Del(key)
	}
	return &AiLocalDbError{message: fmt.Sprintf("table[%s] not exists", tab)}
}

func (alb *AiLocalDb) PutBatch(tab string, keys *[][]byte, vals *[][]byte) error {
	if db, ok := alb.dbs[tab]; ok {
		return db.PutBatch(keys, vals)
	}
	return &AiLocalDbError{message: fmt.Sprintf("table[%s] not exists", tab)}
}

func (alb *AiLocalDb) DelBatch(tab string, keys *[][]byte) error {
	if db, ok := alb.dbs[tab]; ok {
		return db.DelBatch(keys)
	}
	return &AiLocalDbError{message: fmt.Sprintf("table[%s] not exists", tab)}
}

func (alb *AiLocalDb) GetString(tab string, key string) (string, error) {
	if db, ok := alb.dbs[tab]; ok {
		return db.GetString(key)
	}
	return "", &AiLocalDbError{message: fmt.Sprintf("table[%s] not exists", tab)}
}

func (alb *AiLocalDb) PutString(tab string, key string, val string) error {
	if db, ok := alb.dbs[tab]; ok {
		return db.PutString(key, val)
	}
	return &AiLocalDbError{message: fmt.Sprintf("table[%s] not exists", tab)}
}

func (alb *AiLocalDb) DelString(tab string, key string) error {
	if db, ok := alb.dbs[tab]; ok {
		return db.DelString(key)
	}
	return &AiLocalDbError{message: fmt.Sprintf("table[%s] not exists", tab)}
}

func (alb *AiLocalDb) PutStringBatch(tab string, keys *[]string, vals *[]string) error {
	if db, ok := alb.dbs[tab]; ok {
		return db.PutStringBatch(keys, vals)
	}
	return &AiLocalDbError{message: fmt.Sprintf("table[%s] not exists", tab)}
}

func (alb *AiLocalDb) DelStringBatch(tab string, keys *[]string) error {
	if db, ok := alb.dbs[tab]; ok {
		return db.DelStringBatch(keys)
	}
	return &AiLocalDbError{message: fmt.Sprintf("table[%s] not exists", tab)}
}
