package ai_db

import (
	"bufio"
	"bytes"
	"github.com/syndtr/goleveldb/leveldb"
	"io"
	"os"
)

func ReadFile(file, split string) (*[][]byte, *[][]byte, error) {
	fi, err := os.Open(file)
	if err != nil {
		return nil, nil, err
	}
	defer fi.Close()

	var keys [][]byte
	var values [][]byte
	br := bufio.NewReader(fi)
	for {
		a, _, err := br.ReadLine()
		if err == io.EOF {
			break
		}

		data := bytes.Split(a, []byte(split))
		if len(data) < 2 {
			continue
		}
		keys = append(keys, data[0])
		values = append(values, data[1])
	}
	return &keys, &values, nil
}

func CreateLevelDb(dbpath, file, split string) (*leveldb.DB, error) {
	keys, values, err := ReadFile(file, split)
	if err != nil {
		return nil, err
	}
	db, err := leveldb.OpenFile(dbpath, nil)
	if err != nil {
		return nil, err
	}
	for i := 0; i < len(*keys); i++ {
		batch := &leveldb.Batch{}
		for j := 0; i < len(*keys) && j < BATCHNUM; j++ {
			batch.Put((*keys)[i], (*values)[i])
			i++
		}
		err := db.Write(batch, nil)
		if err != nil {
			return nil, err
		}
	}
	return db, nil
}
