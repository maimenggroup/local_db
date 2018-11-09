package ai_rocks

import (
	"github.com/tecbot/gorocksdb"
	"testing"
)

var rocksTable *AiRocksTable

const table = "test_table"

func init() {
	rocksTable = &AiRocksTable{}
	option := gorocksdb.NewDefaultOptions()
	option.SetCreateIfMissing(true)
	option.SetCompression(gorocksdb.ZLibCompression)
	writeOption := gorocksdb.NewDefaultWriteOptions()
	readOption := gorocksdb.NewDefaultReadOptions()
	rocksTable.Init(table, "../../data/rocks_test", option, writeOption, readOption)
}

func TestAiRocksTable_GetString(t *testing.T) {
	t.Log(rocksTable.GetString("jim"))
}

func TestAiRocksTable_PutString(t *testing.T) {
	err := rocksTable.PutString("jim", "100")
	if err != nil {
		t.Error(err.Error())
	} else {
		t.Log("test PutString ok")
	}
}
