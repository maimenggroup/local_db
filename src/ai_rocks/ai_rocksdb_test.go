package ai_rocks

import "testing"

var aiRocksDb *AiRocksDbs

const (
	WHITELISTTABLE = "white_list"
	BLACKLISTTABLE = "black_list"
)

func init(){
	aiRocksDb = &AiRocksDbs{}
	aiRocksDb.Init("../../data/rocks_test/", []string{WHITELISTTABLE, BLACKLISTTABLE})
}

func TestAiRocksDbs_GetString(t *testing.T) {
	val, err := aiRocksDb.GetString(WHITELISTTABLE, "jim")
	if err != nil {
		t.Log(err.Error())
	} else {
		t.Log(val)
	}
}

func TestAiRocksDbs_PutString(t *testing.T) {
	err := aiRocksDb.PutString(WHITELISTTABLE, "jim", "32")
	if err != nil {
		t.Error(err.Error())
	} else {
		t.Log("TestAiRocksDbs_PutString ok")
	}
}
