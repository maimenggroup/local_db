package ai_db

import (
	"fmt"
	"testing"
)

var aiLocal *AiLevelGroupDb

func init() {
	aiLocal = &AiLevelGroupDb{}
	aiLocal.Init("../../data/level_group_test", []string{"maimeng", "mindata"})
}

func TestAiLevelGroupDb_Get(t *testing.T) {
	t.Log(aiLocal.GetString("maimeng", "name"))
}

func TestAiLevelGroupDb_Put(t *testing.T) {
	if aiLocal.PutString("maimeng", "name", "robin") != nil {
		t.Error("put failed.")
	}
}

func TestAiLevelGroupDb_Del(t *testing.T) {
	if aiLocal.DelString("maimeng", "name") != nil {
		t.Error("del failed.")
	}
}

func TestAiLevelGroupDb_PutBatch(t *testing.T) {
	aiLocal.PutStringBatch("mindata", &[]string{"name", "mail"}, &[]string{"robin", "robin@foxmail.com"})
	val, err := aiLocal.GetString("mindata", "name")
	if val != "robin" || err != nil {
		t.Errorf("execpted (robin, nil), but (%s, %T) returned", val, err)
	}
	val, err = aiLocal.GetString("mindata", "mail")
	if val != "robin@foxmail.com" || err != nil {
		t.Errorf("execpted (robin@foxmail.com, nil), but (%s, %T) returned", val, err)
	}
}

func TestAiLevelGroupDb_DelBatch(t *testing.T) {
	if aiLocal.DelStringBatch("mindata", &[]string{"name", "mail"}) != nil {
		t.Error("del batch failed.")
	}
}

func TestAiLevelGroupDb_Concurrent(t *testing.T) {
	done := make(chan bool, 30)
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				aiLocal.PutString("maimeng", fmt.Sprintf("key%d", i*100+j), fmt.Sprintf("val%d", i*100+j))
			}
			done <- true
		}()
	}
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				aiLocal.DelString("maimeng", fmt.Sprintf("key%d", i*100+j))
			}
			done <- true
		}()
	}
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				aiLocal.GetString("maimeng", fmt.Sprintf("key%d", i*100+j))
			}
			done <- true
		}()
	}

	for i := 0; i < 30; i++ {
		<-done
	}
}
