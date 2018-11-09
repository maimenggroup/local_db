package ai_db

import (
	"fmt"
	"strconv"
	"testing"
)

var aiLevel *AiLevel

func init() {
	aiLevel = &AiLevel{}
	aiLevel.Init("../../data/level_test/")
}

func TestAiLevel_Get(t *testing.T) {
	t.Log(aiLevel.GetString("name"))
}

func TestAiLevel_Put(t *testing.T) {
	if aiLevel.PutString("name", "mindata") != nil {
		t.Error("put failed.")
	}
}

func TestAiLevel_Del(t *testing.T) {
	if aiLevel.DelString("name") != nil {
		t.Error("del failed.")
	}
}

func TestAiLevel_PutBatch(t *testing.T) {
	aiLevel.PutStringBatch(&[]string{"name", "mail"}, &[]string{"maimeng", "maimeng@163.com"})
	val, err := aiLevel.GetString("name")
	if val != "maimeng" || err != nil {
		t.Errorf("execpted (maimeng, nil), but (%s, %T) returned", val, err)
	}
	val, err = aiLevel.GetString("mail")
	if val != "maimeng@163.com" || err != nil {
		t.Errorf("execpted (maimeng@163.com, nil), but (%s, %T) returned", val, err)
	}
}

func TestAiLevel_DelBatch(t *testing.T) {
	if aiLevel.DelStringBatch(&[]string{"name", "mail"}) != nil {
		t.Error("del batch failed.")
	}
}

func TestAiLevel_IncrFloat64(t *testing.T) {
	key := "jim_budget_2"
	var val, inc float64
	val = 2507604.49
	// s1 := strconv.FormatFloat(val, 'f', -1, 64)
	// aiLevel.PutString(key, s1)
	s2, _ := aiLevel.GetString(key)
	t.Log(s2)
	inc = 10000.12
	aiLevel.IncrFloat64(key, inc)
	s2, _ = aiLevel.GetString(key)
	t.Log(s2)
	val, _ = strconv.ParseFloat(s2, 64)
	t.Log(fmt.Sprintf("%.2f", val))
	t.Log("any way the diff occurs, 有误差!!!")
}

func TestAiLevel_IncrInt64(t *testing.T) {
	key := "jim_budget_1"
	var val, inc int64
	val = 2507604
	// s1 := strconv.FormatInt(val, 10)
	// aiLevel.PutString(key, s1)
	s2, _ := aiLevel.GetString(key)
	t.Log(s2)
	inc = 10000
	aiLevel.IncrInt64(key, inc)
	s2, _ = aiLevel.GetString(key)
	t.Log(s2)
	val, _ = strconv.ParseInt(s2, 10, 64)
	t.Log(fmt.Sprintf("%d", val))
}

func TestAiLevel_Scan(t *testing.T) {
	aiLevel.PutString("one", "1")
	aiLevel.PutString("two", "2")
	aiLevel.PutString("ten", "10")
	aiLevel.Scan(func(key []byte, val []byte) {
		fmt.Println(string(key), "=>", string(val))
	})
}

func TestAiLevel_Concurrent(t *testing.T) {
	done := make(chan bool, 30)
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				aiLevel.PutString(fmt.Sprintf("key%d", i*100+j), fmt.Sprintf("val%d", i*100+j))
			}
			done <- true
		}()
	}
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				aiLevel.DelString(fmt.Sprintf("key%d", i*100+j))
			}
			done <- true
		}()
	}
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				aiLevel.GetString(fmt.Sprintf("key%d", i*100+j))
			}
			done <- true
		}()
	}

	for i := 0; i < 30; i++ {
		<-done
	}
}
