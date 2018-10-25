package main

import (
	"os"
	"os/signal"
	"syscall"
	"ai_rocks"
	"fmt"
)

const (
	WHITETABLE = "white_list"
	BLACKTABLE = "black_list"
)

func main() {
	var aiRocksDbs *ai_rocks.AiRocksDbs
	aiRocksDbs = &ai_rocks.AiRocksDbs{}
	err := aiRocksDbs.Init("../../data/rocks_test", []string{WHITETABLE, BLACKTABLE})
	if err != nil {
		fmt.Printf("init rocksdbs error[%s]\n", err.Error())
	}
	err = aiRocksDbs.PutString(WHITETABLE, "jim", "32")
	if err != nil {
		fmt.Printf("rocksdbs put error[%s]\n", err.Error())
	}
	err = aiRocksDbs.PutString(WHITETABLE, "maimeng", "10months")
	if err != nil {
		fmt.Printf("rocksdbs put error[%s]\n", err.Error())
	}
	err = aiRocksDbs.PutString(WHITETABLE, "mindata", "20")
	if err != nil {
		fmt.Printf("rocksdbs put error[%s]\n", err.Error())
	}
	val, err := aiRocksDbs.GetString(WHITETABLE, "jim")
	if err != nil {
		fmt.Printf("rocksdbs get error[%s]\n", err.Error())
	} else {
		fmt.Printf("get [%s]\n", val)
	}
	quit := make(chan os.Signal)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGINT)
	<-quit
	aiRocksDbs.Close()
}
