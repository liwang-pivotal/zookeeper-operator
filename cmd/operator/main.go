package main

import (
	"time"
	"fmt"
)

func main() {
	timer := time.NewTicker(1*time.Second)

	defer timer.Stop()

	for {
		<-timer.C
		fmt.Println("hello")
	}
}
