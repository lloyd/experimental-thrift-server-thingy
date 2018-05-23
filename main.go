package main

import (
	"fmt"

	reverseif "./gen-go/reverse"
)

func main() {
	_ = reverseif.ReverseClient{}
	fmt.Printf("hello world\n")
}
