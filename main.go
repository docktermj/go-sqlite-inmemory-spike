/*
 */
package main

import (
	"log"

	"github.com/senzing-garage/go-sqlite-inmemory-spike/cmd"
)

func main() {
	log.SetFlags(0)
	cmd.Execute()
}
