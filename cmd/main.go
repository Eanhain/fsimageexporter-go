package main

import (
	"fmt"
	"main/internal/parser"
)

const (
	ROOT_NODE_ID        = 16385
	INVALID_INODE_ID    = -1
	FILE_SUMMARY_LENGHT = 4
)

// func summaryToMap(summary *pb.FileSummary) []any {
// 	summaryRet := make([]any, len(summary))
// 	for k, v := range &summary {

// 	}
// }

func main() {
	path := "/Users/pkirillovs/fsimageexporter-go/tests/fsimage_2026-01-17_01-00"
	parser, err := parser.InitialParser(path)
	if err != nil {
		panic(err)
	}
	err = parser.SetSummaryMap()
	if err != nil {
		panic(err)
	}
	inodeDecode, err := parser.DecodeInode()
	if err != nil {
		panic(err)
	}
	fmt.Println(inodeDecode.String())

}
