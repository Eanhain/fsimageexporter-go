package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	pb "main/pkg/hadoop_hdfs_fsimage"
	"os"

	"google.golang.org/protobuf/proto"
)

	"time"
const (
	FILE_SUM_BYTES = 4
	ROOT_INODE_ID  = 16385
)

// Global Variables
var pathCounter int = 0

func logIfErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	fileName := os.Args[1]
	fInfo, err := os.Stat(fileName)
	logIfErr(err)

	f, err := os.Open(fileName)
	logIfErr(err)

	fileLength := fInfo.Size()
	fSummaryLength := decodeFileSummaryLength(fileLength, f)
	sectionMap := parseFileSummary(f, fileLength, fSummaryLength)

	inodeSectionInfo := sectionMap["INODE"]
	inodeNames, entityCount := parseInodeSection(inodeSectionInfo, f)

	inodeDirectorySectionInfo := sectionMap["INODE_DIR"]
	parChildrenMap := parseInodeDirectorySection(inodeDirectorySectionInfo, f)
		outputTSV(inodeNames, parChildrenMap, InodeId(ROOT_INODE_ID))
	}
