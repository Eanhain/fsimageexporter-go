package parser

import (
	pb "main/pkg/hadoop_hdfs_fsimage"
	"os"
)

// FILE := MAGIC SECTION* <FileSummary> FileSummaryLength
// MAGIC := 'HDFSIMG1'
// SECTION := <NameSystemSection> | ...
// FileSummaryLength := 4 byte int
const (
	ROOT_NODE_ID        int   = 16385
	FILE_SUMMARY_LENGHT int64 = 4
)

type fsimageFileAttr struct {
	fsImageFile  *os.File
	fsImageStats os.FileInfo
}

type FSImageParser struct {
	summarySection map[string]*pb.FileSummary_Section
	fsimageFileAttr
}

func InitialParser(path string) (*FSImageParser, error) {
	file, err := os.OpenFile(path, os.O_RDONLY, 0422)
	if err != nil {
		return nil, err
	}
	fileStats, err := file.Stat()
	if err != nil {
		return nil, err
	}
	newParser := FSImageParser{
		summarySection: make(map[string]*pb.FileSummary_Section),
		fsimageFileAttr: fsimageFileAttr{
			fsImageFile:  file,
			fsImageStats: fileStats,
		},
	}
	return &newParser, nil

}
