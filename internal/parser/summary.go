package parser

import (
	"bytes"
	"encoding/binary"
	"fmt"
	pb "main/pkg/hadoop_hdfs_fsimage"

	"google.golang.org/protobuf/proto"
)

func (parser *FSImageParser) decodeFileSummaryLength() (int32, error) {
	var (
		fSumLenBytes   = make([]byte, FILE_SUMMARY_LENGHT)
		fSummaryLength int32
	)
	fileSummaryLengthStart := parser.fsImageStats.Size() - FILE_SUMMARY_LENGHT
	bReader := bytes.NewReader(fSumLenBytes)
	_, err := parser.fsImageFile.ReadAt(fSumLenBytes, fileSummaryLengthStart)
	if err != nil {
		return -1, err
	}
	if err = binary.Read(bReader, binary.BigEndian, &fSummaryLength); err != nil {
		return -1, err
	}
	return fSummaryLength, nil
}

func (parser *FSImageParser) DecodeSummary() (*pb.FileSummary, error) {

	summaryLenght, err := parser.decodeFileSummaryLength()

	if err != nil {
		return &pb.FileSummary{}, err
	}

	var (
		summaryBytes = make([]byte, summaryLenght)
		pbSummary    = &pb.FileSummary{}
	)

	off := parser.fsImageStats.Size() - int64(summaryLenght) - FILE_SUMMARY_LENGHT
	_, err = parser.fsImageFile.ReadAt(summaryBytes, off)
	if err != nil {
		return &pb.FileSummary{}, err
	}
	_, readBytes := binary.Uvarint(summaryBytes)
	if readBytes <= 0 {
		return &pb.FileSummary{}, fmt.Errorf("can't read summary invariant %v", readBytes)
	}
	summaryBytes = summaryBytes[readBytes:]
	err = proto.Unmarshal(summaryBytes, pbSummary)
	if err != nil {
		return &pb.FileSummary{}, err
	}
	return pbSummary, nil

}

func (parser *FSImageParser) SetSummaryMap() error {
	pbSummary, err := parser.DecodeSummary()
	if err != nil {
		return err
	}
	for _, v := range pbSummary.GetSections() {
		parser.summarySection[v.GetName()] = v
	}
	return nil
}
