package parser

import (
	"encoding/binary"
	"fmt"

	pb "main/pkg/hadoop_hdfs_fsimage"

	"google.golang.org/protobuf/proto"
)

func (parser *FSImageParser) DecodeInode() (*pb.INodeSection, error) {
	fsumSection := parser.summarySection["INODE"]
	imageFile := parser.fsImageFile
	var (
		summaryBytes = make([]byte, *fsumSection.Length)
		inodeSection = &pb.INodeSection{}
	)
	off := *fsumSection.Offset
	_, err := imageFile.ReadAt(summaryBytes, int64(off))
	if err != nil {
		return &pb.INodeSection{}, err
	}

	headerLength, readBytes := binary.Uvarint(summaryBytes)
	if readBytes <= 0 {
		return &pb.INodeSection{}, fmt.Errorf("can't read summary invariant %v", readBytes)
	}
	headerBytes := summaryBytes[readBytes : readBytes+int(headerLength)]

	err = proto.Unmarshal(headerBytes, inodeSection)
	if err != nil {
		return &pb.INodeSection{}, err
	}
	fmt.Printf("Всего файлов: %d\n", inodeSection.GetNumInodes())
	summaryBytes = summaryBytes[readBytes+int(headerLength):]
	for i := 0; i < int(inodeSection.GetNumInodes()); i++ {
		// Читаем длину очередного айнода
		inodeLen, n := binary.Uvarint(summaryBytes)
		summaryBytes = summaryBytes[n:]

		// Читаем тело айнода
		inodeBytes := summaryBytes[:inodeLen]
		summaryBytes = summaryBytes[inodeLen:]

		inode := &pb.INodeSection_INode{}
		proto.Unmarshal(inodeBytes, inode)

		inodeType := inode.GetType()
		switch inodeType {
		case 1:
			fmt.Println("file", string(inode.GetName()))
		case 2:
			fmt.Println("dir", string(inode.GetName()))
		default:
			fmt.Println("symlink", string(inode.GetName()))
		}

		// fmt.Printf("[%d] Файл: %s (Access: %d)\n", i, inode.Name, inode.File.GetAccessTime())
	}
	return inodeSection, nil

}
