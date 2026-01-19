package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	pb "main/pkg/hadoop_hdfs_fsimage"

	"google.golang.org/protobuf/proto"
)

const (
	FILE_SUM_BYTES = 4
	ROOT_INODE_ID  = 16385
)

var (
	inodeData = make(map[uint64]*pb.INodeSection_INode)
	stringMap = make(map[uint32]string)
)

type PermissionStatus struct {
	Permission string
	UserName   string
	GroupName  string
}

type Row struct {
	Path               string
	Replication        uint32
	ModificationTime   string
	AccessTime         string
	PreferredBlockSize uint64
	BlocksCount        uint32
	FileSize           uint64
	NsQuota            int64
	DsQuota            int64
	Permission         string
	UserName           string
	GroupName          string
}

func logIfErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s [output.tsv]\n", os.Args[0])
		os.Exit(1)
	}

	fileName := os.Args[1]
	outputPath := ""
	if len(os.Args) > 2 {
		outputPath = os.Args[2]
	}

	fInfo, err := os.Stat(fileName)
	logIfErr(err)
	f, err := os.Open(fileName)
	logIfErr(err)
	defer f.Close()

	fileLength := fInfo.Size()
	fSummaryLength := decodeFileSummaryLength(fileLength, f)
	sectionMap := parseFileSummary(f, fileLength, fSummaryLength)

	// ИСПРАВЛЕНИЕ 1: Правильное имя секции
	if sec, ok := sectionMap["STRING_TABLE"]; ok {
		parseStringTable(sec, f)
		fmt.Printf("Loaded %d strings\n", len(stringMap))
	} else {
		// Попробуем альтернативные имена, если вдруг версия Hadoop странная
		if sec, ok := sectionMap["STRINGTABLE"]; ok {
			parseStringTable(sec, f)
		} else {
			fmt.Println("Warning: STRING_TABLE section not found!")
		}
	}

	inodeSectionInfo := sectionMap["INODE"]
	parseInodeSection(inodeSectionInfo, f)

	inodeDirectorySectionInfo := sectionMap["INODE_DIR"]
	parChildrenMap := parseInodeDirectorySection(inodeDirectorySectionInfo, f)

	rootTreeNode := findChildren(parChildrenMap, ROOT_INODE_ID)
	rows := make([]Row, 0, 10000)

	if rootInode, exists := inodeData[ROOT_INODE_ID]; exists {
		rootRow := buildRowForINode(rootInode, "/")
		rows = append(rows, rootRow)
	}

	for _, child := range rootTreeNode {
		collectRows(child, "/", &rows)
	}

	writeTSV(rows, outputPath)
}

func buildRowForINode(inode *pb.INodeSection_INode, path string) Row {
	path = convertSpecialSymbols(path)
	row := Row{
		Path: path,
	}

	iType := inode.GetType()
	switch iType {
	case 1: // FILE
		file := inode.GetFile()
		row.Replication = file.GetReplication()
		row.ModificationTime = formatTime(file.GetModificationTime())
		row.AccessTime = formatTime(file.GetAccessTime())
		row.PreferredBlockSize = file.GetPreferredBlockSize()
		row.BlocksCount = uint32(len(file.GetBlocks()))
		row.FileSize = getFileSize(file)
		row.NsQuota = 0
		row.DsQuota = 0
		perm := decodePermission(file.GetPermission())
		row.Permission = perm.Permission
		row.UserName = convertSpecialSymbols(perm.UserName)
		row.GroupName = convertSpecialSymbols(perm.GroupName)
	case 2: // DIRECTORY
		dir := inode.GetDirectory()
		row.Replication = 0
		row.ModificationTime = formatTime(dir.GetModificationTime())
		row.AccessTime = "1970-01-01 00:00:00"
		row.PreferredBlockSize = 0
		row.BlocksCount = 0
		row.FileSize = 0
		row.NsQuota = int64(dir.GetNsQuota())
		row.DsQuota = int64(dir.GetDsQuota())
		perm := decodePermission(dir.GetPermission())
		row.Permission = perm.Permission
		row.UserName = convertSpecialSymbols(perm.UserName)
		row.GroupName = convertSpecialSymbols(perm.GroupName)
	}

	return row
}

func collectRows(node *INodeTree, parentPath string, rows *[]Row) {
	if node == nil {
		return
	}

	inode := node.Inode
	nodeNameStr := string(inode.Name)
	currentPath := parentPath
	if parentPath != "/" {
		currentPath = parentPath + "/" + nodeNameStr
	} else {
		currentPath = "/" + nodeNameStr
	}

	if inodeProto, exists := inodeData[inode.Id]; exists {
		row := buildRowForINode(inodeProto, currentPath)
		*rows = append(*rows, row)
	}

	if node.Children != nil {
		for _, child := range node.Children {
			collectRows(child, currentPath, rows)
		}
	}
}

func convertSpecialSymbols(input string) string {
	replacements := map[string]string{
		"\x00": "\\x00",
		"\t":   "\\t",
		"\n":   "\\n",
		"\r":   "\\r",
	}
	result := input
	for old, new := range replacements {
		result = strings.ReplaceAll(result, old, new)
	}
	return result
}

func formatTime(millis uint64) string {
	if millis == 0 {
		return "1970-01-01 00:00:00"
	}
	t := time.Unix(0, int64(millis)*int64(time.Millisecond)).UTC()
	return t.Format("2006-01-02 15:04:05")
}

func getFileSize(file *pb.INodeSection_INodeFile) uint64 {
	var size uint64 = 0
	for _, block := range file.GetBlocks() {
		size += block.GetNumBytes()
	}
	return size
}

func decodePermission(perm uint64) PermissionStatus {
	mode := uint16(perm & 0xFFFF)

	// В HDFS: [User 24] [Group 24] [Mode 16]
	// На Little Endian машине (x86) при чтении fixed64, байты перевернуты?
	// Если Hadoop писал BigEndian Long, то:
	//   byte 0 (addr) -> bits 56-63 (MSB)
	//   byte 7 (addr) -> bits 0-7 (LSB)
	// fixed64 читается как есть.

	// Если perm = 0x...User...Group...Mode
	// User = (perm >> 40)
	// Group = (perm >> 16)

	// Но мы уже пробовали менять местами.
	// Давайте вернемся к (>> 40) и (>> 16) и применим гипотезу о флагах.

	// Попробуем так:
	// USER - это (perm >> 0) & 0xFFFFFF (младшие, если перевернуто) или (>> 40)?
	// Давайте используем ту схему, где User нашелся (pkirillovs).
	// Если pkirillovs (ID 6) нашелся как User, то какой сдвиг вы использовали?
	// В последнем варианте вы использовали:
	// userId := (perm >> 40) & 0xFFFFFF

	// Ок, оставим сдвиги как в последнем варианте.
	// userId = 6.
	// groupId = 3.

	userId := (perm >> 40) & 0xFFFFFF
	groupId := (perm >> 16) & 0xFFFFFF

	// ГИПОТЕЗА:
	// User ID живут в пространстве 0x20... (536...)
	// Group ID живут в пространстве 0x40... (107...)

	// Пробуем искать User с флагом 0x20, а Group с флагом 0x40
	userName := lookupUser(uint32(userId))
	groupName := lookupGroup(uint32(groupId))

	return PermissionStatus{Permission: formatPermissionBits(mode), UserName: userName, GroupName: groupName}
}

func lookupUser(id uint32) string {
	// Ищем строго с флагом 0x20000000
	if s, ok := stringMap[id|0x20000000]; ok {
		return s
	}
	// Fallback
	if s, ok := stringMap[id]; ok {
		return s
	}
	return fmt.Sprintf("%d", id)
}

func lookupGroup(id uint32) string {
	// Ищем строго с флагом 0x40000000
	if s, ok := stringMap[id|0x40000000]; ok {
		return s
	}
	// Fallback
	if s, ok := stringMap[id]; ok {
		return s
	}
	return fmt.Sprintf("%d", id)
}

func lookupString(id uint32) string {
	// Прямой поиск
	if s, ok := stringMap[id]; ok {
		return s
	}

	// Флаги (как было)
	if s, ok := stringMap[id|0x20000000]; ok {
		return s
	}
	if s, ok := stringMap[id|0x40000000]; ok {
		return s
	}

	// А ТЕПЕРЬ КОРРЕКЦИЯ: Возможно ID в Inode = index, а в таблице index+1?
	// Попробуем найти id+1 с флагами
	idPlus := id + 1
	if s, ok := stringMap[idPlus|0x20000000]; ok {
		return s
	}
	if s, ok := stringMap[idPlus|0x40000000]; ok {
		return s
	}

	return fmt.Sprintf("%d", id)
}

func getStringFromId(id uint32) string {
	if str, exists := stringMap[id]; exists {
		return str
	}
	return ""
}

// ИСПРАВЛЕНИЕ 2: Корректное форматирование прав
func formatPermissionBits(mode uint16) string {
	// Mode bits:
	// 0100000 (directory) - игнорируем, тип файла берется из InodeType
	// 0000777 (права)
	// 0001000 (sticky bit)

	const (
		READ    = 4
		WRITE   = 2
		EXECUTE = 1
	)

	u := (mode >> 6) & 7
	g := (mode >> 3) & 7
	o := mode & 7
	sticky := (mode & (1 << 9)) != 0

	sb := make([]byte, 0, 9)

	// User
	if u&READ != 0 {
		sb = append(sb, 'r')
	} else {
		sb = append(sb, '-')
	}
	if u&WRITE != 0 {
		sb = append(sb, 'w')
	} else {
		sb = append(sb, '-')
	}
	if u&EXECUTE != 0 {
		sb = append(sb, 'x')
	} else {
		sb = append(sb, '-')
	}

	// Group
	if g&READ != 0 {
		sb = append(sb, 'r')
	} else {
		sb = append(sb, '-')
	}
	if g&WRITE != 0 {
		sb = append(sb, 'w')
	} else {
		sb = append(sb, '-')
	}
	if g&EXECUTE != 0 {
		sb = append(sb, 'x')
	} else {
		sb = append(sb, '-')
	}

	// Other
	if o&READ != 0 {
		sb = append(sb, 'r')
	} else {
		sb = append(sb, '-')
	}
	if o&WRITE != 0 {
		sb = append(sb, 'w')
	} else {
		sb = append(sb, '-')
	}

	if o&EXECUTE != 0 {
		if sticky {
			sb = append(sb, 't')
		} else {
			sb = append(sb, 'x')
		}
	} else {
		if sticky {
			sb = append(sb, 'T')
		} else {
			sb = append(sb, '-')
		}
	}

	return string(sb)
}

// ... Остальные функции (writeTSV, decodeFileSummaryLength, etc) можно оставить как есть
// ... НО убедитесь, что в decodeFileSummaryLength используется binary.BigEndian
// ... И функции parseFileSummary, parseStringTable и т.д. должны быть здесь же или в util.go
// Для целостности приведу их тут же, замените весь файл.

func decodeFileSummaryLength(fileLength int64, imageFile *os.File) int32 {
	var fSummaryLength int32
	fileSummaryLengthStart := fileLength - FILE_SUM_BYTES
	fSumLenBytes := make([]byte, FILE_SUM_BYTES)
	_, err := imageFile.ReadAt(fSumLenBytes, fileSummaryLengthStart)
	if err != nil && err != io.EOF {
		log.Fatal(err)
	}
	bReader := bytes.NewReader(fSumLenBytes)
	if err = binary.Read(bReader, binary.BigEndian, &fSummaryLength); err != nil {
		log.Fatal(err)
	}
	return fSummaryLength
}

func parseFileSummary(imageFile *os.File, fileLength int64, fSummaryLength int32) map[string]*pb.FileSummary_Section {
	sectionMap := make(map[string]*pb.FileSummary_Section)
	fileSummary := &pb.FileSummary{}
	fSummaryLength64 := int64(fSummaryLength)
	readAt := fileLength - fSummaryLength64 - FILE_SUM_BYTES
	fSummaryBytes := make([]byte, fSummaryLength)
	_, err := imageFile.ReadAt(fSummaryBytes, readAt)
	if err != nil && err != io.EOF {
		log.Fatal(err)
	}
	length, c := binary.Uvarint(fSummaryBytes)
	if c <= 0 {
		log.Fatal("buf too small(0) or overflows(-1): ", c)
	}
	fSummaryBytes = fSummaryBytes[c:]
	if err = proto.Unmarshal(fSummaryBytes[:length], fileSummary); err != nil {
		log.Fatal(err)
	}
	for _, value := range fileSummary.GetSections() {
		sectionMap[value.GetName()] = value
	}
	return sectionMap
}

func parseStringTable(info *pb.FileSummary_Section, imageFile *os.File) {
	if info == nil {
		return
	}

	stringTableBytes := make([]byte, info.GetLength())
	_, err := imageFile.ReadAt(stringTableBytes, int64(info.GetOffset()))
	logIfErr(err)

	// Пропускаем header секции
	headerLen, c := binary.Uvarint(stringTableBytes)
	if c <= 0 {
		log.Fatal("StringTable header error")
	}

	stringTableBytes = stringTableBytes[c+int(headerLen):]

	// Читаем записи
	for len(stringTableBytes) > 0 {
		msgLen, c := binary.Uvarint(stringTableBytes)
		if c <= 0 {
			break
		}
		stringTableBytes = stringTableBytes[c:]

		if uint64(len(stringTableBytes)) < msgLen {
			break
		}
		entryBytes := stringTableBytes[:msgLen]
		stringTableBytes = stringTableBytes[msgLen:]

		entry := &pb.StringTableSection_Entry{}
		if err = proto.Unmarshal(entryBytes, entry); err == nil {
			stringMap[entry.GetId()] = entry.GetStr()
		}
	}
	fmt.Println("DEBUG: String Table Content:")
	for id, str := range stringMap {
		fmt.Printf("ID %d -> %s\n", id, str)
	}
}

func parseInodeSection(info *pb.FileSummary_Section, imageFile *os.File) {
	inodeSectionBytes := make([]byte, info.GetLength())
	_, err := imageFile.ReadAt(inodeSectionBytes, int64(info.GetOffset()))
	logIfErr(err)

	headerLen, c := binary.Uvarint(inodeSectionBytes)
	if c <= 0 {
		log.Fatal("Inode header error")
	}
	inodeSectionBytes = inodeSectionBytes[c+int(headerLen):]

	for len(inodeSectionBytes) > 0 {
		msgLen, c := binary.Uvarint(inodeSectionBytes)
		if c <= 0 {
			break
		}
		inodeSectionBytes = inodeSectionBytes[c:]

		if uint64(len(inodeSectionBytes)) < msgLen {
			break
		}
		inodeBytes := inodeSectionBytes[:msgLen]
		inodeSectionBytes = inodeSectionBytes[msgLen:]

		inode := &pb.INodeSection_INode{}
		if err = proto.Unmarshal(inodeBytes, inode); err == nil {
			inodeData[inode.GetId()] = inode
		}
	}
}

func parseInodeDirectorySection(info *pb.FileSummary_Section, imageFile *os.File) map[uint64][]uint64 {
	parChildrenMap := make(map[uint64][]uint64)
	dirSectionBytes := make([]byte, info.GetLength())
	_, err := imageFile.ReadAt(dirSectionBytes, int64(info.GetOffset()))
	logIfErr(err)

	for len(dirSectionBytes) > 0 {
		msgLen, c := binary.Uvarint(dirSectionBytes)
		if c <= 0 {
			break
		}
		dirSectionBytes = dirSectionBytes[c:]

		if uint64(len(dirSectionBytes)) < msgLen {
			break
		}
		entryBytes := dirSectionBytes[:msgLen]
		dirSectionBytes = dirSectionBytes[msgLen:]

		dirEntry := &pb.INodeDirectorySection_DirEntry{}
		if err = proto.Unmarshal(entryBytes, dirEntry); err == nil {
			parChildrenMap[dirEntry.GetParent()] = dirEntry.GetChildren()
		}
	}
	return parChildrenMap
}

type INodeTree struct {
	Inode    INode
	Children []*INodeTree
}

type INode struct {
	Name []byte
	Id   uint64
	Type int
}

func findChildren(parChildrenMap map[uint64][]uint64, curInodeId uint64) []*INodeTree {
	children, ok := parChildrenMap[curInodeId]
	if !ok || len(children) == 0 {
		return nil
	}

	refs := make([]*INodeTree, len(children))
	for i, child := range children {
		inode := INode{Id: child}
		if inodeProto, exists := inodeData[child]; exists {
			inode.Name = inodeProto.GetName()
		}

		subChildren := findChildren(parChildrenMap, child)
		refs[i] = &INodeTree{inode, subChildren}
	}
	return refs
}

func writeTSV(rows []Row, outputPath string) {
	var writer *bufio.Writer
	if outputPath == "" {
		writer = bufio.NewWriter(os.Stdout)
	} else {
		f, err := os.Create(outputPath)
		logIfErr(err)
		defer f.Close()
		writer = bufio.NewWriter(f)
	}

	header := "Path\tReplication\tModificationTime\tAccessTime\tPreferredBlockSize\tBlocksCount\tFileSize\tNSQUOTA\tDSQUOTA\tPermission\tUserName\tGroupName\n"
	writer.WriteString(header)

	for _, row := range rows {
		line := fmt.Sprintf("%s\t%d\t%s\t%s\t%d\t%d\t%d\t%d\t%d\t%s\t%s\t%s\n",
			row.Path,
			row.Replication,
			row.ModificationTime,
			row.AccessTime,
			row.PreferredBlockSize,
			row.BlocksCount,
			row.FileSize,
			row.NsQuota,
			row.DsQuota,
			row.Permission,
			row.UserName,
			row.GroupName,
		)
		writer.WriteString(line)
	}
	writer.Flush()
}
