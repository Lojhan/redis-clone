package persistence

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"
	"unsafe"

	"github.com/lojhan/redis-clone/internal/store"
)

const (
	RDBVersion = 9

	opEOF        = 0xFF
	opSelectDB   = 0xFE
	opExpireTime = 0xFD
	opExpireMS   = 0xFC
	opResizeDB   = 0xFB
	opAux        = 0xFA

	typeString  = 0
	typeList    = 1
	typeSet     = 2
	typeZSet    = 3
	typeHash    = 4
	typeZSet2   = 5
	typeHashZip = 9
	typeListZip = 10
	typeSetInt  = 11
	typeZSetZip = 12
	typeQuick   = 14
)

type RDBWriter struct {
	writer io.Writer
}

func NewRDBWriter(w io.Writer) *RDBWriter {
	return &RDBWriter{writer: w}
}

func (w *RDBWriter) WriteHeader() error {

	header := fmt.Sprintf("REDIS%04d", RDBVersion)
	_, err := w.writer.Write([]byte(header))
	return err
}

func (w *RDBWriter) WriteAuxField(key, value string) error {
	if err := w.writeByte(opAux); err != nil {
		return err
	}
	if err := w.writeString(key); err != nil {
		return err
	}
	return w.writeString(value)
}

func (w *RDBWriter) WriteSelectDB(dbNum int) error {
	if err := w.writeByte(opSelectDB); err != nil {
		return err
	}
	return w.writeLength(uint64(dbNum))
}

func (w *RDBWriter) WriteResizeDB(dbSize, expiresSize uint64) error {
	if err := w.writeByte(opResizeDB); err != nil {
		return err
	}
	if err := w.writeLength(dbSize); err != nil {
		return err
	}
	return w.writeLength(expiresSize)
}

func (w *RDBWriter) WriteKeyValue(key string, obj *store.RedisObject, expiry *time.Time) error {

	if expiry != nil && !expiry.IsZero() {
		if err := w.writeByte(opExpireMS); err != nil {
			return err
		}
		ms := expiry.UnixMilli()
		if err := binary.Write(w.writer, binary.LittleEndian, ms); err != nil {
			return err
		}
	}

	if err := w.writeType(obj.Type); err != nil {
		return err
	}
	if err := w.writeString(key); err != nil {
		return err
	}

	return w.writeValue(obj)
}

func (w *RDBWriter) WriteEOF() error {
	if err := w.writeByte(opEOF); err != nil {
		return err
	}

	checksum := make([]byte, 8)
	_, err := w.writer.Write(checksum)
	return err
}

func (w *RDBWriter) writeByte(b byte) error {
	_, err := w.writer.Write([]byte{b})
	return err
}

func (w *RDBWriter) writeType(objType store.ObjectType) error {
	var typeCode byte
	switch objType {
	case store.ObjString:
		typeCode = typeString
	case store.ObjList:
		typeCode = typeQuick
	case store.ObjHash:
		typeCode = typeHash
	case store.ObjSet:
		typeCode = typeSet
	case store.ObjZSet:
		typeCode = typeZSet2
	default:
		return fmt.Errorf("unknown object type: %v", objType)
	}
	return w.writeByte(typeCode)
}

func (w *RDBWriter) writeLength(length uint64) error {
	if length < 64 {

		return w.writeByte(byte(length))
	} else if length < 16384 {

		b1 := byte(0x40 | (length >> 8))
		b2 := byte(length & 0xFF)
		if err := w.writeByte(b1); err != nil {
			return err
		}
		return w.writeByte(b2)
	} else {

		if err := w.writeByte(0x80); err != nil {
			return err
		}
		return binary.Write(w.writer, binary.BigEndian, uint32(length))
	}
}

func (w *RDBWriter) writeString(s string) error {
	if err := w.writeLength(uint64(len(s))); err != nil {
		return err
	}
	_, err := w.writer.Write([]byte(s))
	return err
}

func (w *RDBWriter) writeValue(obj *store.RedisObject) error {
	switch obj.Type {
	case store.ObjString:
		return w.writeStringValue(obj)
	case store.ObjList:
		return w.writeListValue(obj)
	case store.ObjHash:
		return w.writeHashValue(obj)
	case store.ObjSet:
		return w.writeSetValue(obj)
	case store.ObjZSet:
		return w.writeZSetValue(obj)
	default:
		return fmt.Errorf("unsupported type: %v", obj.Type)
	}
}

func (w *RDBWriter) writeStringValue(obj *store.RedisObject) error {
	if obj.Ptr == nil {
		return w.writeString("")
	}

	var str string
	switch obj.Encoding {
	case store.EncodingInt:

		num, ok := obj.Ptr.(int64)
		if !ok {
			return fmt.Errorf("invalid integer encoding")
		}
		str = fmt.Sprintf("%d", num)
	case store.EncodingEmbstr, store.EncodingRaw:

		var ok bool
		str, ok = obj.Ptr.(string)
		if !ok {
			return fmt.Errorf("invalid string value")
		}
	default:
		return fmt.Errorf("unknown string encoding: %v", obj.Encoding)
	}

	return w.writeString(str)
}

func (w *RDBWriter) writeListValue(obj *store.RedisObject) error {
	list, ok := obj.Ptr.(*store.Quicklist)
	if !ok {
		return fmt.Errorf("invalid list value")
	}

	elements := list.Range(0, -1)
	if err := w.writeLength(uint64(len(elements))); err != nil {
		return err
	}

	for _, elem := range elements {
		if err := w.writeString(elem); err != nil {
			return err
		}
	}
	return nil
}

func (w *RDBWriter) writeHashValue(obj *store.RedisObject) error {
	hash, ok := obj.Ptr.(*store.HashTable)
	if !ok {
		return fmt.Errorf("invalid hash value")
	}

	fields := hash.GetAll()
	if err := w.writeLength(uint64(len(fields))); err != nil {
		return err
	}

	for field, value := range fields {
		if err := w.writeString(field); err != nil {
			return err
		}
		if err := w.writeString(value); err != nil {
			return err
		}
	}
	return nil
}

func (w *RDBWriter) writeSetValue(obj *store.RedisObject) error {
	set, ok := obj.Ptr.(*store.Set)
	if !ok {
		return fmt.Errorf("invalid set value")
	}

	members := set.Members()
	if err := w.writeLength(uint64(len(members))); err != nil {
		return err
	}

	for _, member := range members {
		if err := w.writeString(member); err != nil {
			return err
		}
	}
	return nil
}

func (w *RDBWriter) writeZSetValue(obj *store.RedisObject) error {
	zset, ok := obj.Ptr.(*store.ZSet)
	if !ok {
		return fmt.Errorf("invalid zset value")
	}

	members := zset.Range(0, -1)
	if err := w.writeLength(uint64(len(members))); err != nil {
		return err
	}

	for _, m := range members {

		if err := w.writeString(m.Member); err != nil {
			return err
		}

		if err := binary.Write(w.writer, binary.LittleEndian, m.Score); err != nil {
			return err
		}
	}
	return nil
}

func SaveRDB(filepath string, s *store.Store) error {

	tmpFile := filepath + ".tmp"
	file, err := os.Create(tmpFile)
	if err != nil {
		return fmt.Errorf("failed to create RDB file: %w", err)
	}
	defer file.Close()

	writer := NewRDBWriter(file)

	if err := writer.WriteHeader(); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	if err := writer.WriteAuxField("redis-ver", "7.0.0"); err != nil {
		return err
	}
	if err := writer.WriteAuxField("redis-bits", "64"); err != nil {
		return err
	}
	if err := writer.WriteAuxField("ctime", fmt.Sprintf("%d", time.Now().Unix())); err != nil {
		return err
	}

	if err := writer.WriteSelectDB(0); err != nil {
		return err
	}

	data, expires := s.Snapshot()

	if err := writer.WriteResizeDB(uint64(len(data)), uint64(len(expires))); err != nil {
		return err
	}

	for key, obj := range data {
		var expiry *time.Time
		if exp, ok := expires[key]; ok {
			expiry = &exp
		}
		if err := writer.WriteKeyValue(key, obj, expiry); err != nil {
			return fmt.Errorf("failed to write key %s: %w", key, err)
		}
	}

	if err := writer.WriteEOF(); err != nil {
		return fmt.Errorf("failed to write EOF: %w", err)
	}

	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}

	file.Close()

	if err := os.Rename(tmpFile, filepath); err != nil {
		return fmt.Errorf("failed to rename RDB file: %w", err)
	}

	return nil
}

type RDBReader struct {
	reader io.Reader
}

func NewRDBReader(r io.Reader) *RDBReader {
	return &RDBReader{reader: r}
}

func (r *RDBReader) readByte() (byte, error) {
	buf := make([]byte, 1)
	_, err := io.ReadFull(r.reader, buf)
	return buf[0], err
}

func (r *RDBReader) readBytes(n int) ([]byte, error) {
	buf := make([]byte, n)
	_, err := io.ReadFull(r.reader, buf)
	return buf, err
}

func (r *RDBReader) readLength() (uint32, bool, error) {
	b, err := r.readByte()
	if err != nil {
		return 0, false, err
	}

	lenType := (b & 0xC0) >> 6

	switch lenType {
	case 0:
		return uint32(b & 0x3F), false, nil
	case 1:
		nextByte, err := r.readByte()
		if err != nil {
			return 0, false, err
		}
		return uint32(b&0x3F)<<8 | uint32(nextByte), false, nil
	case 2:
		buf, err := r.readBytes(4)
		if err != nil {
			return 0, false, err
		}
		return binary.BigEndian.Uint32(buf), false, nil
	case 3:
		return uint32(b & 0x3F), true, nil
	}

	return 0, false, fmt.Errorf("invalid length encoding")
}

func (r *RDBReader) readString() (string, error) {
	length, special, err := r.readLength()
	if err != nil {
		return "", err
	}

	if special {

		switch length {
		case 0:
			b, err := r.readByte()
			return fmt.Sprintf("%d", int8(b)), err
		case 1:
			buf, err := r.readBytes(2)
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("%d", int16(binary.LittleEndian.Uint16(buf))), nil
		case 2:
			buf, err := r.readBytes(4)
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("%d", int32(binary.LittleEndian.Uint32(buf))), nil
		default:
			return "", fmt.Errorf("unknown special encoding: %d", length)
		}
	}

	buf, err := r.readBytes(int(length))
	if err != nil {
		return "", err
	}
	return string(buf), nil
}

func LoadRDB(filepath string, st *store.Store) error {
	file, err := os.Open(filepath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to open RDB file: %w", err)
	}
	defer file.Close()

	reader := NewRDBReader(file)

	header, err := reader.readBytes(9)
	if err != nil {
		return fmt.Errorf("failed to read header: %w", err)
	}
	expectedHeader := fmt.Sprintf("REDIS%04d", RDBVersion)
	if string(header) != expectedHeader {
		return fmt.Errorf("invalid RDB header: %s", string(header))
	}

	var currentDB uint32 = 0
	var expireTime *time.Time

	for {
		opcode, err := reader.readByte()
		if err != nil {
			return fmt.Errorf("failed to read opcode: %w", err)
		}

		switch opcode {
		case opEOF:

			_, err := reader.readBytes(8)
			if err != nil && err != io.EOF {
				return fmt.Errorf("failed to read CRC: %w", err)
			}
			return nil

		case opSelectDB:
			length, _, err := reader.readLength()
			if err != nil {
				return fmt.Errorf("failed to read DB number: %w", err)
			}
			currentDB = length
			if currentDB != 0 {
				return fmt.Errorf("only DB 0 is supported")
			}

		case opExpireTime:

			buf, err := reader.readBytes(4)
			if err != nil {
				return fmt.Errorf("failed to read expire time: %w", err)
			}
			seconds := int64(binary.LittleEndian.Uint32(buf))
			t := time.Unix(seconds, 0)
			expireTime = &t

		case opExpireMS:

			buf, err := reader.readBytes(8)
			if err != nil {
				return fmt.Errorf("failed to read expire time: %w", err)
			}
			ms := int64(binary.LittleEndian.Uint64(buf))
			t := time.Unix(ms/1000, (ms%1000)*1000000)
			expireTime = &t

		case opResizeDB:

			_, _, err := reader.readLength()
			if err != nil {
				return fmt.Errorf("failed to read DB size: %w", err)
			}
			_, _, err = reader.readLength()
			if err != nil {
				return fmt.Errorf("failed to read expire size: %w", err)
			}

		case opAux:

			_, err := reader.readString()
			if err != nil {
				return fmt.Errorf("failed to read aux key: %w", err)
			}
			_, err = reader.readString()
			if err != nil {
				return fmt.Errorf("failed to read aux value: %w", err)
			}

		default:

			if err := reader.readKeyValue(opcode, st, expireTime); err != nil {
				return fmt.Errorf("failed to read key-value: %w", err)
			}
			expireTime = nil
		}
	}
}

func (r *RDBReader) readKeyValue(valueType byte, st *store.Store, expireTime *time.Time) error {

	key, err := r.readString()
	if err != nil {
		return fmt.Errorf("failed to read key: %w", err)
	}

	var obj *store.RedisObject
	switch valueType {
	case typeString:
		obj, err = r.readStringObject()
	case typeList, typeQuick:
		obj, err = r.readListObject()
	case typeSet:
		obj, err = r.readSetObject()
	case typeHash:
		obj, err = r.readHashObject()
	case typeZSet, typeZSet2:
		obj, err = r.readZSetObject()
	default:
		return fmt.Errorf("unsupported value type: %d", valueType)
	}

	if err != nil {
		return fmt.Errorf("failed to read value: %w", err)
	}

	st.SetObject(key, obj)

	if expireTime != nil && expireTime.After(time.Now()) {
		st.SetObjectExpire(key, *expireTime)
	}

	return nil
}

func (r *RDBReader) readStringObject() (*store.RedisObject, error) {
	str, err := r.readString()
	if err != nil {
		return nil, err
	}

	obj := &store.RedisObject{
		Type: store.ObjString,
	}

	if len(str) <= 20 {
		if num, err := strconv.ParseInt(str, 10, 64); err == nil {
			obj.Encoding = store.EncodingInt
			obj.Ptr = num
			return obj, nil
		}
	}

	if len(str) <= 44 {
		obj.Encoding = store.EncodingEmbstr
	} else {
		obj.Encoding = store.EncodingRaw
	}
	obj.Ptr = str
	return obj, nil
}

func (r *RDBReader) readListObject() (*store.RedisObject, error) {
	length, _, err := r.readLength()
	if err != nil {
		return nil, err
	}

	list := store.NewQuicklist()
	for i := uint32(0); i < length; i++ {
		str, err := r.readString()
		if err != nil {
			return nil, err
		}
		list.PushTail(str)
	}

	return &store.RedisObject{
		Type:     store.ObjList,
		Encoding: store.EncodingQuicklist,
		Ptr:      list,
	}, nil
}

func (r *RDBReader) readSetObject() (*store.RedisObject, error) {
	length, _, err := r.readLength()
	if err != nil {
		return nil, err
	}

	set := store.NewSet()
	for i := uint32(0); i < length; i++ {
		member, err := r.readString()
		if err != nil {
			return nil, err
		}
		set.Add(member)
	}

	return &store.RedisObject{
		Type:     store.ObjSet,
		Encoding: store.EncodingHT,
		Ptr:      set,
	}, nil
}

func (r *RDBReader) readHashObject() (*store.RedisObject, error) {
	length, _, err := r.readLength()
	if err != nil {
		return nil, err
	}

	hash := store.NewHashTable()
	for i := uint32(0); i < length; i++ {
		field, err := r.readString()
		if err != nil {
			return nil, err
		}
		value, err := r.readString()
		if err != nil {
			return nil, err
		}
		hash.Set(field, value)
	}

	return &store.RedisObject{
		Type:     store.ObjHash,
		Encoding: store.EncodingHT,
		Ptr:      hash,
	}, nil
}

func (r *RDBReader) readZSetObject() (*store.RedisObject, error) {
	length, _, err := r.readLength()
	if err != nil {
		return nil, err
	}

	zset := store.NewZSet()
	for i := uint32(0); i < length; i++ {
		member, err := r.readString()
		if err != nil {
			return nil, err
		}

		buf, err := r.readBytes(8)
		if err != nil {
			return nil, err
		}
		score := float64frombits(binary.LittleEndian.Uint64(buf))

		zset.Add(score, member)
	}

	return &store.RedisObject{
		Type:     store.ObjZSet,
		Encoding: store.EncodingSkiplist,
		Ptr:      zset,
	}, nil
}

func float64frombits(b uint64) float64 {
	return *(*float64)(unsafe.Pointer(&b))
}
