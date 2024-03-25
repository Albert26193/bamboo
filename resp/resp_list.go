package resp

import (
	"bamboo/db"
	"encoding/binary"
)

type listWrappedKey struct {
	key     []byte
	version int64
	index   uint64
}

/*
key + version + index ==> value
*/
func (lk *listWrappedKey) encoder() []byte {
	buf := make([]byte, len(lk.key)+8+8)

	// key
	var index = 0
	copy(buf[index:index+len(lk.key)], lk.key)
	index += len(lk.key)

	// version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(lk.version))
	index += 8

	// index
	binary.LittleEndian.PutUint64(buf[index:], lk.index)

	return buf
}

// insert into head
func (rds *RedisStructure) LPush(key, element []byte) (uint32, error) {
	return rds.pushInner(key, element, true)
}

// push into tail
func (rds *RedisStructure) RPush(key, element []byte) (uint32, error) {
	return rds.pushInner(key, element, false)
}

// pop from head
func (rds *RedisStructure) LPop(key []byte) ([]byte, error) {
	return rds.popInner(key, true)
}

// pop from tail
func (rds *RedisStructure) RPop(key []byte) ([]byte, error) {
	return rds.popInner(key, false)
}

// push element into list
// isLeft: true: push into head, false: push into tail
func (rds *RedisStructure) pushInner(key, element []byte, isLeft bool) (uint32, error) {
	// 1. check metadata
	meta, err := rds.findMetadata(key, RedisList)
	if err != nil {
		return 0, err
	}

	// get the wrappedKey
	lk := &listWrappedKey{
		key:     key,
		version: meta.version,
	}
	if isLeft {
		lk.index = meta.head - 1
	} else {
		lk.index = meta.tail
	}

	// update metadata and element
	wb := rds.dataBase.NewAtomicWrite(db.DefaultWriteOptions)
	meta.size++
	if isLeft {
		meta.head--
	} else {
		meta.tail++
	}
	_ = wb.Put(key, meta.encodeMeta())
	_ = wb.Put(lk.encoder(), element)
	if err = wb.Commit(); err != nil {
		return 0, err
	}

	return meta.size, nil
}

// pop element from list
// isLeft: true: pop from head, false: pop from tail
func (rds *RedisStructure) popInner(key []byte, isLeft bool) ([]byte, error) {
	// 1. check metadata
	meta, err := rds.findMetadata(key, RedisList)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		return nil, nil
	}

	// 2. get the element
	lk := &listWrappedKey{
		key:     key,
		version: meta.version,
	}
	if isLeft {
		lk.index = meta.head
	} else {
		lk.index = meta.tail - 1
	}

	element, err := rds.dataBase.Get(lk.encoder())
	if err != nil {
		return nil, err
	}

	// update metadata
	meta.size--
	if isLeft {
		meta.head++
	} else {
		meta.tail--
	}
	if err = rds.dataBase.Put(key, meta.encodeMeta()); err != nil {
		return nil, err
	}

	return element, nil
}
