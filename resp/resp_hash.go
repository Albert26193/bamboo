package resp

import (
	"bamboo/db"
	"encoding/binary"
)

/*
key + version + field ==> value
hash internal key:
+----------------+----------------+----------------+
|      key       |    version     |     field      |
+----------------+----------------+----------------+
*/
type hashWrappedKey struct {
	key     []byte
	version int64
	field   []byte
}

/*
encode hash key to []byte
why do this? because we need to store and save space
*/
func (hk *hashWrappedKey) encoder() []byte {
	buf := make([]byte, len(hk.key)+len(hk.field)+8)
	// key
	var index = 0
	copy(buf[index:index+len(hk.key)], hk.key)
	index += len(hk.key)

	// version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(hk.version))
	index += 8

	// field
	copy(buf[index:], hk.field)

	return buf
}

/*
1. find metadata by key, if not exist, then create a new meta
2. wrap the key and encode
3. check if exist
4. if not exist, then add
return: true: add new field, false: update field
*/
func (rds *RedisStructure) HSet(key, field, value []byte) (bool, error) {
	// find metadata
	meta, err := rds.findMetadata(key, RedisHash)
	if err != nil {
		return false, err
	}

	// wrap the key and encode
	hk := &hashWrappedKey{
		key:     key,
		version: meta.version,
		field:   field,
	}
	encodedKey := hk.encoder()

	// check if exist
	var exist = true
	if _, err = rds.dataBase.Get(encodedKey); err == db.ErrKeyNotFound {
		exist = false
	}

	wa := rds.dataBase.NewAtomicWrite(db.DefaultWriteOptions)

	// if not exist, then add
	// 1. key --> meta
	// 2. encodedKey --> value
	if !exist {
		meta.size++
		_ = wa.Put(key, meta.encodeMeta())
	}
	_ = wa.Put(encodedKey, value)
	if err = wa.Commit(); err != nil {
		return false, err
	}
	return !exist, nil
}

/*
1. find metadata by key
2. wrap the key and encode
3. get value by encoded key
return value, error
*/
func (rds *RedisStructure) HGet(key, field []byte) ([]byte, error) {
	meta, err := rds.findMetadata(key, RedisHash)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		return nil, nil
	}

	hk := &hashWrappedKey{
		key:     key,
		version: meta.version,
		field:   field,
	}

	// encode key --> value
	encodedKey := hk.encoder()
	return rds.dataBase.Get(encodedKey)
}

/*
1. find metadata by key
2. wrap the key and encode
3. check if exist
4. if exist, then delete
return: true: delete success, false: not exist
*/
func (rds *RedisStructure) HDel(key, field []byte) (bool, error) {
	meta, err := rds.findMetadata(key, RedisHash)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}

	hk := &hashWrappedKey{
		key:     key,
		version: meta.version,
		field:   field,
	}
	encodedKey := hk.encoder()

	// check if exist
	var exist = true
	if _, err = rds.dataBase.Get(encodedKey); err == db.ErrKeyNotFound {
		exist = false
	}

	if exist {
		wb := rds.dataBase.NewAtomicWrite(db.DefaultWriteOptions)
		meta.size--
		_ = wb.Put(key, meta.encodeMeta())
		_ = wb.Delete(encodedKey)
		if err = wb.Commit(); err != nil {
			return false, err
		}
	}

	return exist, nil
}
