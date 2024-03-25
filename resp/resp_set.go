package resp

import (
	"bamboo/db"
	"encoding/binary"
)

type setWrappedKey struct {
	key     []byte
	version int64
	member  []byte
}

/*
encode set key to []byte
key | version | member | member size ==> encoded key buf
*/
func (sk *setWrappedKey) encoder() []byte {
	buf := make([]byte, len(sk.key)+len(sk.member)+8+4)

	// key
	var index = 0
	copy(buf[index:index+len(sk.key)], sk.key)
	index += len(sk.key)

	// version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(sk.version))
	index += 8

	// member
	copy(buf[index:index+len(sk.member)], sk.member)
	index += len(sk.member)

	// member size
	binary.LittleEndian.PutUint32(buf[index:], uint32(len(sk.member)))

	return buf
}

/*
SAdd: add member to set
*/
func (rds *RedisStructure) SAdd(key, member []byte) (bool, error) {
	// 1. find metadata
	meta, err := rds.findMetadata(key, RedisSet)
	if err != nil {
		return false, err
	}

	// 2. wrap the key and encode
	sk := &setWrappedKey{
		key:     key,
		member:  member,
		version: meta.version,
	}

	var ok bool
	if _, err = rds.dataBase.Get(sk.encoder()); err == db.ErrKeyNotFound {
		// 不存在的话则更新
		wb := rds.dataBase.NewAtomicWrite(db.DefaultWriteOptions)
		meta.size++
		_ = wb.Put(key, meta.encodeMeta())
		_ = wb.Put(sk.encoder(), nil)
		if err = wb.Commit(); err != nil {
			return false, err
		}
		ok = true
	}

	return ok, nil
}

// judge if member is in set
func (rds *RedisStructure) SIsMember(key, member []byte) (bool, error) {
	meta, err := rds.findMetadata(key, RedisSet)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}

	sk := &setWrappedKey{
		key:     key,
		member:  member,
		version: meta.version,
	}

	_, err = rds.dataBase.Get(sk.encoder())
	if err != nil && err != db.ErrKeyNotFound {
		return false, err
	}
	if err == db.ErrKeyNotFound {
		return false, nil
	}
	return true, nil
}

// remove member from set
func (rds *RedisStructure) SRem(key, member []byte) (bool, error) {
	meta, err := rds.findMetadata(key, RedisSet)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}

	sk := &setWrappedKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	if _, err = rds.dataBase.Get(sk.encoder()); err == db.ErrKeyNotFound {
		return false, nil
	}

	wb := rds.dataBase.NewAtomicWrite(db.DefaultWriteOptions)
	meta.size--
	_ = wb.Put(key, meta.encodeMeta())
	_ = wb.Delete(sk.encoder())
	if err = wb.Commit(); err != nil {
		return false, err
	}
	return true, nil
}
