package resp

import (
	"bamboo/db"
	"bamboo/db/utils"
	"encoding/binary"
)

type zsetWrappedKey struct {
	key     []byte
	member  []byte
	version int64
	score   float64
}

// key | version | member --> score
func (zk *zsetWrappedKey) encodeWithMember() []byte {
	const versionSize = 8
	buf := make([]byte, len(zk.key)+len(zk.member)+versionSize)

	// key
	var index = 0
	copy(buf[index:index+len(zk.key)], zk.key)
	index += len(zk.key)

	// version
	binary.LittleEndian.PutUint64(buf[index:index+versionSize], uint64(zk.version))
	index += versionSize

	// member
	copy(buf[index:], zk.member)

	return buf
}

// key | version | score | member | member size --> encoded key buf
func (zk *zsetWrappedKey) encodeWithScore() []byte {
	const versionSize = 8
	scoreBuf := utils.Float64ToBytes(zk.score)
	buf := make([]byte, len(zk.key)+len(zk.member)+len(scoreBuf)+versionSize+4)

	// key
	var index = 0
	copy(buf[index:index+len(zk.key)], zk.key)
	index += len(zk.key)

	// version
	binary.LittleEndian.PutUint64(buf[index:index+versionSize], uint64(zk.version))
	index += versionSize

	// score
	copy(buf[index:index+len(scoreBuf)], scoreBuf)
	index += len(scoreBuf)

	// member
	copy(buf[index:index+len(zk.member)], zk.member)
	index += len(zk.member)

	// member size
	binary.LittleEndian.PutUint32(buf[index:], uint32(len(zk.member)))

	return buf
}

func (rds *RedisStructure) ZAdd(key []byte, score float64, member []byte) (bool, error) {
	meta, err := rds.findMetadata(key, RedisZset)
	if err != nil {
		return false, err
	}

	// wrap key
	zk := &zsetWrappedKey{
		key:     key,
		version: meta.version,
		score:   score,
		member:  member,
	}

	var exist = true

	// check if exist
	value, err := rds.dataBase.Get(zk.encodeWithMember())
	if err != nil && err != db.ErrKeyNotFound {
		return false, err
	}
	if err == db.ErrKeyNotFound {
		exist = false
	}
	if exist {
		if score == utils.BytesToFloat64(value) {
			return false, nil
		}
	}

	// update meta
	wb := rds.dataBase.NewAtomicWrite(db.DefaultWriteOptions)
	if !exist {
		meta.size++
		_ = wb.Put(key, meta.encodeMeta())
	}
	if exist {
		oldKey := &zsetWrappedKey{
			key:     key,
			version: meta.version,
			member:  member,
			score:   utils.BytesToFloat64(value),
		}
		_ = wb.Delete(oldKey.encodeWithScore())
	}
	_ = wb.Put(zk.encodeWithMember(), utils.Float64ToBytes(score))
	_ = wb.Put(zk.encodeWithScore(), nil)
	if err = wb.Commit(); err != nil {
		return false, err
	}

	return !exist, nil
}

func (rds *RedisStructure) ZScore(key []byte, member []byte) (float64, error) {
	meta, err := rds.findMetadata(key, RedisZset)
	if err != nil {
		return -1, err
	}
	if meta.size == 0 {
		return -1, nil
	}

	// wrap key
	zk := &zsetWrappedKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	value, err := rds.dataBase.Get(zk.encodeWithMember())
	if err != nil {
		return -1, err
	}

	return utils.BytesToFloat64(value), nil
}
