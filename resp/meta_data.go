package resp

import (
	"bamboo/db"
	"encoding/binary"
	"math"
	"time"
)

/*
key:
+----------------+-----------------+---------------+-----------+
|  data type(1)  |   expire(1-10) | version(1-10) |  size(1-5)|
+----------------+----------------+---------------+-----------+
*/
const (
	maxMetaLen       = 1 + binary.MaxVarintLen64*2 + binary.MaxVarintLen32
	extraListMetaLen = binary.MaxVarintLen64 * 2
	initialListMark  = math.MaxUint64 / 2
)

// metaData
type metaData struct {
	dataType byte
	version  int64
	expire   int64
	size     uint32
	head     uint64
	tail     uint64
}

func (md *metaData) encodeMeta() []byte {
	var size = maxMetaLen
	if md.dataType == RedisList {
		size += extraListMetaLen
	}

	buf := make([]byte, size)

	buf[0] = md.dataType
	var index = 1
	index += binary.PutVarint(buf[index:], md.expire)
	index += binary.PutVarint(buf[index:], md.version)
	index += binary.PutVarint(buf[index:], int64(md.size))

	if md.dataType == RedisList {
		index += binary.PutUvarint(buf[index:], md.head)
		index += binary.PutUvarint(buf[index:], md.tail)
	}

	return buf[:index]
}

func decodeMeta(buf []byte) *metaData {
	dataType := buf[0]

	var index = 1
	// put expire
	expire, n := binary.Varint(buf[index:])
	index += n

	// put version
	version, n := binary.Varint(buf[index:])
	index += n

	// put size
	size, n := binary.Varint(buf[index:])
	index += n

	var head uint64 = 0
	var tail uint64 = 0
	if dataType == RedisList {
		head, n = binary.Uvarint(buf[index:])
		index += n
		tail, _ = binary.Uvarint(buf[index:])
	}

	return &metaData{
		dataType: dataType,
		expire:   expire,
		version:  version,
		size:     uint32(size),
		head:     head,
		tail:     tail,
	}
}

/*
1. find metadata by key:
  - if not exist, or
  - if expire
  - if type not match
  - if version not match
  - create a new meta, and set version to now, and size to 0

other wrapped structure will judge the meta status, if size == 0, then abandon the operation
2. if eligible, return meta
*/
func (rds *RedisStructure) findMetadata(key []byte, dataType redisStructType) (*metaData, error) {
	metaBuf, err := rds.dataBase.Get(key)
	if err != nil && err != db.ErrKeyNotFound {
		return nil, err
	}

	var meta *metaData
	var exist = true
	if err == db.ErrKeyNotFound {
		exist = false
	} else {
		meta = decodeMeta(metaBuf)
		// if type not match
		if meta.dataType != dataType {
			return nil, ErrTypeOperation
		}
		// if expire
		if meta.expire != 0 && meta.expire <= time.Now().UnixNano() {
			exist = false
		}
	}

	// if not has meta, then create a new meta
	if !exist {
		meta = &metaData{
			dataType: dataType,
			expire:   0,
			version:  time.Now().UnixNano(),
			size:     0,
		}
		if dataType == RedisList {
			meta.head = initialListMark
			meta.tail = initialListMark
		}
	}
	return meta, nil
}
