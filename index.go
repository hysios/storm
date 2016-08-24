package storm

import (
	"github.com/asdine/storm/index"
	"github.com/boltdb/bolt"
)

// Index returns indexed ids given a field and a value
func (n *node) Index(bucketName string, fieldName string, value interface{}) ([][]byte, error) {
	if n.tx != nil {
		return n.index(n.tx, bucketName, fieldName, value)
	}

	var ids [][]byte
	var err error
	err = n.s.Bolt.View(func(tx *bolt.Tx) error {
		ids, err = n.index(tx, bucketName, fieldName, value)
		return err
	})

	return ids, err
}

// Index returns indexed ids given a field and a value
func (n *node) index(tx *bolt.Tx, bucketName string, fieldName string, value interface{}) ([][]byte, error) {
	bucket := n.GetBucket(tx, bucketName)
	if bucket == nil {
		return nil, ErrNotFound
	}

	idx, err := getIndex(bucket, fieldName)
	if err != nil {
		return nil, err
	}

	val, err := toBytes(value, n.s.Codec)
	if err != nil {
		return nil, err
	}

	list, err := idx.All(val, nil)
	if err != nil {
		if err == index.ErrNotFound {
			return nil, ErrNotFound
		}
		return nil, err
	}

	return list, nil
}

// Index returns indexed ids given a field and a value
func (s *DB) Index(bucketName string, fieldName string, value interface{}) ([][]byte, error) {
	return s.root.Index(bucketName, fieldName, value)
}
