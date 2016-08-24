package storm

import (
	"reflect"

	"github.com/boltdb/bolt"
)

// Get a value from a bucket
func (n *node) Get(bucketName string, key interface{}, to interface{}) error {
	ref := reflect.ValueOf(to)

	if !ref.IsValid() || ref.Kind() != reflect.Ptr {
		return ErrPtrNeeded
	}

	id, err := toBytes(key, n.s.Codec)
	if err != nil {
		return err
	}

	if n.tx != nil {
		return n.get(n.tx, bucketName, id, to)
	}

	return n.s.Bolt.View(func(tx *bolt.Tx) error {
		return n.get(tx, bucketName, id, to)
	})
}

func (n *node) get(tx *bolt.Tx, bucketName string, id []byte, to interface{}) error {
	bucket := n.GetBucket(tx, bucketName)
	if bucket == nil {
		return ErrNotFound
	}

	raw := bucket.Get(id)
	if raw == nil {
		return ErrNotFound
	}

	return n.s.Codec.Decode(raw, to)
}

// Set a key/value pair into a bucket
func (n *node) Set(bucketName string, key interface{}, value interface{}) error {
	if key == nil {
		return ErrNilParam
	}

	id, err := toBytes(key, n.s.Codec)
	if err != nil {
		return err
	}

	var data []byte
	if value != nil {
		data, err = n.s.Codec.Encode(value)
		if err != nil {
			return err
		}
	}

	if n.tx != nil {
		return n.set(n.tx, bucketName, id, data)
	}

	return n.s.Bolt.Update(func(tx *bolt.Tx) error {
		return n.set(tx, bucketName, id, data)
	})
}

func (n *node) set(tx *bolt.Tx, bucketName string, id, data []byte) error {
	bucket, err := n.CreateBucketIfNotExists(tx, bucketName)
	if err != nil {
		return err
	}
	return bucket.Put(id, data)
}

// Delete deletes a key from a bucket
func (n *node) Delete(bucketName string, key interface{}) error {
	id, err := toBytes(key, n.s.Codec)
	if err != nil {
		return err
	}

	if n.tx != nil {
		return n.delete(n.tx, bucketName, id)
	}

	return n.s.Bolt.Update(func(tx *bolt.Tx) error {
		return n.delete(tx, bucketName, id)
	})
}

func (n *node) delete(tx *bolt.Tx, bucketName string, id []byte) error {
	bucket := n.GetBucket(tx, bucketName)
	if bucket == nil {
		return ErrNotFound
	}

	return bucket.Delete(id)
}

// GetAll values from a list of ids
func (n *node) GetAll(bucketName string, ids [][]byte, fn func(idx int) interface{}) error {
	if n.tx != nil {
		return n.getAll(n.tx, bucketName, ids, fn)
	}

	return n.s.Bolt.View(func(tx *bolt.Tx) error {
		return n.getAll(tx, bucketName, ids, fn)
	})
}

func (n *node) getAll(tx *bolt.Tx, bucketName string, ids [][]byte, fn func(idx int) interface{}) error {
	bucket := n.GetBucket(tx, bucketName)
	if bucket == nil {
		return ErrNotFound
	}

	for i, id := range ids {
		raw := bucket.Get(id)
		if raw == nil {
			return ErrNotFound
		}

		err := n.s.Codec.Decode(raw, fn(i))
		if err != nil {
			return err
		}
	}

	return nil
}

// Get a value from a bucket
func (s *DB) Get(bucketName string, key interface{}, to interface{}) error {
	return s.root.Get(bucketName, key, to)
}

// Set a key/value pair into a bucket
func (s *DB) Set(bucketName string, key interface{}, value interface{}) error {
	return s.root.Set(bucketName, key, value)
}

// Delete deletes a key from a bucket
func (s *DB) Delete(bucketName string, key interface{}) error {
	return s.root.Delete(bucketName, key)
}

// GetAll values from a list of ids
func (s *DB) GetAll(bucketName string, ids [][]byte, fn func(idx int) interface{}) error {
	return s.root.GetAll(bucketName, ids, fn)
}
