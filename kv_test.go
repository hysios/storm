package storm

import (
	"fmt"
	"net/mail"
	"testing"
	"time"

	"github.com/asdine/storm/codec"
	"github.com/asdine/storm/codec/gob"
	"github.com/asdine/storm/codec/json"
	"github.com/boltdb/bolt"
	"github.com/stretchr/testify/assert"
)

func TestGet(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	err := db.Set("trash", 10, 100)
	assert.NoError(t, err)

	var nb int
	err = db.Get("trash", 10, &nb)
	assert.NoError(t, err)
	assert.Equal(t, 100, nb)

	tm := time.Now()
	err = db.Set("logs", tm, "I'm hungry")
	assert.NoError(t, err)

	var message string
	err = db.Get("logs", tm, &message)
	assert.NoError(t, err)
	assert.Equal(t, "I'm hungry", message)

	var hand int
	err = db.Get("wallet", "100 bucks", &hand)
	assert.Equal(t, ErrNotFound, err)

	err = db.Set("wallet", "10 bucks", 10)
	assert.NoError(t, err)

	err = db.Get("wallet", "100 bucks", &hand)
	assert.Equal(t, ErrNotFound, err)

	err = db.Get("logs", tm, nil)
	assert.Equal(t, ErrPtrNeeded, err)

	err = db.Get("", nil, nil)
	assert.Equal(t, ErrPtrNeeded, err)

	err = db.Get("", "100 bucks", &hand)
	assert.Equal(t, ErrNotFound, err)
}

func TestSet(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	err := db.Set("b1", 10, 10)
	assert.NoError(t, err)
	err = db.Set("b1", "best friend's mail", &mail.Address{Name: "Gandalf", Address: "gandalf@lorien.ma"})
	assert.NoError(t, err)
	err = db.Set("b2", []byte("i'm already a slice of bytes"), "a value")
	assert.NoError(t, err)
	err = db.Set("b2", []byte("i'm already a slice of bytes"), nil)
	assert.NoError(t, err)
	err = db.Set("b1", 0, 100)
	assert.NoError(t, err)
	err = db.Set("b1", nil, 100)
	assert.Error(t, err)

	db.Bolt.View(func(tx *bolt.Tx) error {
		b1 := tx.Bucket([]byte("b1"))
		assert.NotNil(t, b1)
		b2 := tx.Bucket([]byte("b2"))
		assert.NotNil(t, b2)

		k1, err := toBytes(10, gob.Codec)
		assert.NoError(t, err)
		val := b1.Get(k1)
		assert.NotNil(t, val)

		k2 := []byte("best friend's mail")
		val = b1.Get(k2)
		assert.NotNil(t, val)

		k3, err := toBytes(0, gob.Codec)
		assert.NoError(t, err)
		val = b1.Get(k3)
		assert.NotNil(t, val)

		return nil
	})

	err = db.Set("", 0, 100)
	assert.Error(t, err)

	err = db.Set("b", nil, 100)
	assert.Error(t, err)

	err = db.Set("b", 10, nil)
	assert.NoError(t, err)

	err = db.Set("b", nil, nil)
	assert.Error(t, err)
}

func TestDelete(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	err := db.Set("files", "myfile.csv", "a,b,c,d")
	assert.NoError(t, err)
	err = db.Delete("files", "myfile.csv")
	assert.NoError(t, err)
	err = db.Delete("files", "myfile.csv")
	assert.NoError(t, err)
	err = db.Delete("i don't exist", "myfile.csv")
	assert.Equal(t, ErrNotFound, err)
	err = db.Delete("", nil)
	assert.Equal(t, ErrNotFound, err)
}

func TestGetAll(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	var ids [][]byte
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("k%d", i)
		ids = append(ids, []byte(key))
		err := db.Set("bucket", key, i)
		assert.NoError(t, err)
	}

	results := make([]int, 10)
	err := db.GetAll("bucket", ids, func(idx int) interface{} {
		return &results[idx]
	})
	assert.NoError(t, err)
	assert.Equal(t, 0, results[0])
	assert.Equal(t, 9, results[9])
}

func TestGetAllWithIndex(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	for i := 0; i < 10; i++ {
		w := User{Name: "John", ID: i + 1, Slug: fmt.Sprintf("John%d", i+1)}
		err := db.Save(&w)
		assert.NoError(t, err)
	}

	ids, err := db.Index("User", "Name", "John")
	results := make([]User, 10)
	err = db.GetAll("User", ids, func(idx int) interface{} {
		r := &results[idx]
		return r
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, results[0].ID)
	assert.Equal(t, 10, results[9].ID)
}

func benchmarkGetAll100(b *testing.B, codec codec.EncodeDecoder) {
	db, cleanup := createDB(b, Codec(codec))
	defer cleanup()

	var ids [][]byte
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("k%d", i)
		ids = append(ids, []byte(key))
		err := db.Set("bucket", key, i)
		if err != nil {
			b.Error(err)
		}
	}

	results := make([]int, 100)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		err := db.GetAll("bucket", ids, func(idx int) interface{} {
			return &results[idx]
		})
		if err != nil {
			b.Error(err)
		}
	}
}

func benchmarkGetAll100WithIndex(b *testing.B, codec codec.EncodeDecoder) {
	db, cleanup := createDB(b, Codec(codec))
	defer cleanup()

	for i := 0; i < 200; i++ {
		var w User

		if i%2 == 0 {
			w.Name = "John"
		} else {
			w.Name = "Jack"
		}

		w.ID = i + 1
		err := db.Save(&w)
		if err != nil {
			b.Error(err)
		}
	}

	users := make([]User, 100)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		ids, err := db.Index("User", "Name", "John")
		if err != nil {
			b.Error(err)
		}

		err = db.GetAll("User", ids, func(idx int) interface{} {
			return &users[idx]
		})
		if err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkGetAll100JSON(b *testing.B) {
	benchmarkGetAll100(b, json.Codec)
}

func BenchmarkGetAll100GOB(b *testing.B) {
	benchmarkGetAll100(b, gob.Codec)
}

func BenchmarkGetAll100WithIndexJSON(b *testing.B) {
	benchmarkGetAll100WithIndex(b, json.Codec)
}

func BenchmarkGetAll100WithIndexGOB(b *testing.B) {
	benchmarkGetAll100WithIndex(b, gob.Codec)
}
