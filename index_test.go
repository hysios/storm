package storm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIndex(t *testing.T) {
	type User struct {
		ID       string
		Group    string `storm:"index"`
		Username string `storm:"unique"`
	}

	db, cleanup := createDB(t)
	defer cleanup()

	err := db.Save(&User{
		ID:       "100",
		Group:    "Staff",
		Username: "john",
	})
	assert.NoError(t, err)

	err = db.Save(&User{
		ID:       "101",
		Group:    "Staff",
		Username: "jack",
	})
	assert.NoError(t, err)

	err = db.Save(&User{
		ID:       "102",
		Group:    "Admin",
		Username: "paul",
	})
	assert.NoError(t, err)

	ids, err := db.Index("User", "Group", "Staff")
	assert.NoError(t, err)
	assert.Len(t, ids, 2)
	assert.Equal(t, []byte("100"), ids[0])
	assert.Equal(t, []byte("101"), ids[1])

	ids, err = db.Index("User", "Username", "jack")
	assert.NoError(t, err)
	assert.Len(t, ids, 1)
	assert.Equal(t, []byte("101"), ids[0])
}
