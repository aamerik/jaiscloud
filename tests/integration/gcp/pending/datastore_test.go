//go:build gcp_pending

package tests

import (
	"context"
	"testing"

	"jaiscloud-gcp-pending-test/internal/testutil"

	"cloud.google.com/go/datastore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type Task struct {
	Description string
	Done        bool
	Priority    int
}

func TestDatastore(t *testing.T) {
	ctx := context.Background()
	client := testutil.DatastoreClient(ctx)
	defer client.Close()

	suffix := uniqueName("go-ds")

	t.Run("PutAndGet", func(t *testing.T) {
		key := datastore.NameKey("Task", "task-"+suffix, nil)
		task := &Task{Description: "Buy groceries", Done: false, Priority: 4}

		_, err := client.Put(ctx, key, task)
		require.NoError(t, err)

		t.Cleanup(func() { client.Delete(ctx, key) })

		var got Task
		err = client.Get(ctx, key, &got)
		require.NoError(t, err)
		assert.Equal(t, "Buy groceries", got.Description)
		assert.False(t, got.Done)
		assert.Equal(t, 4, got.Priority)
	})

	t.Run("Query", func(t *testing.T) {
		key1 := datastore.NameKey("Task", "task1-"+suffix, nil)
		key2 := datastore.NameKey("Task", "task2-"+suffix, nil)

		client.Put(ctx, key1, &Task{Description: "Task A", Done: false})
		client.Put(ctx, key2, &Task{Description: "Task B", Done: true})

		t.Cleanup(func() {
			client.Delete(ctx, key1)
			client.Delete(ctx, key2)
		})

		var results []Task
		q := datastore.NewQuery("Task").FilterField("Done", "=", false)
		_, err := client.GetAll(ctx, q, &results)
		require.NoError(t, err)

		descriptions := make([]string, len(results))
		for i, r := range results {
			descriptions[i] = r.Description
		}
		assert.Contains(t, descriptions, "Task A")
	})

	t.Run("Update", func(t *testing.T) {
		key := datastore.NameKey("Task", "update-"+suffix, nil)
		_, err := client.Put(ctx, key, &Task{Description: "Original", Done: false})
		require.NoError(t, err)

		t.Cleanup(func() { client.Delete(ctx, key) })

		var task Task
		require.NoError(t, client.Get(ctx, key, &task))
		task.Done = true
		_, err = client.Put(ctx, key, &task)
		require.NoError(t, err)

		var updated Task
		require.NoError(t, client.Get(ctx, key, &updated))
		assert.True(t, updated.Done)
	})

	t.Run("Delete", func(t *testing.T) {
		key := datastore.NameKey("Task", "delete-"+suffix, nil)
		_, err := client.Put(ctx, key, &Task{Description: "Delete Me"})
		require.NoError(t, err)

		err = client.Delete(ctx, key)
		require.NoError(t, err)

		var task Task
		err = client.Get(ctx, key, &task)
		assert.ErrorIs(t, err, datastore.ErrNoSuchEntity)
	})

	t.Run("AllocateIDs", func(t *testing.T) {
		incomplete := datastore.IncompleteKey("Task", nil)
		keys, err := client.AllocateIDs(ctx, []*datastore.Key{incomplete})
		require.NoError(t, err)
		require.Len(t, keys, 1)
		assert.Greater(t, keys[0].ID, int64(0))
	})
}
