package repo

import (
	"context"
	"github.com/eendLabs/eh-pg/pkg/mocks"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	eh "github.com/looplab/eventhorizon"
)

// AcceptanceTest is the acceptance test that all implementations of Repo
// should pass. It should manually be called from a test case in each
// implementation:
//
//   func TestRepo(t *testing.T) {
//       ctx := context.Background() // Or other when testing namespaces.
//       store := NewRepo()
//       repo.AcceptanceTest(t, ctx, store)
//   }
//
func AcceptanceTest(t *testing.T, ctx context.Context, r eh.ReadWriteRepo) {
	// Find non-existing item.
	entity, err := r.Find(ctx, uuid.New())
	if rrErr, ok := err.(eh.RepoError); !ok || rrErr.Err != eh.ErrEntityNotFound {
		t.Error("there should be a ErrEntityNotFound error:", err)
	}
	if entity != nil {
		t.Error("there should be no entity:", entity)
	}

	// FindAll with no items.
	result, err := r.FindAll(ctx)
	if err != nil {
		t.Error("there should be no error:", err)
	}
	if len(result) != 0 {
		t.Error("there should be no items:", len(result))
	}

	// Save model without ID.
	entityMissingID := &mocks.Model{
		Content:   "entity1",
		CreatedAt: time.Date(2009, time.November, 10, 23, 0, 0, 0, time.FixedZone("", 0)),
	}
	err = r.Save(ctx, entityMissingID)
	if rrErr, ok := err.(eh.RepoError); !ok || rrErr.BaseErr != eh.ErrMissingEntityID {
		t.Error("there should be a ErrMissingEntityID error:", err)
	}

	// Save and find one item.
	entity1 := &mocks.Model{
		ID:        uuid.New(),
		Content:   "entity1",
		CreatedAt: time.Date(2009, time.November, 10, 23, 0, 0, 0, time.FixedZone("", 0)),
	}
	if err = r.Save(ctx, entity1); err != nil {
		t.Error("there should be no error:", err)
	}
	entity, err = r.Find(ctx, entity1.ID)
	if err != nil {
		t.Error("there should be no error:", err)
	}
	if !reflect.DeepEqual(entity, entity1) {
		t.Error("the item should be correct:", entity)
	}

	// FindAll with one item.
	result, err = r.FindAll(ctx)
	if err != nil {
		t.Error("there should be no error:", err)
	}
	if len(result) != 1 {
		t.Error("there should be one item:", len(result))
	}
	if !reflect.DeepEqual(result, []eh.Entity{entity1}) {
		t.Error("the item should be correct:", entity1)
	}

	// Save and overwrite with same ID.
	entity1Alt := &mocks.Model{
		ID:        entity1.ID,
		Content:   "entity1Alt",
		CreatedAt: time.Date(2009, time.November, 10, 23, 0, 0, 0, time.FixedZone("", 0)),
	}
	if err = r.Save(ctx, entity1Alt); err != nil {
		t.Error("there should be no error:", err)
	}
	entity, err = r.Find(ctx, entity1Alt.ID)
	if err != nil {
		t.Error("there should be no error:", err)
	}
	if !reflect.DeepEqual(entity, entity1Alt) {
		t.Error("the item should be correct:", entity)
	}

	// Save with another ID.
	entity2 := &mocks.Model{
		ID:        uuid.New(),
		Content:   "entity2",
		CreatedAt: time.Date(2009, time.November, 10, 23, 0, 0, 0, time.FixedZone("", 0)),
	}
	if err = r.Save(ctx, entity2); err != nil {
		t.Error("there should be no error:", err)
	}
	entity, err = r.Find(ctx, entity2.ID)
	if err != nil {
		t.Error("there should be no error:", err)
	}
	if !reflect.DeepEqual(entity, entity2) {
		t.Error("the item should be correct:", entity)
	}

	// FindAll with two items, order should be preserved from insert.
	result, err = r.FindAll(ctx)
	if err != nil {
		t.Error("there should be no error:", err)
	}
	if len(result) != 2 {
		t.Error("there should be two items:", len(result))
	}
	// Retrieval in any order is accepted.
	if !reflect.DeepEqual(result, []eh.Entity{entity1Alt, entity2}) &&
		!reflect.DeepEqual(result, []eh.Entity{entity2, entity1Alt}) {
		t.Error("the items should be correct:", result)
	}

	// Remove item.
	if err := r.Remove(ctx, entity1Alt.ID); err != nil {
		t.Error("there should be no error:", err)
	}
	entity, err = r.Find(ctx, entity1Alt.ID)
	if rrErr, ok := err.(eh.RepoError); !ok || rrErr.Err != eh.ErrEntityNotFound {
		t.Error("there should be a ErrEntityNotFound error:", err)
	}
	if entity != nil {
		t.Error("there should be no entity:", entity)
	}

	// Remove non-existing item.
	err = r.Remove(ctx, entity1Alt.ID)
	if rrErr, ok := err.(eh.RepoError); !ok || rrErr.Err != eh.ErrEntityNotFound {
		t.Error("there should be a ErrEntityNotFound error:", err)
	}
}
