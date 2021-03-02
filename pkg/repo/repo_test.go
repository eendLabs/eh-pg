package repo

import (
	"context"
	"github.com/eendLabs/eh-pg/pkg/mocks"
	"github.com/jmoiron/sqlx"
	eh "github.com/looplab/eventhorizon"
	ehmocks "github.com/looplab/eventhorizon/mocks"
	"testing"
)

func TestReadRepoIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	config := &Config{}
	config.provideDefaults()
	config.TableName = "models"
	client, err := sqlx.Connect("postgres",
		config.DbConfig.GetConnString())
	if err != nil {
		t.Fatal(err)
	}

	var schema = `
	DROP TABLE IF EXISTS models;
	CREATE TABLE models (
	    id uuid primary key,
	    version integer,
	    content text,
	    created_at timestamp
	)
	`

	ctx := context.Background()
	client.MustExecContext(ctx, schema)

	r, err := NewRepoWithClient(config, client)
	if err != nil {
		t.Error("there should be no error:", err)
	}
	if r == nil {
		t.Error("there should be a repository")
	}

	r.SetEntityFactory(func() eh.Entity {
		return &mocks.Model{} //&mocks.Model{}
	})
	if r.Parent() != nil {
		t.Error("the parent repo should be nil")
	}

	customNamespaceCtx := eh.NewContextWithNamespace(ctx, "ns")

	defer r.Close(ctx)
	defer func() {
		if err = r.Clear(ctx); err != nil {
			t.Fatal("there should be no error:", err)
		}
		if err = r.Clear(customNamespaceCtx); err != nil {
			t.Fatal("there should be no error:", err)
		}
	}()

	AcceptanceTest(t, context.Background(), r)
	//extraRepoTests(t, context.Background(), r)
	//AcceptanceTest(t, customNamespaceCtx, r)
	//extraRepoTests(t, customNamespaceCtx, r)

}

/*
func extraRepoTests(t *testing.T, ctx context.Context, r *Repo) {
	// Insert a custom item.
	modelCustom := &mocks.Model{
		ID:        uuid.New(),
		Content:   "modelCustom",
		CreatedAt: time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC),
	}
	if err := r.Save(ctx, modelCustom); err != nil {
		t.Error("there should be no error:", err)
	}

	// FindCustom by content.
	result, err := r.FindCustom(ctx, func(ctx context.Context, c *mongo.Collection) (*mongo.Cursor, error) {
		return c.Find(ctx, bson.M{"content": "modelCustom"})
	})
	if len(result) != 1 {
		t.Error("there should be one item:", len(result))
	}
	if !reflect.DeepEqual(result[0], modelCustom) {
		t.Error("the item should be correct:", modelCustom)
	}

	// FindCustom with no query.
	result, err = r.FindCustom(ctx, func(ctx context.Context, c *mongo.Collection) (*mongo.Cursor, error) {
		return nil, nil
	})
	var repoErr eh.RepoError
	if !errors.As(err, &repoErr) || !errors.Is(err, ErrInvalidQuery) {
		t.Error("there should be a invalid query error:", err)
	}

	var count int64
	// FindCustom with query execution in the callback.
	_, err = r.FindCustom(ctx, func(ctx context.Context, c *mongo.Collection) (*mongo.Cursor, error) {
		if count, err = c.CountDocuments(ctx, bson.M{}); err != nil {
			t.Error("there should be no error:", err)
		}

		// Be sure to return nil to not execute the query again in FindCustom.
		return nil, nil
	})
	if !errors.As(err, &repoErr) || !errors.Is(err, ErrInvalidQuery) {
		t.Error("there should be a invalid query error:", err)
	}
	if count != 2 {
		t.Error("the count should be correct:", count)
	}

	modelCustom2 := &mocks.Model{
		ID:      uuid.New(),
		Content: "modelCustom2",
	}
	if err := r.Collection(ctx, func(ctx context.Context, c *mongo.Collection) error {
		_, err := c.InsertOne(ctx, modelCustom2)
		return err
	}); err != nil {
		t.Error("there should be no error:", err)
	}
	model, err := r.Find(ctx, modelCustom2.ID)
	if err != nil {
		t.Error("there should be no error:", err)
	}
	if !reflect.DeepEqual(model, modelCustom2) {
		t.Error("the item should be correct:", model)
	}

	// FindCustomIter by content.
	iter, err := r.FindCustomIter(ctx, func(ctx context.Context, c *mongo.Collection) (*mongo.Cursor, error) {
		return c.Find(ctx, bson.M{"content": "modelCustom"})
	})
	if err != nil {
		t.Error("there should be no error:", err)
	}

	if iter.Next(ctx) != true {
		t.Error("the iterator should have results")
	}
	if !reflect.DeepEqual(iter.Value(), modelCustom) {
		t.Error("the item should be correct:", modelCustom)
	}
	if iter.Next(ctx) == true {
		t.Error("the iterator should have no results")
	}
	err = iter.Close(ctx)
	if err != nil {
		t.Error("there should be no error:", err)
	}

}
*/

func TestRepository(t *testing.T) {
	if r := Repository(nil); r != nil {
		t.Error("the parent repository should be nil:", r)
	}

	inner := &ehmocks.Repo{}
	if r := Repository(inner); r != nil {
		t.Error("the parent repository should be nil:", r)
	}

	// Local Mongo testing with Docker
	config := &Config{}
	r, err := NewRepo(config)
	if err != nil {
		t.Error("there should be no error:", err)
	}
	defer r.Close(context.Background())

	outer := &ehmocks.Repo{ParentRepo: r}
	if r := Repository(outer); r != r {
		t.Error("the parent repository should be correct:", r)
	}
}
