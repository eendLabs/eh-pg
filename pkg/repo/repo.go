package repo

import (
	"context"
	"errors"
	"github.com/go-pg/pg/v10"
	"github.com/google/uuid"
	eh "github.com/looplab/eventhorizon"
	"github.com/looplab/eventhorizon/mocks"
	"log"
	"os"
	"time"
)

var ErrCouldNotDialDB = errors.New("could not dial database")

// ErrCouldNotClearDB is when the database could not be cleared.
var ErrCouldNotClearDB = errors.New("could not clear database")

var ErrNoDBClient = errors.New("no database client")

// ErrModelNotSet is when an model factory is not set on the Repo.
var ErrModelNotSet = errors.New("model not set")

type Config struct {
	Addr     string
	Database string
	User     string
	Password string
}

func (c *Config) provideDefaults() {
	if c.Addr == "" {
		c.Addr = os.Getenv("POSTGRES_ADDR")
	}
	if c.Database == "" {
		c.Database = os.Getenv("POSTGRES_DB")
	}
	if c.User == "" {
		c.User = os.Getenv("POSTGRES_USER")
	}
	if c.Password == "" {
		c.Password = os.Getenv("POSTGRES_PASSWORD")
	}
}

type IModel struct {
	ID        uuid.UUID `pg:"id,type:uuid,pk"`
	Version   int       `pg:"version"`
	Content   string    `pg:"content,type:varchar(250)"`
	CreatedAt time.Time `pg:"created_at,type:timestamp"`
}

func(i IModel)   EntityID() uuid.UUID {
	return i.ID
}

type Repo struct {
	client    *pg.DB
	config    *Config
	factoryFn func() eh.Entity
}

func NewRepo(config *Config) (*Repo, error) {
	config.provideDefaults()

	client := pg.Connect(&pg.Options{
		Addr:     config.Addr,
		Database: config.Database,
		User:     config.User,
		Password: config.Password,
	})
	return NewRepoWithClient(config, client)

}

func NewRepoWithClient(config *Config, client *pg.DB) (*Repo, error) {
	if client == nil {
		return nil, ErrNoDBClient
	}

	r := &Repo{
		client: client,
		config: config,
	}

	return r, nil
}

// Parent implements the Parent method of the eventhorizon.ReadRepo interface.
func (r *Repo) Parent() eh.ReadRepo {
	return nil
}

func (r *Repo) Find(ctx context.Context, id uuid.UUID) (eh.Entity, error) {
	ns := eh.NamespaceFromContext(ctx)

	if r.factoryFn == nil {
		return nil, eh.RepoError{
			Err:       ErrModelNotSet,
			Namespace: ns,
		}
	}
	entity := r.factoryFn()
	err := r.client.
		//WithParam("namespace", ns).
		WithContext(ctx).
		Model(entity).
		Where("id = ?", id).
		Column("*").
		Select(entity)

	if err != nil {
		return nil, eh.RepoError{
			Err:       eh.ErrEntityNotFound,
			BaseErr:   err,
			Namespace: eh.NamespaceFromContext(ctx),
		}
	}

	return entity, nil
}

type X struct {
	mocks.Model
}
func (c X)  EntityID() uuid.UUID {
	return c.ID
}

// FindAll implements the FindAll method of the eventhorizon.ReadRepo interface.
func (r *Repo) FindAll(ctx context.Context) ([]eh.Entity, error) {
	ns := eh.NamespaceFromContext(ctx)

	if r.factoryFn == nil {
		return nil, eh.RepoError{
			Err:       ErrModelNotSet,
			Namespace: ns,
		}
	}
	var result []eh.Entity
	entity := r.factoryFn()

	err := r.client.
		//f func() eh.Entity
		//WithParam("namespace", ns).
		WithContext(ctx).
		Model(entity).
		ForEach(func(*eh.Entity) func(e X) error {

			return func(e X) error {
				result = append(result, e)
				return nil
			}

		}(&entity))

	if err != nil {
		return nil, eh.RepoError{
			Err:       eh.ErrEntityNotFound,
			BaseErr:   err,
			Namespace: ns,
		}
	}

	return result, nil
}

// FindWithFilter allows to find entities with a filter
func (r *Repo) FindWithFilter(ctx context.Context, expr string, args ...interface{}) ([]eh.Entity, error) {
	if r.factoryFn == nil {
		return nil, eh.RepoError{
			Err:       ErrModelNotSet,
			Namespace: eh.NamespaceFromContext(ctx),
		}
	}

	return nil, nil
}

// FindWithFilterUsingIndex allows to find entities with a filter using an index
func (r *Repo) FindWithFilterUsingIndex(ctx context.Context, indexInput IndexInput, filterQuery string, filterArgs ...interface{}) ([]eh.Entity, error) {
	if r.factoryFn == nil {
		return nil, eh.RepoError{
			Err:       ErrModelNotSet,
			Namespace: eh.NamespaceFromContext(ctx),
		}
	}

	return nil, nil
}

// Save implements the Save method of the eventhorizon.WriteRepo interface.
func (r *Repo) Save(ctx context.Context, entity eh.Entity) error {

	if entity.EntityID() == uuid.Nil {
		return eh.RepoError{
			Err:       eh.ErrCouldNotSaveEntity,
			BaseErr:   eh.ErrMissingEntityID,
			Namespace: eh.NamespaceFromContext(ctx),
		}
	}

	fromDB := r.factoryFn()
	_ = r.client.
		Model(fromDB).
		Where("id = ?", entity.EntityID()).
		Select()

	if fromDB.EntityID() == entity.EntityID() {
		result, err := r.client.
			Model(entity).
			Where("id = ?", entity.EntityID()).
			Update()
		if err != nil {
			return eh.RepoError{
				Err:       eh.ErrCouldNotSaveEntity,
				BaseErr:   err,
				Namespace: eh.NamespaceFromContext(ctx),
			}
		}
		if result != nil && result.RowsAffected() != 1 {
			return eh.RepoError{
				Err:       eh.ErrCouldNotSaveEntity,
				BaseErr:   err,
				Namespace: eh.NamespaceFromContext(ctx),
			}
		}

		return nil
	}

	if _, err := r.client.
		Model(entity).
		//OnConflict("(id) DO UPDATE").
		//Set("title = EXCLUDED.title").
		Insert(); err != nil {
		return eh.RepoError{
			Err:       eh.ErrCouldNotSaveEntity,
			BaseErr:   err,
			Namespace: eh.NamespaceFromContext(ctx),
		}
	}

	return nil
}

// Remove implements the Remove method of the eventhorizon.WriteRepo interface.
func (r *Repo) Remove(ctx context.Context, id uuid.UUID) error {
	if r.factoryFn == nil {
		return eh.RepoError{
			Err:       ErrModelNotSet,
			Namespace: eh.NamespaceFromContext(ctx),
		}
	}
	entity := r.factoryFn()
	w, err := r.client.
		Model(entity).
		Where("id = ?", id).
		Delete()
	if err != nil {
		return eh.RepoError{
			Err:       eh.ErrCouldNotRemoveEntity,
			BaseErr:   err,
			Namespace: eh.NamespaceFromContext(ctx),
		}
	}
	if w != nil && w.RowsAffected() != 1 {
		return eh.RepoError{
			Err:       eh.ErrEntityNotFound,
			BaseErr:   err,
			Namespace: eh.NamespaceFromContext(ctx),
		}
	}

	return nil
}

// SetEntityFactory sets a factory function that creates concrete entity types.
func (r *Repo) SetEntityFactory(f func() eh.Entity) {
	r.factoryFn = f
}

// IndexInput is all the params we need to filter on an index
type IndexInput struct {
	IndexName         string
	PartitionKey      string
	PartitionKeyValue interface{}
	SortKey           string
	SortKeyValue      interface{}
}

// Clear clears the read model database.
func (r *Repo) Clear(ctx context.Context) error {
	entity := r.factoryFn()
	if _, err := r.client.WithContext(ctx).
		Model(entity).
		WherePK().
		Delete(); err != nil {
		return eh.RepoError{
			Err:       ErrCouldNotClearDB,
			BaseErr:   err,
			Namespace: eh.NamespaceFromContext(ctx),
		}
	}
	return nil
}

// Close closes a database session.
func (r *Repo) Close(_ context.Context) {
	if err := r.client.Close(); err != nil {
		log.Fatalf("cannot close db %v", err)
	}

}

// Repository returns a parent ReadRepo if there is one.
func Repository(repo eh.ReadRepo) *Repo {
	if repo == nil {
		return nil
	}

	if r, ok := repo.(*Repo); ok {
		return r
	}

	return Repository(repo.Parent())
}
