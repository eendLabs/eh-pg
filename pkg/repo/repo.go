package repo

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"reflect"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/jmoiron/sqlx/reflectx"
	_ "github.com/lib/pq"
	eh "github.com/looplab/eventhorizon"
)

var ErrCouldNotDialDB = errors.New("could not dial database")

// ErrCouldNotClearDB is when the database could not be cleared.
var ErrCouldNotClearDB = errors.New("could not clear database")

var ErrNoDBClient = errors.New("no database client")

// ErrModelNotSet is when an model factory is not set on the Repo.
var ErrModelNotSet = errors.New("model not set")

type DBConfig struct {
	Host     string `json:"POSTGRES_HOST,omitempty"`
	Port     int    `json:"POSTGRES_PORT,omitempty"`
	Database string `json:"POSTGRES_DB,omitempty"`
	User     string `json:"POSTGRES_USER,omitempty"`
	Password string `json:"POSTGRES_PASSWORD,omitempty"`
}

func (d DBConfig) GetConnString() string {
	if d.Database == "" {
		return fmt.Sprintf("host=%s port=%d user=%s "+
			"password=%s sslmode=disable timezone=UCT",
			d.Host, d.Port, d.User, d.Password)
	}
	return fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable timezone=UCT",
		d.Host, d.Port, d.User, d.Password, d.Database)
}

type Config struct {
	TableName string
	dbName    func(ctx context.Context) string
	DbConfig  *DBConfig
}

func (c *Config) provideDefaults() {
	c.DbConfig = &DBConfig{}
	if c.DbConfig.Host == "" {
		if host := os.Getenv("POSTGRES_HOST"); host == "" {
			c.DbConfig.Host = "localhost"
		} else {
			c.DbConfig.Host = host
		}

	}
	if c.DbConfig.Port == 0 {
		defaultPort := 5432
		if port := os.Getenv("POSTGRES_PORT"); port == "" {
			c.DbConfig.Port = defaultPort
		} else {
			if p, err := strconv.Atoi(port); p == 0 {
				c.DbConfig.Port = defaultPort
			} else if err != nil {
				log.Fatalf("could not cast port: %v", err)
			} else {
				c.DbConfig.Port = p
			}
		}
	}
	if c.DbConfig.Database == "" {
		if db := os.Getenv("POSTGRES_DB"); db == "" {
			c.DbConfig.Database = "postgres"
		} else {
			c.DbConfig.Database = db
		}
	}
	if c.DbConfig.User == "" {
		if user := os.Getenv("POSTGRES_USER"); user == "" {
			c.DbConfig.User = "postgres"
		} else {
			c.DbConfig.User = user
		}
	}
	if c.DbConfig.Password == "" {
		if pwd := os.Getenv("POSTGRES_PASSWORD"); pwd == "" {
			c.DbConfig.Password = "postgres"
		} else {
			c.DbConfig.Password = pwd
		}
	}
}

type Repo struct {
	client    *sqlx.DB
	config    *Config
	factoryFn func() eh.Entity
}

func NewRepo(config *Config) (*Repo, error) {
	config.provideDefaults()

	client, err := sqlx.Connect("postgres",
		config.DbConfig.GetConnString())
	if err != nil {
		return nil, eh.RepoError{
			Err:     ErrCouldNotDialDB,
			BaseErr: err,
		}
	}

	return NewRepoWithClient(config, client)
}

func NewRepoWithClient(config *Config, client *sqlx.DB) (*Repo, error) {
	if client == nil {
		return nil, ErrNoDBClient
	}

	r := &Repo{
		client: client,
		config: config,
	}

	r.config.dbName = func(ctx context.Context) string {
		ns := eh.NamespaceFromContext(ctx)
		return r.config.TableName + "_" + ns
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
	err := r.client.GetContext(ctx, entity,
		fmt.Sprintf("SELECT * FROM %s WHERE id=$1",
			r.config.TableName), id.String())

	if err != nil {
		return nil, eh.RepoError{
			Err:       eh.ErrEntityNotFound,
			BaseErr:   err,
			Namespace: eh.NamespaceFromContext(ctx),
		}
	}

	return entity, nil
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

	rows, err := r.client.
		QueryxContext(ctx,
			fmt.Sprintf("SELECT * FROM %s", r.config.TableName))

	if rows != nil {
		for rows.Next() {
			if err := rows.StructScan(entity); err != nil {
				return nil, err
			}
			result = append(result, entity)
			entity = r.factoryFn()
		}
	}

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
func (r *Repo) FindWithFilter(ctx context.Context, expr string,
	args ...interface{}) ([]eh.Entity, error) {
	if r.factoryFn == nil {
		return nil, eh.RepoError{
			Err:       ErrModelNotSet,
			Namespace: eh.NamespaceFromContext(ctx),
		}
	}

	return nil, nil
}

// FindWithFilterUsingIndex allows to find entities with a filter using an index
func (r *Repo) FindWithFilterUsingIndex(ctx context.Context,
	indexInput IndexInput, filterQuery string,
	filterArgs ...interface{}) ([]eh.Entity, error) {
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

	mapper := reflectx.NewMapper("db")
	fields := mapper.FieldMap(reflect.Indirect(reflect.ValueOf(entity)))
	var mapFields, excludedFields []string
	mapValues := make(map[string]interface{})

	for field, v := range fields {
		mapFields = append(mapFields, field)

		// getting type from reflect.Value
		vi := v.Interface()
		switch x := vi.(type) {
		default:
			mapValues[field] = x
		}
		excludedFields = append(excludedFields,
			fmt.Sprintf("%s = EXCLUDED.%s", field, field))

	}

	joinedFields := strings.Join(mapFields, ", ")
	joinedFieldsBindVar := ":" + strings.Join(mapFields, ", :")
	joinedFieldsExcluded := strings.Join(excludedFields, ", ")
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) "+
		"ON CONFLICT (id) DO UPDATE SET %s;",
		r.config.TableName, joinedFields, joinedFieldsBindVar,
		joinedFieldsExcluded)
	log.Println(query)

	if w, err := r.client.
		NamedExecContext(ctx,
			query,
			mapValues); err != nil {
		return eh.RepoError{
			Err:       eh.ErrCouldNotSaveEntity,
			BaseErr:   err,
			Namespace: eh.NamespaceFromContext(ctx),
		}
	} else {
		affected, err := w.RowsAffected()
		if err != nil || affected != 1 {
			return eh.RepoError{
				Err:       eh.ErrCouldNotSaveEntity,
				BaseErr:   err,
				Namespace: eh.NamespaceFromContext(ctx),
			}
		}

	}

	return nil
}

// Remove implements the Remove method of the eventhorizon.WriteRepo interface.
func (r *Repo) Remove(ctx context.Context, id uuid.UUID) error {
	w, err := r.client.ExecContext(ctx,
		fmt.Sprintf("DELETE FROM %s WHERE id = $1",
			r.config.TableName), id)
	if err != nil {
		return eh.RepoError{
			Err:       eh.ErrCouldNotRemoveEntity,
			BaseErr:   err,
			Namespace: eh.NamespaceFromContext(ctx),
		}
	}
	affected, err := w.RowsAffected()
	if w != nil && affected != 1 {
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
	tx := r.client.MustBeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelDefault})
	tx.MustExec(fmt.Sprintf("delete from %s", r.config.TableName))
	if err := tx.Commit(); err != nil {
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
