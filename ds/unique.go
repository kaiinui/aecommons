package ds

import (
	"errors"

	"github.com/mjibson/goon"
	"golang.org/x/net/context"
	"google.golang.org/appengine/datastore"
)

type UniqueIndex struct {
	ID string `datastore:"-" goon:"id"`
}

var ErrAlreadyExists = errors.New("UniqueIndex: specified pair of kind and key already allocated")

func AllocateUnique(ctx context.Context, kind string, key string) error {
	e := datastore.RunInTransaction(ctx, func(ctx context.Context) error {
		g := goon.FromContext(ctx)

		idx := UniqueIndex{ID: makeUniqueIndexID(kind, key)}
		e := g.Get(&idx)
		if e == datastore.ErrNoSuchEntity {
			if _, e := g.Put(&idx); e != nil {
				return e
			}

			return nil
		}
		if e != nil {
			return e
		}

		return ErrAlreadyExists
	}, nil)

	return e
}

func makeUniqueIndexID(kind, key string) string {
	return kind + "$" + key
}
