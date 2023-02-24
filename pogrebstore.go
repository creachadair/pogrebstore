// Copyright 2021 Michael J. Fromberger. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package pogrebstore implements the blob.Store interface using pogreb.
// See https://github.com/akrylysov/pogreb.
package pogrebstore

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"sort"
	"time"

	"github.com/akrylysov/pogreb"
	"github.com/creachadair/ffs/blob"
)

// Store implements the blob.Store interface using a pogreb database.
type Store struct{ db *pogreb.DB }

// Opener constructs a filestore from an address comprising a URL, for use with
// the store package. The host and path identify the database directory. The
// following optional query parameters are understood:
//
//	sync    : interval between automatic syncs (duration; default 10s)
//	compact : interval between automatic compactions (duration; default 1m)
func Opener(_ context.Context, addr string) (blob.Store, error) {
	u, err := url.Parse(addr)
	if err != nil {
		return nil, err
	}
	opts := &Options{
		DBOptions: &pogreb.Options{
			BackgroundSyncInterval:       10 * time.Second,
			BackgroundCompactionInterval: 1 * time.Minute,
		},
	}
	dirPath := filepath.Join(u.Host, filepath.FromSlash(u.Path))
	if s := u.Query().Get("sync"); s != "" {
		d, err := time.ParseDuration(s)
		if err != nil {
			return nil, fmt.Errorf("invalid sync interval: %v", err)
		}
		opts.DBOptions.BackgroundSyncInterval = d
	}
	if s := u.Query().Get("compact"); s != "" {
		d, err := time.ParseDuration(s)
		if err != nil {
			return nil, fmt.Errorf("invalid compact interval: %v", err)
		}
		opts.DBOptions.BackgroundCompactionInterval = d
	}
	return Open(dirPath, opts)
}

// Open creates a Store by opening the pogreb database specified by path.
func Open(path string, opts *Options) (*Store, error) {
	db, err := pogreb.Open(path, opts.dbOptions())
	if err != nil {
		return nil, err
	}
	return &Store{db: db}, nil
}

// Options provides options for opening a pogreb database.
type Options struct {
	// Options specific to the underlying database implementation.
	DBOptions *pogreb.Options
}

func (o *Options) dbOptions() *pogreb.Options {
	if o == nil {
		return nil
	}
	return o.DBOptions
}

// Close implements part of the blob.Store interface. It closes the underlying
// database instance and reports its result.
func (s *Store) Close(_ context.Context) error { return s.db.Close() }

// Get implements part of blob.Store.
func (s *Store) Get(_ context.Context, key string) ([]byte, error) { return s.dbGet(key) }

// dbGet fetches the data for the specified key.
func (s *Store) dbGet(key string) ([]byte, error) {
	bkey := []byte(key)
	if ok, err := s.db.Has(bkey); err != nil {
		return nil, err
	} else if !ok {
		return nil, blob.KeyNotFound(key)
	}
	return s.db.Get(bkey)
}

// Put implements part of blob.Store.
func (s *Store) Put(_ context.Context, opts blob.PutOptions) error {
	bkey := []byte(opts.Key)
	ok, err := s.db.Has(bkey)
	if err != nil {
		return err
	} else if ok && !opts.Replace {
		return blob.KeyExists(opts.Key)
	}
	return s.db.Put(bkey, opts.Data)
}

// Delete implements part of blob.Store.
func (s *Store) Delete(_ context.Context, key string) error {
	bkey := []byte(key)
	if ok, err := s.db.Has(bkey); err != nil {
		return err
	} else if !ok {
		return blob.KeyNotFound(key)
	}
	return s.db.Delete(bkey)
}

// List implements part of blob.Store. Note that the pogreb database does not
// iterate keys in a specified order, so a full scan is require, and all keys
// at or after start must be brought into memory and sorted.
func (s *Store) List(_ context.Context, start string, f func(string) error) error {
	// The database only supports complete scans, and moreover does not deliver
	// keys in lexicographic order.
	it := s.db.Items()
	var keys []string
	for {
		bkey, _, err := it.Next() // we don't need the data
		if err == pogreb.ErrIterationDone {
			break
		} else if err != nil {
			return err
		}

		key := string(bkey)
		if key < start {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if err := f(key); err != nil {
			if errors.Is(err, blob.ErrStopListing) {
				break
			}
			return err
		}
	}
	return nil
}

// Len implements part of blob.Store.
func (s *Store) Len(ctx context.Context) (int64, error) { return int64(s.db.Count()), nil }
