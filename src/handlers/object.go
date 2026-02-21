package handlers

import (
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"log"
	"os"
	"time"

	"github.com/mattn/go-sqlite3"
)

type CachedObject struct {
	id              int64
	CreatedMs       uint64
	ContentTypeMime sql.NullString // The mime type that was specified in the "Content-Type" header (if at all).
	Key             []byte

	File CachedFile
}

var ObjectOperationConflictError = errors.New("Object under specified key already exists")

// Attempts to create an object, returning ObjectOperationConflictError if an object under this key already exists.
// In-case of an error, the file object under objectUid is not consumed and should either be retried or removed by the caller.
func (ObjectHandler) CreateObject(bucket *CachedBucket, objectUid string, contentTypeMime sql.NullString, digest []byte, size uint64, key []byte) error {
	var err error

	dbCtx := context.Background()
	dbConn, err := DB.Conn(dbCtx)
	if err != nil {
		log.Println("Problem while acquiring scoped database session ", err)
		return err
	}
	defer dbConn.Close()

	// We need to acquire an immediate lock here to prevent duplicate files.
	if _, err := dbConn.ExecContext(dbCtx, "BEGIN IMMEDIATE TRANSACTION"); err != nil {
		log.Println("Problem while acquiring immediate transaction lock ", err)
		return err
	}

	fileId, isNew, err := File.DeduplicateOrCreateFile(dbConn, dbCtx, bucket, objectUid, digest, size)
	if err != nil {
		if _, err := dbConn.ExecContext(dbCtx, "ROLLBACK"); err != nil {
			log.Println("Problem while rolling back database transaction ", err)
		}

		return err
	}

	// Past this point, we have the fileId of either an existing file with the refcount incremented, or a new file.

	// Try add the new object to the database.
	_, err = dbConn.ExecContext(dbCtx,
		"INSERT INTO objects(bucket_id,file_id,created_ms,key,content_type_mime) VALUES(?,?,?,?,?)",
		bucket.id, fileId, time.Now().UnixMilli(), key, contentTypeMime, digest,
	)
	if err != nil {
		if _, err := dbConn.ExecContext(dbCtx, "ROLLBACK"); err != nil {
			log.Println("Problem while rolling back database transaction ", err)
		}

		// If this was a SQLite error indicating that this key already exists.
		if sqliteError, ok := err.(sqlite3.Error); ok && sqliteError.Code == sqlite3.ErrNo(sqlite3.ErrConstraint) {
			return ObjectOperationConflictError
		}

		log.Println("Problem while inserting object to database ", err)
		return err
	}

	// We're clear!
	if _, err := dbConn.ExecContext(dbCtx, "COMMIT"); err != nil {
		// Any errors are ignored here since there's not much we can do about it.
		dbConn.ExecContext(dbCtx, "ROLLBACK")

		log.Println("Problem while committing database transaction ", err)
		return err
	}

	// If we've incremented an existing file reference count, delete the one that was provided.
	if !isNew {
		if err := os.Remove(bucket.GetObjectPath(objectUid)); err != nil {
			log.Println("Problem while removing redundant objectUid "+objectUid+" from disk ", err)
		}
	}

	return err
}

// Attempts to replace an existing object, returning ObjectOperationConflictError if an object under this key doesn't exist.
// In-case of an error, the file object under objectUid is not consumed and should either be retried or removed by the caller.
func (ObjectHandler) ReplaceObject(bucket *CachedBucket, objectUid string, contentTypeMime sql.NullString, digest []byte, size uint64, key []byte) (errReturn error) {
	dbCtx := context.Background()
	dbConn, err := DB.Conn(dbCtx)
	if err != nil {
		log.Println("Problem while acquiring scoped database session ", err)
		return err
	}
	defer dbConn.Close()

	// We need to acquire an immediate lock here to prevent duplicate files.
	if _, err := dbConn.ExecContext(dbCtx, "BEGIN IMMEDIATE TRANSACTION"); err != nil {
		log.Println("Problem while acquiring immediate transaction lock ", err)
		return err
	}

	var objectId, prevFileId int64

	// Try fetch the object referenced.
	if err := dbConn.QueryRowContext(dbCtx, "SELECT id,file_id FROM objects WHERE key = ?", key).Scan(&objectId, &prevFileId); err != nil {
		if _, err := dbConn.ExecContext(dbCtx, "ROLLBACK"); err != nil {
			log.Println("Problem while rolling back database transaction ", err)
		}

		if err == sql.ErrNoRows {
			return ObjectOperationConflictError
		}

		log.Println("Problem while finding object by key for replace ", err)
		return err
	}

	fileId, isFileNew, err := File.DeduplicateOrCreateFile(dbConn, dbCtx, bucket, objectUid, digest, size)
	if err != nil {
		if _, err := dbConn.ExecContext(dbCtx, "ROLLBACK"); err != nil {
			log.Println("Problem while rolling back database transaction ", err)
		}

		return err
	}

	// Swap the file pointer in the object with the updated one, as well as other parameters.
	if _, err := dbConn.ExecContext(dbCtx, "UPDATE objects SET file_id = ?, content_type_mime = ? WHERE id = ?", fileId, contentTypeMime, objectId); err != nil {
		if _, err := dbConn.ExecContext(dbCtx, "ROLLBACK"); err != nil {
			log.Println("Problem while rolling back database transaction ", err)
		}

		log.Println("Problem while updating object in database ", err)
		return err
	}

	// Decrement the previous file's reference count.
	var prevRefCount int64
	if err := dbConn.QueryRowContext(dbCtx, "UPDATE files SET ref_count = ref_count - 1 WHERE id = ? RETURNING ref_count", prevFileId).Scan(&prevRefCount); err != nil {
		if _, err := dbConn.ExecContext(dbCtx, "ROLLBACK"); err != nil {
			log.Println("Problem while rolling back database transaction ", err)
		}

		log.Println("Problem while decrementing ref count for object in database ", err)
		return err
	}

	// If the reference count for the former file reached 0, we can remove it entirely.
	if prevRefCount == 0 {
		var orphanedFileUid string
		if err := dbConn.QueryRowContext(dbCtx, "DELETE FROM files WHERE id = ? RETURNING uid", prevFileId).Scan(&orphanedFileUid); err != nil {
			if _, err := dbConn.ExecContext(dbCtx, "ROLLBACK"); err != nil {
				log.Println("Problem while rolling back database transaction ", err)
			}

			log.Println("Problem while deleting zero-reference file in database ", err)
			return err
		}

		// We don't want to do a potentially expensive IO operation while keeping the table locked, so we can do it after.
		defer func() {
			if errReturn == nil {
				if err := os.Remove(bucket.GetObjectPath(orphanedFileUid)); err != nil {
					log.Println("Problem while deleting orphaned object file ", err)
				}
			}
		}()
	}

	// We're clear!
	if _, err := dbConn.ExecContext(dbCtx, "COMMIT"); err != nil {
		// Any errors are ignored here since there's not much we can do about it.
		dbConn.ExecContext(dbCtx, "ROLLBACK")

		log.Println("Problem while committing database transaction ", err)
		return err
	}

	// If the uploaded new object wasn't used due to deduplication, remove it.
	if !isFileNew {
		if err := os.Remove(bucket.GetObjectPath(objectUid)); err != nil {
			log.Println("Problem while deleting redundant object file ", err)
		}
	}

	return nil
}

// Creates a new object from the key, or replaces the file if an object with the specified key already exists.
// In-case of an error, the file object under objectUid is not consumed and should either be retried or removed by the caller.
// Takes in booleans 'allowUpdate' and 'allowCreate' which determines whether to allow replacing an existing object with the same key, if an update is required error COROUpdateRequiredError is returned.
// Returns a boolean indicating whether a new object was created, or if the object's file was replaced (false).
func (ObjectHandler) CreateOrReplaceObject(bucket *CachedBucket, objectUid string, contentTypeMime sql.NullString, digest []byte, size uint64, key []byte) (bool, error) {
	err := Object.CreateObject(bucket, objectUid, contentTypeMime, digest, size, key)
	if err != nil {
		// If the object under this key already exists.
		if err == ObjectOperationConflictError {
			err := Object.ReplaceObject(bucket, objectUid, contentTypeMime, digest, size, key)
			if err != nil {
				return false, err
			}

			return false, nil
		}

		// Fallback on unexpected error.
		return false, err
	}

	// New object created successfully.
	return true, nil
}

// TODO: cache objects.
func (ObjectHandler) GetObjectByKey(bucket *CachedBucket, key []byte) (*CachedObject, error) {
	var object CachedObject
	if err := DB.QueryRow(
		`SELECT
			objects.id, objects.created_ms, objects.content_type_mime, objects.key,
			files.id, files.digest, files.size, files.uid
		FROM objects INNER JOIN files ON objects.file_id = files.id 
		WHERE objects.key = ? AND objects.bucket_id = ?`,
		key, bucket.id,
	).Scan(
		&object.id, &object.CreatedMs, &object.ContentTypeMime, &object.Key,
		&object.File.id, &object.File.Digest, &object.File.Size, &object.File.UID,
	); err != nil {
		// No object with this key exists.
		if err == sql.ErrNoRows {
			return nil, nil
		}

		log.Println("Problem while fetching object from database ", err)
		return nil, err
	}

	// Parse the ETag into a HTTP-ready format.
	etagSize := base64.RawURLEncoding.EncodedLen(len(object.File.Digest)) + 2
	object.File.ETag = make([]byte, etagSize)
	object.File.ETag[0] = '"'
	object.File.ETag[etagSize-1] = '"'
	base64.RawURLEncoding.Encode(object.File.ETag[1:etagSize-1], object.File.Digest)

	return &object, nil
}

func (ObjectHandler) InitDBTables() {
	var err error

	_, err = DB.Exec(`
		CREATE TABLE IF NOT EXISTS objects (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			bucket_id INTEGER NOT NULL,
			file_id INTEGER NOT NULL,
			created_ms UNSIGNED BIGINT NOT NULL,
			
			key BLOB NOT NULL, -- <- this is a blob on purpose so that we can search the key directly without converting fasthttp's path to a string first.
			content_type_mime TEXT,

			FOREIGN KEY (bucket_id) REFERENCES buckets (id) ON DELETE CASCADE,
			FOREIGN KEY (file_id) REFERENCES files (id),
			UNIQUE (bucket_id, key)
		)
	`)

	if err != nil {
		log.Fatal("Error while creating objects table ", err)
	}

	_, err = DB.Exec("CREATE INDEX idx_objects_file_id ON objects(file_id)")
	if err != nil {
		log.Fatal("Error while creating files digest index ", err)
	}
}

type ObjectHandler struct{}

var Object = ObjectHandler{}
