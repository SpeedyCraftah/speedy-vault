package handlers

import (
	"database/sql"
	"encoding/base64"
	"errors"
	"log"
	"os"
	"time"

	"github.com/mattn/go-sqlite3"
)

type CachedObject struct {
	id int64;
	CreatedMs uint64;
	ContentTypeMime sql.NullString; // The mime type that was specified in the "Content-Type" header (if at all).
	Digest []byte; // BLAKE3 digest of the file, doesn't have an explicit length in the type as the SQLite3 library doesn't support this.
	Size uint64;
	ObjectUid string; // The UID of the disk file that this object is housed under.

	ETag []byte; // Not part of the database, but a cached parsed strong ETag for use in HTTP responses.
}

var ObjectOperationConflictError = errors.New("Object under specified key already exists");

// Attempts to create an object, returning ObjectOperationConflictError if an object under this key already exists.
// In-case of an error, the file object under objectUid is not consumed and should either be retried or removed by the caller.
func (ObjectHandler) CreateObject(bucket *CachedBucket, objectUid string, contentTypeMime sql.NullString, digest []byte, size uint64, key []byte) error {
	// Try add the new object to the database.
	_, err := DB.Exec(
		"INSERT INTO objects(bucket_id,created_ms,key,content_type_mime,digest,size,object_uid) VALUES(?,?,?,?,?,?,?)",
		bucket.id, time.Now().UnixMilli(), key, contentTypeMime, digest, size, objectUid,
	);

	if err != nil {
		// If this was a SQLite error indicating that this key already exists.
		if sqliteError, ok := err.(sqlite3.Error); ok && sqliteError.Code == sqlite3.ErrNo(sqlite3.ErrConstraint) {
			return ObjectOperationConflictError;
		}

		log.Println("Problem while inserting object to database ", err);
	}

	return err;
}

// Attempts to replace an existing object, returning ObjectOperationConflictError if an object under this key doesn't exist.
// In-case of an error, the file object under objectUid is not consumed and should either be retried or removed by the caller.
func (ObjectHandler) ReplaceObject(bucket *CachedBucket, objectUid string, contentTypeMime sql.NullString, digest []byte, size uint64, key []byte) error {
	object, err := Object.GetObjectByKey(bucket, key);
	if err != nil {
		return err;
	}

	// Replace the object UID and content type.
	result, err := DB.Exec("UPDATE objects SET object_uid = ?, content_type_mime = ?, digest = ?, size = ? WHERE id = ?", objectUid, contentTypeMime, digest, size, object.id);
	if err != nil {
		log.Println("Problem while updating object in database ", err);
		return err;
	}
	
	// If the update matched no objects.
	if count, _ := result.RowsAffected(); count == 0 {
		return ObjectOperationConflictError;
	}

	// Remove the old object UID from disk (if one was present).
	if err := os.Remove(bucket.GetObjectPath(object.ObjectUid)); err != nil {
		log.Println("Problem while removing old objectUid " + object.ObjectUid + " from disk ", err);
	}

	return nil;
}

// Creates a new object from the key, or replaces the file if an object with the specified key already exists.
// In-case of an error, the file object under objectUid is not consumed and should either be retried or removed by the caller.
// Takes in booleans 'allowUpdate' and 'allowCreate' which determines whether to allow replacing an existing object with the same key, if an update is required error COROUpdateRequiredError is returned.
// Returns a boolean indicating whether a new object was created, or if the object's file was replaced (false).
func (ObjectHandler) CreateOrReplaceObject(bucket *CachedBucket, objectUid string, contentTypeMime sql.NullString, digest []byte, size uint64, key []byte) (bool, error) {
	err := Object.CreateObject(bucket, objectUid, contentTypeMime, digest, size, key);
	if err != nil {
		// If the object under this key already exists.
		if err == ObjectOperationConflictError {
			err := Object.ReplaceObject(bucket, objectUid, contentTypeMime, digest, size, key);
			if err != nil {
				return false, err;
			}

			return false, nil;
		}

		// Fallback on unexpected error.
		return false, err;
	}

	// New object created successfully.
	return true, nil;
}

// TODO: cache objects.
func (ObjectHandler) GetObjectByKey(bucket *CachedBucket, key []byte) (*CachedObject, error) {
	var object CachedObject;
	if err := DB.QueryRow(
		"SELECT id,created_ms,content_type_mime,digest,size,object_uid FROM objects WHERE key = ? AND bucket_id = ? AND deleted = 0",
		key, bucket.id,
	).Scan(&object.id, &object.CreatedMs, &object.ContentTypeMime, &object.Digest, &object.Size, &object.ObjectUid); err != nil {
		// No object with this key exists.
		if err == sql.ErrNoRows {
			return nil, nil;
		}
		
		log.Println("Problem while fetching object from database ", err);
		return nil, err;
	}

	// Parse the ETag into a HTTP-ready format.
	etagSize := base64.RawURLEncoding.EncodedLen(len(object.Digest)) + 2;
	object.ETag = make([]byte, etagSize);
	object.ETag[0] = '"'; object.ETag[etagSize - 1] = '"';
	base64.RawURLEncoding.Encode(object.ETag[1 : etagSize - 1], object.Digest);

	return &object, nil;
}

func (ObjectHandler) InitDBTables() {
	var err error;

	_, err = DB.Exec(`
		CREATE TABLE IF NOT EXISTS objects (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			bucket_id INTEGER NOT NULL,
			created_ms UNSIGNED BIGINT NOT NULL,
			
			key BLOB NOT NULL, -- <- this is a blob on purpose so that we can search the key directly without converting fasthttp's path to a string first.
			content_type_mime TEXT,
			digest BLOB(32) NOT NULL,
			size UNSIGNED INTEGER NOT NULL,
			
			deleted TINYINT NOT NULL DEFAULT 0,
			object_uid VARCHAR(22) NOT NULL,

			FOREIGN KEY (bucket_id) REFERENCES buckets (id) ON DELETE CASCADE,
			UNIQUE (bucket_id, key)
		)
	`);

	if err != nil {
		log.Fatal("Error while creating objects table ", err);
	}
}

type ObjectHandler struct{};
var Object = ObjectHandler{};