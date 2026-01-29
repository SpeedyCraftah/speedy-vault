package handlers

import (
	"database/sql"
	"log"
	"os"
	"time"

	"github.com/mattn/go-sqlite3"
)

type CachedObject struct {
	id int64;
	createdMs int64;

	key string;
	contentTypeMime sql.NullString; // The mime type that was specified in the "Content-Type" header (if at all).

	objectUid string; // The UID of the disk file that this object is housed under.
}

// Creates a new object from the key, or replaces the file if an object with the specified key already exists.
// In-case of an error, the file object under objectUid is not consumed and should either be retried or removed by the caller.
// Returns a boolean indicating whether a new object was created, or if the object's file was replaced (false).
func (ObjectHandler) CreateOrReplaceObject(bucket *CachedBucket, objectUid string, contentTypeMime sql.NullString, key string) (bool, error) {
	// Try add the new object to the database.
	_, err := DB.Exec(
		"INSERT INTO objects(bucket_id,created_ms,key,content_type_mime,object_uid) VALUES(?,?,?,?,?)",
		bucket.id, time.Now().UnixMilli(), key, contentTypeMime, objectUid,
	);

	if err != nil {
		// If this was a SQLite error indicating that this key already exists.
		if sqliteError, ok := err.(sqlite3.Error); ok && sqliteError.Code == sqlite3.ErrNo(sqlite3.ErrConstraint) {
			object, err := Object.GetObjectByKey(bucket, key);
			if err != nil {
				log.Println("Problem while fetching object from database ", err);
				return false, err;
			}

			// Replace the object UID and content type.
			if _, err := DB.Exec("UPDATE objects SET object_uid = ?, content_type_mime = ? WHERE id = ?", objectUid, contentTypeMime, object.id); err != nil {
				log.Println("Problem while updating object in database ", err);
				return false, err;
			}

			// Remove the old object UID from disk (if one was present).
			if err := os.Remove(bucket.GetObjectPath(object.objectUid)); err != nil {
				log.Println("Problem while removing old objectUid " + object.objectUid + " from disk ", err);
			}

			return false, nil;
		}

		// Fallback to returning an error.
		log.Println("Problem while inserting object to database ", err);
		return false, err;
	}

	return true, nil;
}

// TODO: cache objects.
func (ObjectHandler) GetObjectByKey(bucket *CachedBucket, key string) (*CachedObject, error) {
	var object CachedObject;
	if err := DB.QueryRow(
		"SELECT id,created_ms,key,content_type_mime,object_uid FROM objects WHERE key = ? AND bucket_id = ? AND deleted = 0",
		key, bucket.id,
	).Scan(&object.id, &object.createdMs, &object.key, &object.contentTypeMime, &object.objectUid); err != nil {
		// No object with this key exists.
		if err == sql.ErrNoRows {
			return nil, nil;
		}
		
		log.Println("Problem while fetching object from database ", err);
		return nil, err;
	}

	return &object, nil;
}

func (ObjectHandler) InitDBTables() {
	var err error;

	_, err = DB.Exec(`
		CREATE TABLE IF NOT EXISTS objects (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			bucket_id INTEGER NOT NULL,
			created_ms UNSIGNED BIGINT NOT NULL,
			
			key TEXT NOT NULL,
			content_type_mime TEXT,
			
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