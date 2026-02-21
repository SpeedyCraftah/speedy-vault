package handlers

import (
	"context"
	"database/sql"
	"log"
	"time"
)

type CachedFile struct {
	id     int64
	Digest []byte // BLAKE3 digest of the file, doesn't have an explicit length in the type as the SQLite3 library doesn't support this.
	Size   uint64
	UID    string // The UID of the disk file that this file is housed under.

	ETag []byte // Not part of the database, but a cached parsed strong ETag for use in HTTP responses.
}

// Modifies an existing database transaction to safely increment an existing file reference count, or create a new one.
// Returns the ID of the existing or newly created file, a boolean indicating whether a file was reused or created, and an error.
// Does not commit nor rollback on error or success.
// MUST BE EXECUTED IN EITHER AN IMMEDIATE OR EXCLUSIVE TRANSACTION FOR SAFE ATOMIC OPERATION!
func (FileHandler) DeduplicateOrCreateFile(tx *sql.Conn, ctx context.Context, bucket *CachedBucket, objectUid string, digest []byte, size uint64) (int64, bool, error) {
	var err error

	var fileId int64
	var isNew bool = false

	// Attempt to update the refcount on an existing file with this digest.
	err = tx.QueryRowContext(
		ctx, "UPDATE files SET ref_count = ref_count + 1 WHERE id = (SELECT id FROM files WHERE digest = ? AND size = ? LIMIT 1) RETURNING id", digest, size,
	).Scan(&fileId)
	if err != nil && err != sql.ErrNoRows {
		log.Println("Problem while updating file refcount ", err)
		return 0, false, err
	}

	// If there is no existing file with this digest.
	if err == sql.ErrNoRows {
		// Attempt to create a new file.
		err = tx.QueryRowContext(
			ctx, "INSERT INTO files(bucket_id,created_ms,digest,size,ref_count,uid) VALUES(?,?,?,?,?,?) RETURNING id",
			bucket.id, time.Now().UnixMilli(), digest, size, 1, objectUid,
		).Scan(&fileId)
		if err != nil {
			log.Println("Problem while inserting new file ", err)
			return 0, false, err
		}

		isNew = true
	}

	return fileId, isNew, nil
}

func (FileHandler) InitDBTables() {
	var err error

	_, err = DB.Exec(`
		CREATE TABLE IF NOT EXISTS files (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			bucket_id INTEGER NOT NULL,
			created_ms UNSIGNED BIGINT NOT NULL,

			digest BLOB(32) NOT NULL,
			size UNSIGNED INTEGER NOT NULL,
			ref_count UNSIGNED INTEGER NOT NULL,
			
			uid VARCHAR(22) UNIQUE NOT NULL,

			FOREIGN KEY (bucket_id) REFERENCES buckets (id) ON DELETE CASCADE,
			UNIQUE (bucket_id, uid)
		)
	`)

	if err != nil {
		log.Fatal("Error while creating objects table ", err)
	}

	_, err = DB.Exec("CREATE INDEX idx_files_digest ON files(digest)")
	if err != nil {
		log.Fatal("Error while creating files digest index ", err)
	}
}

type FileHandler struct{}

var File = FileHandler{}
