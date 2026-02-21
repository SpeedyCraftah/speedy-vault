package routes

import (
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"speedyvault/src/config"
	"speedyvault/src/handlers"
	. "speedyvault/src/handlers/constants"
	"speedyvault/src/routes/middleware"

	"github.com/valyala/fasthttp"
	"github.com/zeebo/blake3"
)

func BucketUpload(ctx *fasthttp.RequestCtx) {
	bucket, access := middleware.AuthorizeBucketAPIRequest(ctx)
	if bucket == nil {
		ctx.SetConnectionClose()
		return
	}

	// Check if the context is even allowed any of the possible operations.
	if !access.HasAny(ObjectCreate | ObjectUpdate) {
		middleware.GeneralPermissionDeniedAccess(ctx)
		ctx.SetConnectionClose()
		return
	}

	key := ctx.Path()

	// Check for any access constraints to this key, and handle request accordingly.
	if condition := bucket.GetKeyAccessCondition(key); condition == DenyAll {
		// Only API keys can bypass 'DenyAll'.
		if !access.HasRequired(ObjectAPIKeyAccess) {
			ctx.Error("permission denied (resource is restricted)", 403)
			ctx.SetConnectionClose()
			return
		}
	}

	stream := ctx.Request.BodyStream()

	objectId := handlers.Misc.NewRandomUID()
	objectFilePath := bucket.GetObjectPath(objectId)

	// Try create the object file stored on disk.
	file, err := os.Create(objectFilePath)
	if err != nil {
		log.Println(err)
		ctx.SetStatusCode(500)
		ctx.SetConnectionClose()
		return
	}

	hasher := blake3.New()

	// Receive the file in chunks and stream directly to the file.
	streamBuffer := make([]byte, config.AppConfig.UploadStreamingChunkSize)
	var bytesReceived uint64 = 0
	for {
		bytesRead, err := stream.Read(streamBuffer)
		if err != nil && err != io.EOF {
			file.Close()
			os.Remove(objectFilePath)
			log.Println(err)
			ctx.SetStatusCode(500)
			ctx.SetConnectionClose()
			return
		}

		// Count the total bytes received, return 413 if over limit.
		bytesReceived += uint64(bytesRead)
		if bytesReceived > config.AppConfig.MaxSinglePartSize {
			file.Close()
			os.Remove(objectFilePath)
			ctx.Error(fmt.Sprintf("single part cannot exceed %d bytes", config.AppConfig.MaxSinglePartSize), 413)
			ctx.SetConnectionClose()
			return
		}

		bufferSlice := streamBuffer[0:bytesRead]

		// Stream the bytes into the file.
		if _, err := file.Write(bufferSlice); err != nil {
			file.Close()
			os.Remove(objectFilePath)
			log.Println(err)
			ctx.SetStatusCode(500)
			ctx.SetConnectionClose()
			return
		}

		// Add the buffer bytes into the digest (returns an error but the package always returns a hardcoded nil).
		hasher.Write(bufferSlice)

		// If we've received everything.
		if err == io.EOF {
			break
		}
	}

	file.Close()

	digest := hasher.Sum(nil)

	// Extract the content type header (if set by the client).
	derivedContentType := sql.NullString{Valid: false}
	if contentType := ctx.Request.Header.ContentType(); contentType != nil {
		derivedContentType.Valid = true
		derivedContentType.String = string(contentType)
	}

	// Store the object in the database, method depending on permissions.
	var objectCreateError error
	if access.HasRequired(ObjectCreate) {
		objectCreateError = handlers.Object.CreateObject(bucket, objectId, derivedContentType, digest, bytesReceived, key)
		if objectCreateError == nil {
			// Return 201 if a new object was created.
			ctx.SetStatusCode(201)
			return
		} else if objectCreateError != handlers.ObjectOperationConflictError {
			os.Remove(objectFilePath)
			ctx.SetStatusCode(500)
			return
		}
	}

	// If an object cannot be created, the fallback is to overwrite the object.
	var objectUpdateError error
	if access.HasRequired(ObjectUpdate) {
		objectUpdateError = handlers.Object.ReplaceObject(bucket, objectId, derivedContentType, digest, bytesReceived, key)
		if objectUpdateError == nil {
			// Return 200 if the object was replaced.
			ctx.SetStatusCode(200)
			return
		} else if objectUpdateError != handlers.ObjectOperationConflictError {
			os.Remove(objectFilePath)
			ctx.SetStatusCode(500)
			return
		}
	}

	// Remove the file as we don't need it past this point.
	os.Remove(objectFilePath)

	// If both operations resulted in a conflict (which indicates a race, will be restructured in the future).
	if objectCreateError == handlers.ObjectOperationConflictError && objectUpdateError == handlers.ObjectOperationConflictError {
		ctx.Error("operation conflict detected", 503)
		ctx.Response.Header.Set("Retry-After", "0")
		return
	}

	// Return a permission error as the operation has failed due to being unable to perform one or the other operation.
	middleware.GeneralPermissionDeniedAccess(ctx)
}
