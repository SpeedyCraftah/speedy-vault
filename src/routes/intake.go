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

func BucketUpload(ctx* fasthttp.RequestCtx) {
	bucket, access := middleware.AuthorizeBucketAPIRequest(ctx);
	if bucket == nil {
		ctx.SetConnectionClose();
		return;
	}

	// Check if the context is even allowed any of the possible operations.
	if !access.HasAny(ObjectCreate | ObjectUpdate) {
		middleware.PermissionDeniedAccess(ctx);
		ctx.SetConnectionClose();
		return;
	}

	key := ctx.Path();
	stream := ctx.Request.BodyStream();

	objectId := handlers.Misc.NewRandomUID();
	objectFilePath := bucket.GetObjectPath(objectId);

	// Try create the object file stored on disk.
	file, err := os.Create(objectFilePath);
	if err != nil {
		log.Println(err);
		ctx.SetStatusCode(500);
		ctx.SetConnectionClose();
		return;
	}
	
	hasher := blake3.New();
	
	// Receive the file in chunks and stream directly to the file.
	streamBuffer := make([]byte, config.AppConfig.UploadStreamingChunkSize);
	var bytesReceived uint64 = 0;
	for {
		bytesRead, err := stream.Read(streamBuffer);
		if err != nil && err != io.EOF {
			file.Close();
			os.Remove(objectFilePath);
			log.Println(err);
			ctx.SetStatusCode(500);
			ctx.SetConnectionClose();
			return;
		}

		// Count the total bytes received, return 413 if over limit.
		bytesReceived += uint64(bytesRead);
		if bytesReceived > config.AppConfig.MaxSinglePartSize {
			file.Close();
			os.Remove(objectFilePath);
			ctx.Error(fmt.Sprintf("single part cannot exceed %d bytes", config.AppConfig.MaxSinglePartSize), 413);
			ctx.SetConnectionClose();
			return;
		}

		bufferSlice := streamBuffer[0:bytesRead];
		
		// Stream the bytes into the file.
		if _, err := file.Write(bufferSlice); err != nil {
			file.Close();
			os.Remove(objectFilePath);
			log.Println(err);
			ctx.SetStatusCode(500);
			ctx.SetConnectionClose();
			return;
		}

		// Add the buffer bytes into the digest (returns an error but the package always returns a hardcoded nil).
		hasher.Write(bufferSlice);

		// If we've received everything.
		if err == io.EOF {
			break;
		}
	}

	file.Close();

	// Extract the content type header (if set by the client).
	derivedContentType := sql.NullString{ Valid: false };
	if contentType := ctx.Request.Header.ContentType(); contentType != nil {
		derivedContentType.Valid = true;
		derivedContentType.String = string(contentType);
	}
	
	// Store the object in the database.
	isCreated, err := handlers.Object.CreateOrReplaceObject(bucket, objectId, derivedContentType, hasher.Sum(nil), bytesReceived, key);
	if err != nil {
		// Clean up the downloaded file if one was provided.
		os.Remove(objectFilePath);
		ctx.SetStatusCode(500);
		return;
	}

	// Return 201 if a new object was created, otherwise 200.
	if isCreated {
		ctx.SetStatusCode(201);
	} else {
		ctx.SetStatusCode(200);
	}
}