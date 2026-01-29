package routes

import (
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"speedyvault/src/config"
	"speedyvault/src/handlers"
	"speedyvault/src/routes/middleware"

	"github.com/valyala/fasthttp"
)

func BucketUpload(ctx* fasthttp.RequestCtx) {
	bucket := middleware.AuthorizeBucketAPIRequest(ctx);
	if bucket == nil {
		ctx.SetConnectionClose();
		return;
	}

	key := string(ctx.Path());
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
	
	// Receive the file in chunks and stream directly to the file.
	streamBuffer := make([]byte, config.AppConfig.UploadStreamingChunkSize);
	var bytesReceived int = 0;
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
		bytesReceived += bytesRead;
		if bytesReceived > config.AppConfig.MaxSinglePartSize {
			file.Close();
			os.Remove(objectFilePath);
			ctx.Error(fmt.Sprintf("single part cannot exceed %d bytes", config.AppConfig.MaxSinglePartSize), 413);
			ctx.SetConnectionClose();
			return;
		}

		// Stream the bytes into the file.
		if _, err := file.Write(streamBuffer[0:bytesRead]); err != nil {
			file.Close();
			os.Remove(objectFilePath);
			log.Println(err);
			ctx.SetStatusCode(500);
			ctx.SetConnectionClose();
			return;
		}

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
	isCreated, err := handlers.Object.CreateOrReplaceObject(bucket, objectId, derivedContentType, key);
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