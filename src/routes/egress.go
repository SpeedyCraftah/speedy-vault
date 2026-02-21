package routes

import (
	"bytes"
	"log"
	"net"
	"os"
	"speedyvault/src/config"
	"speedyvault/src/handlers"
	. "speedyvault/src/handlers/constants"
	"speedyvault/src/routes/middleware"

	"github.com/valyala/fasthttp"
)

func ObjectDownload(ctx *fasthttp.RequestCtx) {
	bucket, access := middleware.AuthorizeBucketAPIRequest(ctx)
	if bucket == nil {
		return
	}

	key := ctx.Path()

	// Check for any access constraints to this key, and handle request accordingly.
	condition := bucket.GetKeyAccessCondition(key)
	switch condition {
	// Deny all except for API keys.
	case DenyAll:
		{
			if !access.HasRequired(ObjectAPIKeyAccess) {
				ctx.Error("permission denied (resource is restricted)", 403)
				return
			}
		}

	// Verify the context permissions before allowing access.
	case AllowSigned:
		{
			if !access.HasRequired(ObjectRead) {
				middleware.GeneralPermissionDeniedAccess(ctx)
				return
			}
		}
	}

	// Public/authorized access path.

	object, err := handlers.Object.GetObjectByKey(bucket, key)
	if err != nil {
		ctx.SetStatusCode(500)
		return
	}
	if object == nil {
		ctx.Error("object not found", 404)
		return
	}

	// Set the mandatory headers that must be present regardless of response.
	ctx.Response.Header.SetBytesV("ETag", object.File.ETag)
	if condition == AllowPublic {
		ctx.Response.Header.Set("Cache-Control", "max-age=360, public")
	} else {
		ctx.Response.Header.Set("Cache-Control", "max-age=360, private")
	}

	// Check if the client only wants us to return a file if it has changed.
	if etag := ctx.Request.Header.Peek(fasthttp.HeaderIfNoneMatch); len(etag) != 0 && bytes.Equal(etag, object.File.ETag) {
		// File has not changed, we can return a not modified status code.
		ctx.SetStatusCode(304)
		return
	}

	// Setup the read parameters, default to entire file, but otherwise can be overwritten by the "Range" header.
	var readStartByte uint64 = 0
	var readLength uint64 = object.File.Size

	// If the client only wants parts of the file.
	if rangeHeader := ctx.Request.Header.Peek(fasthttp.HeaderRange); len(rangeHeader) != 0 {
		// Proceed with the partial upload if no if-range header exists, or the if-range header contains a different etag.
		if etag := ctx.Request.Header.Peek(fasthttp.HeaderIfRange); len(etag) == 0 || bytes.Equal(etag, object.File.ETag) {
			parsedRange, err := handlers.Misc.ParseRangeHeader(rangeHeader, object.File.Size)
			if err == nil {
				readStartByte = parsedRange.Start
				readLength = parsedRange.Length
				ctx.Response.Header.Set("Content-Range", parsedRange.ContentRangeHeader)
				ctx.SetStatusCode(206)
			} else {
				// If there is an error, an unsatisfiable error should be returned to the client if range is out of bounds, otherwise the header should be ignored.
				if err == handlers.ParseRangeUnsatisfiableError {
					ctx.Response.Header.Set("Content-Range", parsedRange.ContentRangeHeader)
					ctx.Response.Header.Set("Cache-Control", "no-store") // Errors like this shouldn't be cached.
					ctx.SetStatusCode(416)
					return
				}

				// Past this point, the range header is ignored.
			}
		}
	}

	// Try to open the object file.
	file, err := os.Open(bucket.GetObjectPath(object.File.UID))
	if err != nil {
		log.Println(err)
		ctx.SetStatusCode(500)
		return
	}

	ctx.Response.Header.SetContentLength(int(readLength))
	if object.ContentTypeMime.Valid {
		ctx.Response.Header.SetContentType(object.ContentTypeMime.String)
	}

	// Stream the file to the connection.
	// We have to take the request over here as the alternative is to stream the file to fasthttp's internal buffer first which is slow.
	ctx.HijackSetNoResponse(true)
	ctx.Hijack(func(fasthttpConn net.Conn) {
		defer file.Close()

		// Get the actual connection gate-kept by FastHTTP.
		c := handlers.Misc.ExtractNetTCPFromFastHTTPWrapper(fasthttpConn)

		// Send the headers.
		c.Write(ctx.Response.Header.Header())

		buffer := make([]byte, config.AppConfig.DownloadStreamingChunkSize)
		bytesRemaining := readLength
		for {
			bufferSlice := buffer
			if bytesRemaining < uint64(len(buffer)) {
				bufferSlice = buffer[:bytesRemaining]
			}

			_, err := file.ReadAt(bufferSlice, int64(readStartByte+(readLength-bytesRemaining)))
			if err != nil {
				log.Println("Unexpected failure while reading object file ", err)
				c.SetLinger(0)
				return // No error can be returned to the client so we will just reset the connection.
			}

			// Stream to the client.
			c.Write(bufferSlice)

			bytesRemaining -= uint64(len(bufferSlice))

			if bytesRemaining == 0 {
				break
			}
		}
	})
}
