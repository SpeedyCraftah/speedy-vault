/* Middleware is specifically done "manually" as fasthttp's middleware system is a little slow and requires dynamic lookups to get assigned user values */
/* meaning it would be simpler and faster for us to just bypas the middleware system all-together. */

package middleware

import (
	"log"
	"speedyvault/src/handlers"

	"github.com/valyala/fasthttp"
)

const wwwAuthHeaderKey = "WWW-Authenticate";
const wwwAuthHeaderValue = `APIKey realm="bucket", instructions="authorize via header 'X-SV-Auth-Key'", charset="UTF-8"`;

// Fetches the destination bucket from a request.
// If this bucket cannot be found, nil is returned and the request is modified to reflect this.
func GetBucketFromRequest(ctx *fasthttp.RequestCtx) *handlers.CachedBucket {
	rawBucketName := ctx.Request.Header.Peek("x-sv-rp-bucket");
	if rawBucketName == nil {
		log.Println("Warning: Received a request without 'X-SV-RP-Bucket' set, make sure your reverse proxy sets this correctly to the subdomain in the request otherwise the bucket this request belongs to is unknown!");
		ctx.SetStatusCode(500);
		return nil;
	}

	bucketName := string(rawBucketName);
	bucket, err := handlers.Bucket.GetBucketByName(bucketName);
	if err != nil {
		ctx.SetStatusCode(500);
		return nil;
	}
	
	if bucket == nil {
		ctx.Error("bucket not found", 404);
		return nil;
	}

	return bucket;
}

// Authorizes a request and returns the bucket associated with this request.
// If authentication fails, nil will be returned and context/response will be automatically modified.
func AuthorizeBucketAPIRequest(ctx *fasthttp.RequestCtx) *handlers.CachedBucket {
	// Reuse the fetch bucket middleware to get the bucket.
	// As much as I hate function call overhead, this almost definitely doesn't matter.
	bucket := GetBucketFromRequest(ctx);
	if bucket == nil {
		return nil;
	}

	// Check if the user is authenticated.

	rawAPIKeySecret := ctx.Request.Header.Peek("x-sv-auth-key");
	if rawAPIKeySecret == nil {
		ctx.SetStatusCode(401);
		ctx.Response.Header.Add(wwwAuthHeaderKey, wwwAuthHeaderValue);
		return nil;
	}

	apiKey := bucket.APIKeys.Get(rawAPIKeySecret);
	if apiKey == nil {
		ctx.SetStatusCode(401);
		ctx.Response.Header.Add(wwwAuthHeaderKey, wwwAuthHeaderValue);
		return nil;
	}

	// Allow access.
	return bucket;
}