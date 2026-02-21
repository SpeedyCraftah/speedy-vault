/* Middleware is specifically done "manually" as fasthttp's middleware system is a little slow and requires dynamic lookups to get assigned user values */
/* meaning it would be simpler and faster for us to just bypas the middleware system all-together. */

package middleware

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"log"
	"speedyvault/src/config"
	"speedyvault/src/handlers"
	. "speedyvault/src/handlers/constants"
	"time"

	"github.com/valyala/fasthttp"
	"github.com/zeebo/blake3"
)

const wwwAuthHeaderKey = "WWW-Authenticate"
const wwwAuthHeaderValue = `APIKey realm="bucket", instructions="authorize via header 'X-SV-Auth-Key'", charset="UTF-8"`

// A reusable function which returns an access permission error.
func GeneralPermissionDeniedAccess(ctx *fasthttp.RequestCtx) {
	ctx.Error("permission denied (insufficient access)", 401)
}

// Fetches the destination bucket from a request.
// If this bucket cannot be found, nil is returned and the request is modified to reflect this.
func GetBucketFromRequest(ctx *fasthttp.RequestCtx) *handlers.CachedBucket {
	rawBucketName := ctx.Request.Header.Peek("x-sv-rp-bucket")
	if len(rawBucketName) == 0 {
		log.Println("Warning: Received a request without 'X-SV-RP-Bucket' set, make sure your reverse proxy sets this correctly to the subdomain in the request otherwise the bucket this request belongs to is unknown!")
		ctx.SetStatusCode(500)
		return nil
	}

	bucketName := string(rawBucketName)
	bucket, err := handlers.Bucket.GetBucketByName(bucketName)
	if err != nil {
		ctx.SetStatusCode(500)
		return nil
	}

	if bucket == nil {
		ctx.Error("bucket not found", 404)
		return nil
	}

	return bucket
}

// Authorizes a request and returns the bucket associated with this request alongside the access allowed in this context.
// If authentication fails, nil will be returned and context/response will be automatically modified.
func AuthorizeBucketAPIRequest(ctx *fasthttp.RequestCtx) (*handlers.CachedBucket, ObjectOperationFlags) {
	// Reuse the fetch bucket middleware to get the bucket.
	// As much as I hate function call overhead, this almost definitely doesn't matter.
	bucket := GetBucketFromRequest(ctx)
	if bucket == nil {
		return nil, 0
	}

	// Check if the request is authenticated.

	// If the request wants to authenticate via API key.
	if rawAPIKeySecret := ctx.Request.Header.Peek("x-sv-auth-key"); len(rawAPIKeySecret) != 0 {
		apiKey := bucket.APIKeys.Get(rawAPIKeySecret)
		if apiKey == nil {
			ctx.SetStatusCode(401)
			ctx.Response.Header.Add(wwwAuthHeaderKey, wwwAuthHeaderValue)
			return nil, 0
		}

		// Allow access.
		return bucket, ObjectOperationFlagsAll
	}

	query := ctx.QueryArgs()

	// If the request is a signed URL.
	if algorithm := query.Peek("alg"); len(algorithm) != 0 {
		selectorKeyRaw := query.Peek("sel")
		if len(selectorKeyRaw) == 0 {
			ctx.Error("signed objects must contain a selector 'sel'", 400)
			return nil, 0
		}

		expiryRaw := query.Peek("exp")
		if len(expiryRaw) == 0 {
			ctx.Error("signed objects must contain an expiry 'exp'", 400)
			return nil, 0
		}

		accessRaw := query.Peek("acc")
		if len(accessRaw) == 0 {
			ctx.Error("signed objects must contain an access 'acc'", 400)
			return nil, 0
		}

		selectorKey, err := handlers.Misc.Btoui64(selectorKeyRaw)
		if err != nil {
			ctx.Error("invalid selector 'sel' value", 400)
			return nil, 0
		}

		expiry, err := handlers.Misc.Btoui64(expiryRaw)
		if err != nil {
			ctx.Error("invalid expiry 'exp' value", 400)
			return nil, 0
		}

		// Verify if access to the object has expired.
		// Allow some leeway for skewed clocks.
		currentMs := time.Now().UnixMilli()
		if currentMs > int64(expiry)+config.AppConfig.SignatureClockSkewMs {
			ctx.Error("permission denied (access to object has expired)", 401)
			ctx.Response.Header.Add(wwwAuthHeaderKey, wwwAuthHeaderValue)
			return nil, 0
		}

		access, err := handlers.Misc.Btoui64(accessRaw)
		if err != nil || access == 0 || access >= uint64(ObjectFlagBoundary_) {
			ctx.Error("invalid access 'acc' value", 400)
			return nil, 0
		}

		signature := query.Peek("sig")
		if len(signature) == 0 {
			ctx.Error("signed objects must contain a signature 'sig'", 400)
			return nil, 0
		}

		decodedSignature := make([]byte, base64.RawURLEncoding.DecodedLen(len(signature)))
		if _, err := base64.RawURLEncoding.Decode(decodedSignature, signature); err != nil {
			ctx.Error("invalid signature 'sig' encoding", 400)
			return nil, 0
		}

		switch string(algorithm) {
		case "MAC-SHA256":
			if len(decodedSignature) != 32 {
				ctx.Error("invalid signature 'sig' digest length", 400)
				return nil, 0
			}

			selector := bucket.ObjectAuth.MAC[uint32(selectorKey)]
			if selector == nil {
				ctx.Error("permission denied (unknown selector)", 401)
				ctx.Response.Header.Add(wwwAuthHeaderKey, wwwAuthHeaderValue)
				return nil, 0
			}

			hasher := sha256.New()
			hasher.Write(ctx.Path())
			hasher.Write(expiryRaw)
			hasher.Write(accessRaw)
			hasher.Write(selector.Secret)
			digest := hasher.Sum(nil)

			if !bytes.Equal(digest, decodedSignature) {
				ctx.Error("permission denied (invalid signature)", 401)
				ctx.Response.Header.Add(wwwAuthHeaderKey, wwwAuthHeaderValue)
				return nil, 0
			}

			return bucket, ObjectOperationFlags(access)

		case "MAC-BLAKE3256":
			if len(decodedSignature) != 32 {
				ctx.Error("invalid signature 'sig' digest length", 400)
				return nil, 0
			}

			selector := bucket.ObjectAuth.MAC[uint32(selectorKey)]
			if selector == nil {
				ctx.Error("permission denied (unknown selector)", 401)
				ctx.Response.Header.Add(wwwAuthHeaderKey, wwwAuthHeaderValue)
				return nil, 0
			}

			hasher := blake3.New()
			hasher.Write(ctx.Path())
			hasher.Write(expiryRaw)
			hasher.Write(accessRaw)
			hasher.Write(selector.Secret)
			digest := hasher.Sum(nil)

			if !bytes.Equal(digest, decodedSignature) {
				ctx.Error("permission denied (invalid signature)", 401)
				ctx.Response.Header.Add(wwwAuthHeaderKey, wwwAuthHeaderValue)
				return nil, 0
			}

			return bucket, ObjectOperationFlags(access)
		}
	}

	return bucket, 0
}
