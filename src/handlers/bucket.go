package handlers

import (
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"log"
	"path"
	"regexp"
	"speedyvault/src/config"
	"strconv"
	"sync"
	"time"
)

type CachedBucket struct {
	id int64;
	createdMs int64;

	cachedMs int64;
	APIKeys CachedBucketAPIKeyStore;
	AccessRules []*CachedBucketAccessRule;
}

func (b CachedBucket) GetObjectPath(objectId string) string {
	return path.Join(config.AppConfig.DataDirectory, strconv.FormatInt(b.id, 10), "objects", objectId);
}

func (b CachedBucket) GetKeyAccessCondition(key string) BucketAccessRuleAction {
	// Attempt to find a rule that matches this key, and returns its outcome.
	for _, rule := range b.AccessRules {
		if rule.regex.MatchString(key) {
			return rule.action;
		}
	}

	// Default is to allow signed.
	return AllowSigned;
}


type BucketAccessRuleAction uint8;
const (
	AllowPublic BucketAccessRuleAction = iota; // Allow access to everyone, no matter if the URL is signed or not (effectively public access).
	AllowSigned; // Allow access only if the URL is signed and the signature is valid and hasn't expired (discretionary access), this is also the default behaviour if no rule is matched.
	DenyAll; // Blocks access outright regardless of if the URL is signed or not, you can also use it to override the default AllowSigned rule and block all access by placing it at the end matching all keys.
);

type CachedBucketAccessRule struct {
	id int64;
	
	regex *regexp.Regexp;
	action BucketAccessRuleAction;
}

type CachedBucketAPIKeyStore struct {
	cache map[[32]byte]*CachedBucketAPIKey;
}

func (store CachedBucketAPIKeyStore) Get(b64Key []byte) *CachedBucketAPIKey {
	// Our keys are always SHA256 digests (32 bytes after decoding), meaning we can make an assumption on the buffer size and error if incorrect.
	if base64.RawStdEncoding.DecodedLen(len(b64Key)) != 32 {
		return nil;
	}

	rawKey := make([]byte, 32);
	if _, err := base64.RawStdEncoding.Decode(rawKey, b64Key); err != nil {
		return nil;
	}

	// Safety: API keys are read-only, hence no locking is required.
	return store.cache[sha256.Sum256(rawKey)];
}

type CachedBucketAPIKey struct {
	id int64;
	createdMs int64;
}


var nameBucketCacheLock sync.RWMutex;
var nameBucketCache = make(map[string]*CachedBucket);

func (BucketHandler) GetBucketByName(name string) (*CachedBucket, error) {
	nameBucketCacheLock.RLock();
	cachedEntry, bucketInCache := nameBucketCache[name];
	nameBucketCacheLock.RUnlock();

	if bucketInCache {
		return cachedEntry, nil;
	}

	// Fetch the bucket from the database.
	bucket := CachedBucket{};
	if err := DB.QueryRow("SELECT id,created_ms FROM buckets WHERE name = ?", name).Scan(&bucket.id, &bucket.createdMs); err != nil {
		// No bucket of this name exists.
		if err == sql.ErrNoRows {
			return nil, nil;
		}
		
		log.Println("Problem while fetching bucket from database ", err);
		return nil, err;
	}

	// Assign the time of caching.
	bucket.cachedMs = time.Now().UnixMilli();

	// Fetch the access rule priorities for this bucket (if any).
	bucket.AccessRules = []*CachedBucketAccessRule{};
	accessRuleRows, err := DB.Query("SELECT id,regex,action FROM bucket_access_rules WHERE bucket_id = ? ORDER BY priority ASC", bucket.id);
	if err != nil {
		log.Println("Problem while fetching bucket access rules from database ", err);
		return nil, err;
	}

	for accessRuleRows.Next() {
		rule := CachedBucketAccessRule{};
		var rawRegex string;
		if err := accessRuleRows.Scan(&rule.id, &rawRegex, &rule.action); err != nil {
			accessRuleRows.Close();
			log.Println("Problem while reading bucket access rules from database ", err);
			return nil, err;
		}

		// Regex would've been validated at insertion time, so fine to panic on error.
		rule.regex = regexp.MustCompile(rawRegex);

		// Add to the rule list (already in highest to lowest priority order from database).
		bucket.AccessRules = append(bucket.AccessRules, &rule);
	}

	if accessRuleRows.Err() != nil {
		log.Println("Problem while reading bucket access rules from database ", err);
		return nil, accessRuleRows.Err();
	}

	accessRuleRows.Close();

	// Fetch the API keys for this bucket (if any).
	bucket.APIKeys = CachedBucketAPIKeyStore{ cache: make(map[[32]byte]*CachedBucketAPIKey) };
	apiKeyRows, err := DB.Query("SELECT id,created_ms,key_hashed FROM bucket_auth_api_keys WHERE bucket_id = ?", bucket.id);
	if err != nil {
		log.Println("Problem while fetching bucket API keys from database ", err);
		return nil, err;
	}

	for apiKeyRows.Next() {
		key := CachedBucketAPIKey{};
		var hashedAPIKeyBlob []byte;
		if err := apiKeyRows.Scan(&key.id, &key.createdMs, &hashedAPIKeyBlob); err != nil {
			apiKeyRows.Close();
			log.Println("Problem while reading bucket API key columns from database ", err);
			return nil, err;
		}

		var hashedAPIKey [32]byte;
		copy(hashedAPIKey[:], hashedAPIKeyBlob);

		bucket.APIKeys.cache[hashedAPIKey] = &key;
	}

	if apiKeyRows.Err() != nil {
		log.Println("Problem while reading bucket API keys from database ", err);
		return nil, apiKeyRows.Err();
	}

	apiKeyRows.Close();

	// Add to the cache.
	// NOTE: Multiple goroutines could get to this point and replace an existing cache entry, but unlike SpeedyGuard, this here has no consequence hence no check is needed.
	nameBucketCacheLock.Lock();
	nameBucketCache[name] = &bucket;
	nameBucketCacheLock.Unlock();

	return &bucket, nil;
}

func (BucketHandler) InitDBTables() {
	var err error;
	
	_, err = DB.Exec(`
		CREATE TABLE IF NOT EXISTS buckets (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name VARCHAR(64) UNIQUE NOT NULL,
			created_ms UNSIGNED BIGINT NOT NULL
		)
	`);
	
	if err != nil {
		log.Fatal("Error while creating buckets table ", err);
	}

	_, err = DB.Exec(`
		CREATE TABLE IF NOT EXISTS bucket_auth_api_keys (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			bucket_id INTEGER NOT NULL,
			name VARCHAR(64) UNIQUE NOT NULL,
			created_ms UNSIGNED BIGINT NOT NULL,
			key_hashed BLOB(32) NOT NULL,

			FOREIGN KEY (bucket_id) REFERENCES buckets (id) ON DELETE CASCADE
		)
	`);

	if err != nil {
		log.Fatal("Error while creating bucket API keys table ", err);
	}

	_, err = DB.Exec("CREATE INDEX idx_bucket_auth_api_keys_bucket_id ON bucket_auth_api_keys(bucket_id)");
	if err != nil {
		log.Fatal("Error while creating bucket API keys index ", err);
	}

	// TODO: When creating a rule, check if regex contains $ and ^ anchors, and warn user if not since rules can pass with partial matches!
	_, err = DB.Exec(`
		CREATE TABLE IF NOT EXISTS bucket_access_rules (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			bucket_id INTEGER NOT NULL,
			priority UNSIGNED INTEGER NOT NULL,
			
			regex TEXT NOT NULL,
			action UNSIGNED TINYINT NOT NULL,

			FOREIGN KEY (bucket_id) REFERENCES buckets (id) ON DELETE CASCADE
		)
	`);

	if err != nil {
		log.Fatal("Error while creating bucket rules table ", err);
	}

	_, err = DB.Exec("CREATE INDEX idx_bucket_access_rules_bucket_id ON bucket_access_rules(bucket_id)");
	if err != nil {
		log.Fatal("Error while creating bucket access rules index ", err);
	}

	// Test rows.

	if config.DEBUG_MODE {
		if _, err := DB.Exec("INSERT INTO buckets(name,created_ms) VALUES(?,?)", "test-bucket", time.Now().UnixMilli()); err != nil {
			log.Fatal("Error while inserting bucket test row ", err);
		}

		keyHash := sha256.Sum256([]byte("canttouchthis"));
		if _, err := DB.Exec("INSERT INTO bucket_auth_api_keys(bucket_id,name,created_ms,key_hashed) VALUES(?,?,?,?)", 1, "Test Key", time.Now().UnixMilli(), keyHash[:]); err != nil {
			log.Fatal("Error while inserting bucket API key test row ", err);
		}
	}
}

type BucketHandler struct{};
var Bucket = BucketHandler{};