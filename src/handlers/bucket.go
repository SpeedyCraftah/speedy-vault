package handlers

import (
	"crypto/sha512"
	"database/sql"
	"encoding/base64"
	"log"
	"path"
	"regexp"
	"speedyvault/src/config"
	. "speedyvault/src/handlers/constants"
	"strconv"
	"sync"
	"time"
)

type CachedBucket struct {
	id int64;
	createdMs int64;

	cachedMs int64;
	APIKeys CachedBucketAPIKeyStore;
	ObjectAuth CachedBucketObjectAuthStore;
	AccessRules []*CachedBucketAccessRule;
}

func (b CachedBucket) GetObjectPath(objectId string) string {
	return path.Join(config.AppConfig.DataDirectory, strconv.FormatInt(b.id, 10), "objects", objectId);
}

func (b CachedBucket) GetKeyAccessCondition(key []byte) BucketAccessRuleAction {
	// Attempt to find a rule that matches this key, and returns its outcome.
	for _, rule := range b.AccessRules {
		if rule.Regex.Match(key) {
			return rule.Action;
		}
	}

	// Default is to allow signed.
	return AllowSigned;
}

type CachedBucketAccessRule struct {
	id int64;
	
	Regex *regexp.Regexp;
	Action BucketAccessRuleAction;
}

type CachedBucketAPIKeyStore struct {
	cache map[[64]byte]*CachedBucketAPIKey;
}

func (store CachedBucketAPIKeyStore) Get(b64Key []byte) *CachedBucketAPIKey {
	// Our keys are always SHA512 digests (64 bytes after decoding), meaning we can make an assumption on the buffer size and error if incorrect.
	if base64.RawStdEncoding.DecodedLen(len(b64Key)) != 64 {
		return nil;
	}

	rawKey := make([]byte, 64);
	if _, err := base64.RawStdEncoding.Decode(rawKey, b64Key); err != nil {
		return nil;
	}

	// Safety: API keys are read-only, hence no locking is required.
	return store.cache[sha512.Sum512(rawKey)];
}

type CachedBucketAPIKey struct {
	id int64;
	createdMs int64;
}

type CachedBucketObjectAuthStore struct {
	MAC map[uint32]*CachedBucketObjectAuthMAC;
}

type CachedBucketObjectAuthMAC struct {
	id int64;
	createdMs int64;

	Secret []byte; 
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

	// Fetch the MAC secrets and selectors for this bucket.
	bucket.ObjectAuth.MAC = make(map[uint32]*CachedBucketObjectAuthMAC);
	objectAuthMACRows, err := DB.Query("SELECT id,selector,secret,created_ms FROM bucket_object_auth_mac WHERE bucket_id = ?", bucket.id);
	if err != nil {
		log.Println("Problem while fetching bucket object MAC authentication entries from database ", err);
		return nil, err;
	}

	for objectAuthMACRows.Next() {
		entry := CachedBucketObjectAuthMAC{};
		var selector uint32;
		if err := objectAuthMACRows.Scan(&entry.id, &selector, &entry.Secret, &entry.createdMs); err != nil {
			objectAuthMACRows.Close();
			log.Println("Problem while reading bucket object MAC authentication entries from database ", err);
			return nil, err;
		}

		bucket.ObjectAuth.MAC[selector] = &entry;
	}

	objectAuthMACRows.Close();

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
		if err := accessRuleRows.Scan(&rule.id, &rawRegex, &rule.Action); err != nil {
			accessRuleRows.Close();
			log.Println("Problem while reading bucket access rules from database ", err);
			return nil, err;
		}

		// Regex would've been validated at insertion time, so fine to panic on error.
		rule.Regex = regexp.MustCompile(rawRegex);

		// Add to the rule list (already in highest to lowest priority order from database).
		bucket.AccessRules = append(bucket.AccessRules, &rule);
	}

	if accessRuleRows.Err() != nil {
		log.Println("Problem while reading bucket access rules from database ", err);
		return nil, accessRuleRows.Err();
	}

	accessRuleRows.Close();

	// Fetch the API keys for this bucket (if any).
	bucket.APIKeys = CachedBucketAPIKeyStore{ cache: make(map[[64]byte]*CachedBucketAPIKey) };
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

		var hashedAPIKey [64]byte;
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
			key_hashed BLOB(64) NOT NULL,

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

	_, err = DB.Exec(`
		CREATE TABLE IF NOT EXISTS bucket_object_auth_mac (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			bucket_id INTEGER NOT NULL,

			created_ms UNSIGNED BIGINT NOT NULL,
			selector UNSIGNED INTEGER NOT NULL,
			secret BLOB(32) NOT NULL,

			FOREIGN KEY (bucket_id) REFERENCES buckets (id) ON DELETE CASCADE,
			UNIQUE (bucket_id, selector)
		)
	`);

	if err != nil {
		log.Fatal("Error while creating bucket object auth mac table ", err);
	}

	// Test rows.

	if config.DEBUG_MODE {
		if _, err := DB.Exec("INSERT INTO buckets(name,created_ms) VALUES(?,?)", "test-bucket", time.Now().UnixMilli()); err != nil {
			log.Fatal("Error while inserting bucket test row ", err);
		}

		keyHash := sha512.Sum512([]byte("canttouchthiscanttouchthissomemrcanttouchthiscanttouchthissomemr"));
		if _, err := DB.Exec("INSERT INTO bucket_auth_api_keys(bucket_id,name,created_ms,key_hashed) VALUES(?,?,?,?)", 1, "Test Key", time.Now().UnixMilli(), keyHash[:]); err != nil {
			log.Fatal("Error while inserting bucket API key test row ", err);
		}

		if _, err := DB.Exec("INSERT INTO bucket_object_auth_mac(bucket_id,selector,secret,created_ms) VALUES(?,?,?,?)", 1, 1, "supersecretobjectsecretthatis32b", time.Now().UnixMilli()); err != nil {
			log.Fatal("Error while inserting bucket object auth MAC test row ", err);
		}
	}
}

type BucketHandler struct{};
var Bucket = BucketHandler{};