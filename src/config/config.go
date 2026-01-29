package config

/* TODO: convert this to a loadable JSON config */

type AppConfigType struct {
	// Only use when Nginx (or another compatible reverse proxy) is in front of the backend.
	// Requires some additional Nginx config, but will improve file upload/download performance substantially
	// by offloading upload/download onto Nginx rather than having Nginx proxy everything in-between.
	UseNginxStreaming bool;

	// Does not apply when 'UseNginxStreaming' is enabled.
	// Not to be confused with the maximum size of an object.
	// The maximum size in bytes that a single part can be streamed into an object without having to be split into multiple parts.
	// Parts larger than this will be rejected with a 413 status code once the stream goes over this byte threshold.
	MaxSinglePartSize int;

	// Does not apply when 'UseNginxStreaming' is enabled.
	// The chunk size in bytes to receive and stream into an object at once.
	// Higher values usually improve upload performance at a cost of higher memory (mostly with lots of concurrent uploads).
	// Values too low (especially below a few KBs) will use less memory, but will result in significantly degraded upload performance.
	// Up to a certain point, any performance improvements will stagnate which is determined by the system's IO throughput.
	UploadStreamingChunkSize uint32;

	// Where all the magic happens; the root directory of where parts and objects will be uploaded & stored.
	DataDirectory string;
}

var AppConfig = AppConfigType{ UseNginxStreaming: false, UploadStreamingChunkSize: 2097152, MaxSinglePartSize: 104857600, DataDirectory: "./data" };

// Adjusts behaviour depending on the type of build (e.g. database enters memory mode in debug).
const DEBUG_MODE = true;