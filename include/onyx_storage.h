/**
 * onyx-storage C API
 *
 * librbd-style interface to the Onyx userspace block storage engine.
 * Link against libonyx_storage.so / libonyx_storage.dylib.
 */

#ifndef ONYX_STORAGE_H
#define ONYX_STORAGE_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Error codes */
#define ONYX_OK            0
#define ONYX_ERR_NULL     -1
#define ONYX_ERR_INVALID  -2
#define ONYX_ERR_IO       -3
#define ONYX_ERR_NOTFOUND -4
#define ONYX_ERR_NOSPACE  -5
#define ONYX_ERR_CONFIG   -6
#define ONYX_ERR_INTERNAL -7

/* Compression algorithms */
#define ONYX_COMPRESS_NONE 0
#define ONYX_COMPRESS_LZ4  1
#define ONYX_COMPRESS_ZSTD 2

/* Opaque handles */
typedef struct OnyxEngine OnyxEngine;
typedef struct OnyxVolume OnyxVolume;

/* --- Engine lifecycle --- */

/**
 * Open the storage engine with full IO capability.
 * Compression is per-volume (set at create_volume time).
 * @param config_path  Path to TOML config file.
 * @return Engine handle, or NULL on error (call onyx_strerror()).
 */
OnyxEngine *onyx_engine_open(const char *config_path);

/**
 * Open engine in metadata-only mode (no data device needed).
 * Only volume management operations are available.
 */
OnyxEngine *onyx_engine_open_meta(const char *config_path);

/**
 * Close the engine and release all resources.
 */
void onyx_engine_close(OnyxEngine *engine);

/* --- Volume management --- */

/**
 * Create a new volume.
 * @param compression  ONYX_COMPRESS_NONE / ONYX_COMPRESS_LZ4 / ONYX_COMPRESS_ZSTD
 */
int onyx_create_volume(OnyxEngine *engine, const char *name,
                       uint64_t size_bytes, int compression);

/** Delete a volume and free its physical blocks. */
int onyx_delete_volume(OnyxEngine *engine, const char *name);

/**
 * List volumes as JSON array. Caller must free *out_json with onyx_free_string().
 * JSON format: [{"name":"...", "size_bytes":N, "compression":"...", "zone_count":N}, ...]
 */
int onyx_list_volumes(OnyxEngine *engine, char **out_json);

/* --- Volume IO --- */

/**
 * Open a volume for read/write IO.
 * @return Volume handle, or NULL on error.
 */
OnyxVolume *onyx_open_volume(OnyxEngine *engine, const char *name);

/**
 * Write data to a volume at a byte offset.
 * Handles block alignment automatically (RMW for partial blocks).
 */
int onyx_volume_write(OnyxVolume *vol, uint64_t offset,
                      const uint8_t *buf, uint64_t len);

/**
 * Read data from a volume into caller-provided buffer.
 * Unmapped regions return zeros.
 */
int onyx_volume_read(OnyxVolume *vol, uint64_t offset,
                     uint8_t *buf, uint64_t len);

/** Close a volume handle. */
void onyx_volume_close(OnyxVolume *vol);

/* --- Utility --- */

/** Get the last error message (thread-local). Valid until next error. */
const char *onyx_strerror(void);

/** Free a string allocated by onyx (e.g., from onyx_list_volumes). */
void onyx_free_string(char *s);

#ifdef __cplusplus
}
#endif

#endif /* ONYX_STORAGE_H */
