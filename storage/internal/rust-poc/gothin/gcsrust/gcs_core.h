#ifndef GCS_CORE_H
#define GCS_CORE_H
#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

typedef struct GcsClient GcsClient;

typedef struct GcsStatus {
  int32_t code;
  int32_t http_code;
  char *message;
} GcsStatus;

typedef struct GcsReader GcsReader;
typedef struct GcsWriter GcsWriter;
typedef struct GcsObjectAttrs {
  int64_t size;
  int64_t generation;
  int64_t metageneration;
  const char *content_type;
  const char *content_encoding;
} GcsObjectAttrs;

GcsStatus gcs_client_new(const char *endpoint, GcsClient **out);
void gcs_client_free(GcsClient *c);
void gcs_string_free(char *s);
uint64_t gcs_noop(uint64_t x);
uint64_t gcs_callback_bench(uint64_t n, uint64_t (*cb)(void *, uint64_t), void *user);
GcsStatus gcs_read_object(const GcsClient *c, const char *bucket, const char *object,
                          uint64_t offset, uint64_t len, uint8_t *buf, size_t cap,
                          size_t *out_len);
GcsStatus gcs_write_object(const GcsClient *c, const char *bucket, const char *object,
                           const uint8_t *data, size_t len, int64_t *out_generation);
GcsStatus gcs_list_objects(const GcsClient *c, const char *bucket, const char *prefix,
                           char **out_names, size_t *out_count);

typedef void (*GcsDoneCb)(void *user, GcsStatus status, uint64_t n);
void gcs_cancel(uint64_t op_id);
uint64_t gcs_read_object_async(const GcsClient *c, const char *bucket, const char *object,
                               uint64_t offset, uint64_t len, uint8_t *buf, size_t cap,
                               GcsDoneCb cb, void *user);
uint64_t gcs_write_object_async(const GcsClient *c, const char *bucket, const char *object,
                                const uint8_t *data, size_t len, GcsDoneCb cb, void *user);

/* streaming (1:1 facade) */
uint64_t gcs_reader_open_async(const GcsClient *c, const char *bucket, const char *object,
                               int64_t offset, int64_t length, int64_t generation,
                               int64_t if_generation_match, GcsDoneCb cb, void *user);
void gcs_reader_attrs(const GcsReader *r, GcsObjectAttrs *out);
uint64_t gcs_reader_read_async(GcsReader *r, uint8_t *buf, size_t cap, GcsDoneCb cb, void *user);
void gcs_reader_close(GcsReader *r);
GcsStatus gcs_writer_open(const GcsClient *c, const char *bucket, const char *object,
                          const char *content_type, int64_t if_generation_match,
                          size_t chunk_size, int64_t known_size, GcsWriter **out);
uint64_t gcs_writer_write_async(GcsWriter *w, const uint8_t *data, size_t len, GcsDoneCb cb, void *user);
uint64_t gcs_writer_close_async(GcsWriter *w, GcsDoneCb cb, void *user);
int32_t gcs_writer_result(const GcsWriter *w, int64_t *out_size, int64_t *out_generation);
void gcs_writer_free(GcsWriter *w);
GcsStatus gcs_list_page(const GcsClient *c, const char *bucket, const char *prefix,
                        const char *delimiter, const char *start_offset, const char *end_offset,
                        const char *page_token, int32_t page_size, bool versions, char **out_json);
#endif
