// privacy-filter.cpp — minimal GGML engine for the openai-privacy-filter
// token-classification model family (PII NER).
//
// Flat C API: no exceptions cross this boundary, every pointer-returning
// function reports failure via pf_last_error, every free is NULL-safe.
// Shaped for FFI (purego et al.): opaque handle, caller-owned flat buffers.
#ifndef PF_H
#define PF_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define PF_ABI_VERSION 1

typedef struct pf_ctx pf_ctx;

// Byte offsets into the original UTF-8 text. label points into ctx-owned
// memory and is valid until pf_free.
typedef struct {
    int32_t      start;
    int32_t      end;
    float        score;
    const char * label;
} pf_entity;

int pf_abi_version(void);

// device: NULL or "cpu", "gpu", "cuda", "vulkan" (optionally ":N" to pick the
// Nth matching GPU). "gpu" = first GPU of whichever backend was compiled in.
// n_threads <= 0 picks a default (CPU only).
pf_ctx *     pf_load(const char * gguf_path, const char * device, int n_threads);
void         pf_free(pf_ctx * ctx);
const char * pf_last_error(const pf_ctx * ctx);

// Max tokens per forward pass (default 4096). Longer inputs run as
// overlapping windows; must be > 2 * 1024 (the stitch halo) to window.
void pf_set_window(pf_ctx * ctx, int32_t max_forward_tokens);

// text -> entities. Returns 0 on success; *out is malloc'd, free with
// pf_entities_free. Entities scoring below threshold are dropped.
//
// NOT thread-safe: a pf_ctx must not be used for concurrent pf_classify calls
// (the gallocr forward-pass buffer is reused without locking). Hold one handle
// and classify single-threaded, or use one handle per thread.
int  pf_classify(pf_ctx * ctx, const char * text, size_t len, float threshold,
                 pf_entity ** out, size_t * n_out);
void pf_entities_free(pf_entity * ents, size_t n);

// Lower-level entry points, for tests and FFI consumers.
// pf_tokenize: *offsets holds 2n int32 (start,end byte pairs).
int  pf_tokenize(pf_ctx * ctx, const char * text, size_t len,
                 int32_t ** ids, int32_t ** offsets, size_t * n);
// pf_logits: *logits holds n * n_labels floats (per-token classifier logits).
int  pf_logits(pf_ctx * ctx, const int32_t * ids, size_t n, float ** logits);
void pf_buf_free(void * buf);

#ifdef __cplusplus
}
#endif

#endif // PF_H
