/*
 * bwt_fast.c - Fast BWT transform using C for Python ctypes
 *
 * Provides:
 *   - bwt_encode_c:  O(n log n) BWT encoding via qsort
 *   - bwt_decode_c:  O(n) BWT decoding via LF-mapping
 *
 * For cyclic BWT (entire file as one block).
 * Handles inputs up to 256MB.
 */

#include <stdlib.h>
#include <string.h>
#include <stdint.h>

/* Global pointers for qsort comparator (not thread-safe, but fine for compression) */
static const uint8_t *g_data = NULL;
static int32_t g_n = 0;

static int
cyclic_cmp(const void *a, const void *b) {
    int32_t ia = *(const int32_t *)a;
    int32_t ib = *(const int32_t *)b;
    int32_t n = g_n;
    const uint8_t *data = g_data;

    /* Compare cyclic suffixes starting at ia and ib */
    int32_t len = n;
    while (len-- > 0) {
        uint8_t ca = data[ia];
        uint8_t cb = data[ib];
        if (ca != cb) return (int)ca - (int)cb;
        ia++;
        if (ia >= n) ia = 0;
        ib++;
        if (ib >= n) ib = 0;
    }
    return 0;
}

/*
 * Faster comparator using doubled string for small inputs.
 * For inputs <= 16MB, we can create a 2x copy and use memcmp.
 */
static const uint8_t *g_doubled = NULL;
static int32_t g_dbl_n = 0;

static int
dbl_cmp(const void *a, const void *b) {
    int32_t ia = *(const int32_t *)a;
    int32_t ib = *(const int32_t *)b;
    return memcmp(g_doubled + ia, g_doubled + ib, g_dbl_n);
}


/*
 * BWT encode: compute BWT transform of input data.
 *
 * Parameters:
 *   data:     input bytes
 *   n:        length of input
 *   bwt_out:  output buffer (must be pre-allocated, size n)
 *
 * Returns: orig_idx (the row index of the original string in sorted rotations)
 */
int32_t
bwt_encode_c(const uint8_t *data, int32_t n, uint8_t *bwt_out) {
    int32_t *sa;
    int32_t i, orig_idx = -1;

    if (n <= 0) return 0;
    if (n == 1) {
        bwt_out[0] = data[0];
        return 0;
    }

    sa = (int32_t *)malloc(n * sizeof(int32_t));
    if (!sa) return -1;

    for (i = 0; i < n; i++) sa[i] = i;

    if (n <= (1 << 24)) {
        /* For n <= 16MB, use doubled string with memcmp (much faster) */
        uint8_t *doubled = (uint8_t *)malloc(2 * (size_t)n);
        if (doubled) {
            memcpy(doubled, data, n);
            memcpy(doubled + n, data, n);
            g_doubled = doubled;
            g_dbl_n = n;
            qsort(sa, n, sizeof(int32_t), dbl_cmp);
            free(doubled);
            g_doubled = NULL;
        } else {
            /* Fallback to cyclic comparison */
            g_data = data;
            g_n = n;
            qsort(sa, n, sizeof(int32_t), cyclic_cmp);
            g_data = NULL;
        }
    } else {
        /* For very large inputs, use cyclic comparison */
        g_data = data;
        g_n = n;
        qsort(sa, n, sizeof(int32_t), cyclic_cmp);
        g_data = NULL;
    }

    /* Build BWT output and find orig_idx */
    for (i = 0; i < n; i++) {
        if (sa[i] == 0) {
            orig_idx = i;
            bwt_out[i] = data[n - 1];
        } else {
            bwt_out[i] = data[sa[i] - 1];
        }
    }

    free(sa);
    return orig_idx;
}


/*
 * BWT decode: recover original data from BWT transform.
 *
 * Parameters:
 *   bwt_data:  BWT-transformed bytes
 *   n:         length of data
 *   orig_idx:  the row index of the original string
 *   output:    output buffer (must be pre-allocated, size n)
 */
void
bwt_decode_c(const uint8_t *bwt_data, int32_t n, int32_t orig_idx,
             uint8_t *output) {
    int32_t count[256];
    int32_t cumul[256];
    int32_t *lf;
    int32_t i, total, j;
    int32_t occ[256];

    if (n <= 0) return;
    if (n == 1) {
        output[0] = bwt_data[0];
        return;
    }

    /* Count character frequencies */
    memset(count, 0, sizeof(count));
    for (i = 0; i < n; i++) {
        count[bwt_data[i]]++;
    }

    /* Compute cumulative frequencies */
    total = 0;
    for (i = 0; i < 256; i++) {
        cumul[i] = total;
        total += count[i];
    }

    /* Build LF mapping */
    lf = (int32_t *)malloc(n * sizeof(int32_t));
    if (!lf) return;

    memset(occ, 0, sizeof(occ));
    for (i = 0; i < n; i++) {
        uint8_t c = bwt_data[i];
        lf[i] = cumul[c] + occ[c];
        occ[c]++;
    }

    /* Reconstruct original string by following LF mapping */
    j = orig_idx;
    for (i = n - 1; i >= 0; i--) {
        output[i] = bwt_data[j];
        j = lf[j];
    }

    free(lf);
}
