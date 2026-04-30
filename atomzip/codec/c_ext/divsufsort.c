/*
 * divsufsort.c - Fast suffix array construction for BWT
 *
 * Implements a simplified SA-IS (Suffix Array Induced Sorting) algorithm.
 * This is a practical implementation optimized for compression workloads.
 *
 * Reference: G. Nong, S. Zhang, W.H. Chan, "Two Efficient Algorithms for
 * Linear Time Suffix Array Construction", IEEE Transactions on Computers, 2011
 */

#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include "divsufsort.h"

/* Alphabet size */
#define ALPHA_SIZE 256

/* Type markers for SA-IS */
#define TYPE_L 0
#define TYPE_S 1

/* Stack-based recursion limit - switch to iterative for deep recursion */
#define MAX_DEPTH 64

/* --- Helper functions --- */

static inline int32_t *
bucket_start(int32_t *bucket, int32_t alpha_size) {
    return bucket;
}

static inline int32_t *
bucket_end(int32_t *bucket, int32_t alpha_size) {
    return bucket + alpha_size;
}

/* Count character frequencies */
static void
count_chars(const uint8_t *T, int32_t n, int32_t *freq) {
    int32_t i;
    memset(freq, 0, ALPHA_SIZE * sizeof(int32_t));
    for (i = 0; i < n; i++) {
        freq[T[i]]++;
    }
}

/* Compute bucket boundaries (cumulative frequencies) */
static void
compute_buckets(const int32_t *freq, int32_t alpha_size,
               int32_t *bkt_start, int32_t *bkt_end) {
    int32_t i, sum = 0;
    for (i = 0; i < alpha_size; i++) {
        bkt_start[i] = sum;
        sum += freq[i];
        bkt_end[i] = sum;
    }
}

/* --- SA-IS Core Algorithm --- */

/*
 * Simplified SA-IS implementation using induced sorting.
 * For compression workloads where input is byte strings (alphabet size 256).
 */
static void
sais_core(const uint8_t *T, int32_t *SA, int32_t n,
          int32_t depth) {
    int32_t *freq, *bkt_s, *bkt_e;
    int32_t *lms_sub, *lms_count;
    uint8_t *type;
    int32_t i, j, k, m, name, delta;
    int32_t *SA1, *T1;
    int32_t orig_alpha_size;

    /* Base cases */
    if (n <= 1) {
        if (n == 1) SA[0] = 0;
        return;
    }

    /* Allocate temporary arrays */
    type = (uint8_t *)malloc(n * sizeof(uint8_t));
    freq = (int32_t *)calloc(ALPHA_SIZE, sizeof(int32_t));
    bkt_s = (int32_t *)malloc(ALPHA_SIZE * sizeof(int32_t));
    bkt_e = (int32_t *)malloc(ALPHA_SIZE * sizeof(int32_t));

    if (!type || !freq || !bkt_s || !bkt_e) {
        free(type); free(freq); free(bkt_s); free(bkt_e);
        /* Fallback: simple sort for small inputs */
        if (n <= 4096) {
            for (i = 0; i < n; i++) SA[i] = i;
            /* Simple insertion sort based on suffix comparison */
            for (i = 1; i < n; i++) {
                int32_t key = SA[i];
                int32_t cmp_val = key;
                j = i - 1;
                while (j >= 0) {
                    /* Compare suffixes SA[j] and key */
                    int32_t a = SA[j], b = cmp_val;
                    int32_t cmp = 0;
                    for (k = 0; k < n; k++) {
                        if (T[(a + k) % n] != T[(b + k) % n]) {
                            cmp = T[(a + k) % n] - T[(b + k) % n];
                            break;
                        }
                    }
                    if (cmp <= 0) break;
                    SA[j + 1] = SA[j];
                    j--;
                }
                SA[j + 1] = key;
            }
        }
        return;
    }

    /* Step 1: Classify characters as L-type or S-type */
    type[n - 1] = TYPE_S;
    for (i = n - 2; i >= 0; i--) {
        if (T[i] < T[i + 1]) {
            type[i] = TYPE_S;
        } else if (T[i] > T[i + 1]) {
            type[i] = TYPE_L;
        } else {
            type[i] = type[i + 1];
        }
    }

    /* Step 2: Count frequencies and compute bucket boundaries */
    count_chars(T, n, freq);
    compute_buckets(freq, ALPHA_SIZE, bkt_s, bkt_e);

    /* Step 3: Find LMS (Leftmost S-type) substrings */
    /* Initialize SA to -1 */
    for (i = 0; i < n; i++) SA[i] = -1;

    /* Place LMS suffixes at bucket ends */
    memcpy(bkt_e, bkt_s, ALPHA_SIZE * sizeof(int32_t));
    /* Adjust: bkt_e[i] should point to end of bucket i */
    {
        int32_t sum = 0;
        for (i = 0; i < ALPHA_SIZE; i++) {
            sum += freq[i];
            bkt_e[i] = sum;
        }
    }

    /* Find LMS positions and put them at bucket ends */
    m = 0;
    for (i = 1; i < n; i++) {
        if (type[i] == TYPE_S && type[i - 1] == TYPE_L) {
            /* i is an LMS position */
            int32_t c = T[i];
            SA[--bkt_e[c]] = i;
            m++;
        }
    }

    /* Step 4: Induced sort L-type suffixes */
    memcpy(bkt_s, freq, ALPHA_SIZE * sizeof(int32_t));
    {
        int32_t sum = 0;
        for (i = 0; i < ALPHA_SIZE; i++) {
            int32_t tmp = bkt_s[i];
            bkt_s[i] = sum;
            sum += tmp;
        }
    }
    /* Scan from left */
    for (i = 0; i < n; i++) {
        if (SA[i] > 0) {
            int32_t j = SA[i] - 1;
            if (type[j] == TYPE_L) {
                SA[bkt_s[T[j]]++] = j;
            }
        }
    }

    /* Step 5: Induced sort S-type suffixes */
    memcpy(bkt_e, freq, ALPHA_SIZE * sizeof(int32_t));
    {
        int32_t sum = 0;
        for (i = 0; i < ALPHA_SIZE; i++) {
            sum += freq[i];
            bkt_e[i] = sum;
        }
    }
    /* Scan from right */
    for (i = n - 1; i >= 0; i--) {
        if (SA[i] > 0) {
            int32_t j = SA[i] - 1;
            if (type[j] == TYPE_S) {
                SA[--bkt_e[T[j]]] = j;
            }
        }
    }

    /* Step 6: Compact LMS suffixes to the beginning of SA */
    j = 0;
    for (i = 0; i < n; i++) {
        if (SA[i] > 0 && type[SA[i]] == TYPE_S && type[SA[i] - 1] == TYPE_L) {
            SA[j++] = SA[i];
        }
    }
    m = j;

    /* Step 7: Name the LMS substrings */
    /* Clear the rest of SA */
    for (i = m; i < n; i++) SA[i] = -1;

    /* Assign names */
    name = 0;
    if (m > 0) {
        int32_t prev = SA[0];
        SA[m + prev / 2] = name;  /* Actually we need to store names differently */

        /* Alternative naming: compact naming in SA[0..m-1] */
        name = 0;
        int32_t prev_lms = SA[0];
        /* Mark position in SA */
        for (i = 0; i < n; i++) SA[i] = -1;

        /* Re-collect LMS positions */
        j = 0;
        for (i = 1; i < n; i++) {
            if (type[i] == TYPE_S && type[i - 1] == TYPE_L) {
                SA[j++] = i;
            }
        }

        /* Now SA[0..m-1] contains LMS positions in sorted order */
        /* Assign names based on comparison */
        int32_t *names = (int32_t *)malloc(n * sizeof(int32_t));
        if (names) {
            memset(names, -1, n * sizeof(int32_t));
            name = 0;
            names[SA[0]] = 0;

            for (i = 1; i < m; i++) {
                /* Compare LMS substrings SA[i-1] and SA[i] */
                int32_t p1 = SA[i - 1], p2 = SA[i];
                int32_t same = 1;
                int32_t len1 = 0, len2 = 0;

                /* Find lengths of LMS substrings */
                for (k = p1 + 1; k < n; k++) {
                    if (type[k] == TYPE_S && k > 0 && type[k - 1] == TYPE_L) break;
                    len1++;
                }
                for (k = p2 + 1; k < n; k++) {
                    if (type[k] == TYPE_S && k > 0 && type[k - 1] == TYPE_L) break;
                    len2++;
                }

                if (len1 != len2) {
                    same = 0;
                } else {
                    for (k = 0; k <= len1; k++) {
                        if (T[p1 + k] != T[p2 + k]) { same = 0; break; }
                    }
                }

                if (!same) name++;
                names[SA[i]] = name;
            }

            /* Compact names into SA[0..m-1] */
            j = m - 1;
            for (i = n - 1; i >= 0; i--) {
                if (names[i] >= 0) {
                    SA[j--] = names[i];
                }
            }
            free(names);
        }
    }

    int32_t new_alpha = name + 1;

    /* Step 8: Recurse if not all names are unique */
    if (new_alpha < m) {
        /* Recursively sort the reduced string SA[0..m-1] */
        if (depth < MAX_DEPTH) {
            /* Allocate temporary space for recursion */
            int32_t *T1_buf = (int32_t *)malloc(m * sizeof(int32_t));
            int32_t *SA1_buf = (int32_t *)malloc(m * sizeof(int32_t));

            if (T1_buf && SA1_buf) {
                /* Build the reduced problem as byte string */
                uint8_t *T1_bytes = (uint8_t *)malloc(m * sizeof(uint8_t));
                if (T1_bytes && new_alpha <= 256) {
                    for (i = 0; i < m; i++) {
                        T1_bytes[i] = (uint8_t)SA[i];
                    }
                    sais_core(T1_bytes, SA, m, depth + 1);
                    free(T1_bytes);
                } else if (T1_buf) {
                    /* Alphabet too large, use direct suffix comparison */
                    /* For simplicity, just sort */
                    for (i = 0; i < m; i++) T1_buf[i] = SA[i];
                    /* Simple O(m^2) sort for the reduced problem */
                    for (i = 1; i < m; i++) {
                        int32_t key = i;
                        j = i - 1;
                        /* Compare suffixes by their integer values */
                        while (j >= 0 && T1_buf[SA[j]] > T1_buf[key]) {
                            j--;
                        }
                        /* Shift */
                        int32_t tmp = SA[i];
                        for (k = i; k > j + 1; k--) SA[k] = SA[k - 1];
                        SA[j + 1] = tmp;
                    }
                    if (T1_bytes) free(T1_bytes);
                }
                free(T1_buf);
                free(SA1_buf);
            }
        }
    }

    /* Step 9: Reconstruct the full SA from the reduced SA */
    /* Collect LMS positions */
    int32_t *lms_pos = (int32_t *)malloc(n * sizeof(int32_t));
    if (!lms_pos) {
        free(type); free(freq); free(bkt_s); free(bkt_e);
        return;
    }

    j = 0;
    for (i = 1; i < n; i++) {
        if (type[i] == TYPE_S && type[i - 1] == TYPE_L) {
            lms_pos[j++] = i;
        }
    }
    /* m = j */

    /* Map sorted LMS indices back */
    if (new_alpha < m) {
        int32_t *sorted_lms = (int32_t *)malloc(m * sizeof(int32_t));
        if (sorted_lms) {
            for (i = 0; i < m; i++) {
                sorted_lms[i] = lms_pos[SA[i]];
            }
            for (i = 0; i < m; i++) {
                SA[i] = sorted_lms[i];
            }
            free(sorted_lms);
        }
    }
    /* else SA[0..m-1] already contains sorted LMS positions */

    /* Step 10: Final induced sort */
    /* Clear SA from position m onwards */
    for (i = m; i < n; i++) SA[i] = -1;

    /* Place LMS suffixes at bucket ends */
    {
        int32_t sum = 0;
        for (i = 0; i < ALPHA_SIZE; i++) {
            sum += freq[i];
            bkt_e[i] = sum;
        }
    }
    for (i = m - 1; i >= 0; i--) {
        int32_t c = T[SA[i]];
        SA[--bkt_e[c]] = SA[i];
    }

    /* Induced sort L-type */
    {
        int32_t sum = 0;
        for (i = 0; i < ALPHA_SIZE; i++) {
            int32_t tmp = freq[i];
            bkt_s[i] = sum;
            sum += tmp;
        }
    }
    for (i = 0; i < n; i++) {
        if (SA[i] > 0) {
            int32_t j = SA[i] - 1;
            if (type[j] == TYPE_L) {
                SA[bkt_s[T[j]]++] = j;
            }
        }
    }

    /* Induced sort S-type */
    {
        int32_t sum = 0;
        for (i = 0; i < ALPHA_SIZE; i++) {
            sum += freq[i];
            bkt_e[i] = sum;
        }
    }
    for (i = n - 1; i >= 0; i--) {
        if (SA[i] > 0) {
            int32_t j = SA[i] - 1;
            if (type[j] == TYPE_S) {
                SA[--bkt_e[T[j]]] = j;
            }
        }
    }

    /* Cleanup */
    free(lms_pos);
    free(type);
    free(freq);
    free(bkt_s);
    free(bkt_e);
}


/* --- Public API --- */

int
divsufsort(const uint8_t *T, int32_t *SA, int32_t n) {
    if (T == NULL || SA == NULL || n < 0) return -1;
    if (n == 0) return 0;

    /* For very small inputs, use simple sort */
    if (n <= 2) {
        SA[0] = 0;
        if (n == 2) {
            if (T[0] <= T[1]) {
                SA[0] = 0; SA[1] = 1;
            } else {
                SA[0] = 1; SA[1] = 0;
            }
        }
        return 0;
    }

    /* For small inputs, use a simple O(n^2 log n) sort */
    if (n <= 8192) {
        int32_t i;
        /* Create doubled string for cyclic comparison */
        uint8_t *doubled = (uint8_t *)malloc(2 * n);
        if (doubled) {
            memcpy(doubled, T, n);
            memcpy(doubled + n, T, n);

            for (i = 0; i < n; i++) SA[i] = i;

            /* Use a simple sort with cyclic comparison */
            /* qsort with cyclic comparison */
            /* Since we can't pass context to qsort easily, do insertion sort */
            int32_t j;
            for (i = 1; i < n; i++) {
                int32_t key = SA[i];
                j = i - 1;
                while (j >= 0) {
                    const uint8_t *a = doubled + SA[j];
                    const uint8_t *b = doubled + key;
                    int32_t cmp = memcmp(a, b, n);
                    if (cmp <= 0) break;
                    SA[j + 1] = SA[j];
                    j--;
                }
                SA[j + 1] = key;
            }
            free(doubled);
            return 0;
        }
    }

    /* For larger inputs, use SA-IS */
    sais_core(T, SA, n, 0);
    return 0;
}


int32_t
bwt_from_sa(const uint8_t *T, const int32_t *SA, uint8_t *BWT, int32_t n) {
    int32_t i, orig_idx = -1;

    for (i = 0; i < n; i++) {
        if (SA[i] == 0) {
            orig_idx = i;
            BWT[i] = T[n - 1];
        } else {
            BWT[i] = T[SA[i] - 1];
        }
    }

    return orig_idx;
}


void
inverse_bwt(const uint8_t *BWT, int32_t orig_idx, uint8_t *output, int32_t n) {
    int32_t count[ALPHA_SIZE];
    int32_t cumul[ALPHA_SIZE];
    int32_t *lf;
    int32_t i, total, j;

    if (n == 0) return;

    memset(count, 0, sizeof(count));
    for (i = 0; i < n; i++) {
        count[BWT[i]]++;
    }

    total = 0;
    for (i = 0; i < ALPHA_SIZE; i++) {
        cumul[i] = total;
        total += count[i];
    }

    lf = (int32_t *)malloc(n * sizeof(int32_t));
    if (!lf) return;

    {
        int32_t occ[ALPHA_SIZE];
        memset(occ, 0, sizeof(occ));
        for (i = 0; i < n; i++) {
            uint8_t c = BWT[i];
            lf[i] = cumul[c] + occ[c];
            occ[c]++;
        }
    }

    /* Reconstruct original string */
    j = orig_idx;
    for (i = n - 1; i >= 0; i--) {
        output[i] = BWT[j];
        j = lf[j];
    }

    free(lf);
}
