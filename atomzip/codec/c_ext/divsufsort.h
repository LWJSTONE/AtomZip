/*
 * divsufsort - Fast suffix array construction algorithm
 *
 * A simplified implementation suitable for BWT compression.
 * Based on the SA-IS algorithm by Nong, Zhang & Chan (2009)
 * Time complexity: O(n) worst case
 * Space complexity: 5n + O(1)
 *
 * This implementation handles inputs up to 2GB.
 */

#ifndef DIVSUFSORT_H
#define DIVSUFSORT_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Construct the suffix array of a given string.
 * @param T Input string (0-255 byte values)
 * @param SA Output suffix array (must be pre-allocated, size n)
 * @param n Length of input string
 * @return 0 on success, -1 on error
 */
int divsufsort(const uint8_t *T, int32_t *SA, int32_t n);

/*
 * Construct the inverse suffix array (rank array).
 * @param SA Suffix array
 * @param ISA Output inverse suffix array (must be pre-allocated, size n)
 * @param n Length of arrays
 */
void inverse_sa(const int32_t *SA, int32_t *ISA, int32_t n);

/*
 * Compute BWT from suffix array.
 * @param T Input string
 * @param SA Suffix array
 * @param BWT Output BWT string (must be pre-allocated, size n)
 * @param n Length of input
 * @return The index of the original string in the sorted rotation list
 */
int32_t bwt_from_sa(const uint8_t *T, const int32_t *SA, uint8_t *BWT, int32_t n);

/*
 * Inverse BWT (decode).
 * @param BWT Input BWT string
 * @param orig_idx The index returned by bwt_from_sa
 * @param output Output buffer (must be pre-allocated, size n)
 * @param n Length of BWT string
 */
void inverse_bwt(const uint8_t *BWT, int32_t orig_idx, uint8_t *output, int32_t n);

#ifdef __cplusplus
}
#endif

#endif /* DIVSUFSORT_H */
