#ifndef MHASH_H
#define MHASH_H

#include "backend.h"
#include "tbchain.h"
#include "lockable.h"

fs_lockable_t *fs_mhash_open(fs_backend *be, const char *label, int flags);
fs_lockable_t *fs_mhash_open_filename(const char *filename, int flags);
int fs_mhash_close(fs_lockable_t *rh);

int fs_mhash_get(fs_lockable_t *hf, const fs_rid rid, fs_index_node *val);
int fs_mhash_get_r(fs_lockable_t *hf, const fs_rid rid, fs_index_node *val);
int fs_mhash_put(fs_lockable_t *hf, const fs_rid rid, fs_index_node val);
int fs_mhash_put_r(fs_lockable_t *hf, const fs_rid rid, fs_index_node val);

/* return number of unique models stored */
int fs_mhash_count(fs_lockable_t *rh);
int fs_mhash_count_r(fs_lockable_t *rh);

/* return a vector of all they keys where the value is non 0 */
fs_rid_vector *fs_mhash_get_keys(fs_lockable_t *hf);
fs_rid_vector *fs_mhash_get_keys_r(fs_lockable_t *hf);

/* write any outstanding data (only headers at present) out to disk buffers */
int fs_mhash_flush(fs_lockable_t *hf);

void fs_mhash_print(fs_lockable_t *hf, FILE *out, int verbosity);
void fs_mhash_print_r(fs_lockable_t *hf, FILE *out, int verbosity);
void fs_mhash_check_chain(fs_lockable_t *hf, fs_tbchain *tbc, FILE *out, int verbosity);
void fs_mhash_check_chain_r(fs_lockable_t *hf, fs_tbchain *tbc, FILE *out, int verbosity);

#endif
