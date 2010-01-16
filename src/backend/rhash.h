#ifndef RHASH_H
#define RHASH_H

#include "backend.h"
#include "lockable.h"

fs_lockable_t *fs_rhash_open(fs_backend *be, const char *label, int flags);
fs_lockable_t *fs_rhash_open_filename(const char *filename, int flags);
int fs_rhash_close(fs_lockable_t *rh);

int fs_rhash_get(fs_lockable_t *rh, fs_resource *res);
int fs_rhash_get_r(fs_lockable_t *rh, fs_resource *res);
int fs_rhash_put(fs_lockable_t *rh, fs_resource *res);
int fs_rhash_put_r(fs_lockable_t *rh, fs_resource *res);

int fs_rhash_get_multi(fs_lockable_t *rh, fs_resource *res, int count);
int fs_rhash_get_multi_r(fs_lockable_t *rh, fs_resource *res, int count);
int fs_rhash_put_multi(fs_lockable_t *rh, fs_resource *res, int count);
int fs_rhash_put_multi_r(fs_lockable_t *rh, fs_resource *res, int count);

void fs_rhash_print(fs_lockable_t *rh, FILE *out, int verbosity);
void fs_rhash_print_r(fs_lockable_t *rh, FILE *out, int verbosity);

/* return number of unique resources stored */
int fs_rhash_count(fs_lockable_t *rh);

#endif
