#ifndef LIST_H
#define LIST_H

#include "backend.h"
#include "lockable.h"

fs_lockable_t *fs_list_open(fs_backend *be, const char *label, size_t width, int flags);

fs_lockable_t *fs_list_open_filename(const char *filename, size_t width, int flags);

int fs_list_truncate(fs_lockable_t *l);
int fs_list_truncate_r(fs_lockable_t *l);
int fs_list_unlink(fs_lockable_t *l);
int fs_list_close(fs_lockable_t *l);

int32_t fs_list_add(fs_lockable_t *l, const void *data);
int32_t fs_list_add_r(fs_lockable_t *l, const void *data);

void fs_list_rewind_r(fs_lockable_t *l);

int fs_list_next_value_r(fs_lockable_t *l, void *out);
int fs_list_next_sort_uniqed_r(fs_lockable_t *l, void *out);

int fs_list_get(fs_lockable_t *l, int32_t pos, void *data);
int fs_list_get_r(fs_lockable_t *l, int32_t pos, void *data);

int fs_list_length_r(fs_lockable_t *l);

int fs_list_sort(fs_lockable_t *l, int (*comp)(const void *, const void *));
int fs_list_sort_r(fs_lockable_t *l, int (*comp)(const void *, const void *));
int fs_list_sort_chunked(fs_lockable_t *l, int (*comp)(const void *, const void *));
int fs_list_sort_chunked_r(fs_lockable_t *l, int (*comp)(const void *, const void *));

void fs_list_print(fs_lockable_t *l, FILE *out, int verbosity);
void fs_list_print_r(fs_lockable_t *l, FILE *out, int verbosity);

/* vi:set expandtab sts=4 sw=4: */

#endif
