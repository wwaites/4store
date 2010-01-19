/*
    4store - a clustered RDF storage and query engine

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/
/*
 *  Copyright (C) 2006 Steve Harris for Garlik
 */

#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>
#include <syslog.h>
#include <string.h>
#include <errno.h>
#include <glib.h>
#include <sys/mman.h>
#include <sys/file.h>

#include "list.h"
#include "common/error.h"
#include "common/timing.h"

#define LIST_BUFFER_SIZE 256

/* number of rows in the chunk that will be sorted, CHUNK_SIZE has to
 * be a multiple of the page size */
#define CHUNK_SIZE (131072*4096)
/* use smaller size to test chunk sorting on small lists */
//#define CHUNK_SIZE (4096)
//#define CHUNK_SIZE (4096*1024)

enum sort_state { unsorted, chunk_sorted, sorted };

typedef struct _fs_list {
    fs_lockable_t hf;
    size_t width;
    off_t offset;
    int buffer_pos;
    void *buffer;
    enum sort_state sort;
    int chunks;
    long long count;
    int (*sort_func)(const void *, const void *);
    off_t *chunk_pos;
    off_t *chunk_end;
    void *map;
    void *last;
} fs_list;
#define l_fd hf.fd
#define l_filename hf.filename
#define l_flags hf.flags
#define l_read_metadata hf.read_metadata
#define l_write_metadata hf.write_metadata

static int fs_list_read_metadata(fs_lockable_t *);
static int fs_list_flush(fs_lockable_t *);

fs_lockable_t *fs_list_open(fs_backend *be, const char *label, size_t width, int flags)
{
    char *filename = g_strdup_printf(FS_LIST, fs_backend_get_kb(be), fs_backend_get_segment(be), label);
    fs_lockable_t *l = fs_list_open_filename(filename, width, flags);
    g_free(filename);

    return l;
}

fs_lockable_t *fs_list_open_filename(const char *filename, size_t width, int flags)
{
    if (CHUNK_SIZE % width != 0) {
        fs_error(LOG_CRIT, "width of %s (%lld) does no go into %lld", filename,
                 (long long)width, (long long)CHUNK_SIZE);

        return NULL;
    }
    fs_list *l = calloc(1, sizeof(fs_list));
    fs_lockable_t *hf = (fs_lockable_t *)l;
    l->l_filename = g_strdup(filename);
    l->l_fd = open(filename, FS_O_NOATIME | flags, FS_FILE_MODE);
    l->buffer_pos = 0;
    l->buffer = malloc(LIST_BUFFER_SIZE * width);
    if (l->l_fd == -1) {
        fs_error(LOG_ERR, "failed to open list file '%s': %s", l->l_filename, strerror(errno));
        g_free(l->l_filename);
        free(l->buffer);
        free(l);
        return NULL;
    }

    l->sort = unsorted;
    l->width = width;

    l->l_read_metadata = fs_list_read_metadata;
    l->l_write_metadata = fs_list_flush;

    if (fs_lockable_init(hf)) {
        g_free(l->l_filename);
        free(l->buffer);
        free(l);
        return NULL;
    }
   
    return hf;
}
 
static int fs_list_read_metadata(fs_lockable_t *hf)
{
    fs_list *l = (fs_list *)hf;
    off_t end = lseek(l->l_fd, 0, SEEK_END);
    if (end == -1) {
        fs_error(LOG_CRIT, "failed to open list: %s, cannot seek to end", l->l_filename);
        return -1;
    }
    if (end % l->width != 0) {
        fs_error(LOG_CRIT, "failed to open list: %s, length not multiple of data size", l->l_filename);
        return -1;
    }
    l->offset = end / l->width;

    return 0;
}

static int fs_list_flush(fs_lockable_t *hf)
{
    fs_list *l = (fs_list *)hf;
    
    off_t end = lseek(l->l_fd, l->offset * l->width, SEEK_SET);
    if (end == -1) {
        fs_error(LOG_ERR, "failed to seek to end of list %s: %s", l->l_filename,
                strerror(errno));
        return -1;
    }
    if (l->buffer_pos > 0) {
        int ret = write(l->l_fd, l->buffer, l->width * l->buffer_pos);
        if (ret != l->width * l->buffer_pos) {
            fs_error(LOG_ERR, "failed to write to list %s: %s", l->l_filename,
                    strerror(errno));
            return -1;
        }
    }

    l->buffer_pos = 0;
    l->offset = lseek(l->l_fd, 0, SEEK_END) / l->width;

    return 0;
}

int32_t fs_list_add(fs_lockable_t *hf, const void *data)
{
    int32_t ret;
    if (fs_lockable_lock(hf, LOCK_EX))
        return -1;
    ret = fs_list_add_r(hf, data);
    if (fs_lockable_lock(hf, LOCK_UN))
        return -1;
    return ret;
}

int32_t fs_list_add_r(fs_lockable_t *hf, const void *data)
{
    fs_list *l = (fs_list *)hf;

    fs_assert(fs_lockable_test(hf, LOCK_EX));

    if (l->buffer_pos == LIST_BUFFER_SIZE) {
        int ret = fs_list_flush(hf);
        if (ret != 0) return ret;
    }

    memcpy(l->buffer + l->buffer_pos * l->width, data, l->width);

    l->buffer_pos++;

    return l->offset + l->buffer_pos - 1;
}

int fs_list_get(fs_lockable_t *hf, int32_t pos, void *data)
{
    int ret;
    if (fs_lockable_lock(hf, LOCK_SH))
        return -1;
    ret = fs_list_get_r(hf, pos, data);
    if (fs_lockable_lock(hf, LOCK_UN))
        return -1;
    return ret;
}

int fs_list_get_r(fs_lockable_t *hf, int32_t pos, void *data)
{
    fs_list *l = (fs_list *)hf;

    fs_assert(fs_lockable_test(hf, (LOCK_SH|LOCK_EX)));

    if (pos >= l->offset) {
        /* fetch from buffer */
        if (pos >= (long int)l->offset + (long int)l->buffer_pos) {
            fs_error(LOG_CRIT, "tried to read past end of list %s, "
                     "postition %d/%d", l->l_filename, pos,
                     (int)l->offset + l->buffer_pos);

            return 1;
        }
        memcpy(data, l->buffer + (pos - l->offset) * l->width, l->width);
        return 0;
    }

    if (lseek(l->l_fd, pos * l->width, SEEK_SET) == -1) {
        fs_error(LOG_ERR, "failed to seek to position %zd in %s", pos * l->width, l->l_filename);
        return 1;
    }
    int ret = read(l->l_fd, data, l->width);
    if (ret != l->width) {
        if (ret == -1) {
            fs_error(LOG_CRIT, "failed to read %zd bytes from list %s, position %d, %s", l->width, l->l_filename, pos, strerror(errno));
        } else {
            fs_error(LOG_CRIT, "failed to read %zd bytes from list %s, position %d/%ld, got %d bytes", l->width, l->l_filename, pos, (long int)l->offset, ret);
        }
        return 1;
    }

    return 0;
}

int fs_list_length_r(fs_lockable_t *hf)
{
    fs_list *l = (fs_list *)hf;
    fs_assert(fs_lockable_test(hf, (LOCK_SH|LOCK_EX)));
    return l->offset + l->buffer_pos;
}

void fs_list_rewind_r(fs_lockable_t *hf)
{
    fs_list *l = (fs_list *)hf;
    fs_assert(fs_lockable_test(hf, (LOCK_SH|LOCK_EX)));
    lseek(l->l_fd, 0, SEEK_SET);
}

/* return the next item from a sorted list, uniqs as well */
int fs_list_next_sort_uniqed_r(fs_lockable_t *hf, void *out)
{
    fs_list *l = (fs_list *)hf;
    fs_assert(fs_lockable_test(hf, (LOCK_SH|LOCK_EX)));

    switch (l->sort) {
    case unsorted:
        fs_error(LOG_WARNING, "tried to call %s on unsorted list", __func__);
        return fs_list_next_value_r(hf, out);

    case sorted:
        /* could use fs_list_next_value(l, out) but it will cause duplicates */
    case chunk_sorted:
        break;
    }

    /* initialise if this is the first time were called */
    if (!l->chunk_pos) {
        l->count = 0;
        l->chunks = (l->offset * l->width) / CHUNK_SIZE + 1;
        l->chunk_pos = calloc(l->chunks, sizeof(off_t));
        l->chunk_end = calloc(l->chunks, sizeof(off_t));
        for (int c=0; c<l->chunks; c++) {
            l->chunk_pos[c] = c * CHUNK_SIZE;
            l->chunk_end[c] = (c+1) * CHUNK_SIZE;
        }
        l->chunk_end[l->chunks - 1] = l->offset * l->width;
        long long int chunk_length = 0;
        for (int c=0; c<l->chunks; c++) {
            chunk_length += (l->chunk_end[c] - l->chunk_pos[c]) / l->width;
        }
        if (chunk_length != l->offset) {
            fs_error(LOG_ERR, "length(chunks) = %lld, length(list) = %lld, not sorting", chunk_length, (long long)l->offset);
            free(l->chunk_pos);
            l->chunk_pos = NULL;
            free(l->chunk_end);
            l->chunk_end = NULL;

            return 1;
        }
        l->last = calloc(1, l->width);
        l->map = mmap(NULL, l->offset * l->width, PROT_READ,
                     MAP_FILE | MAP_SHARED, l->l_fd, 0);
    }

again:;
    int best_c = -1;
    for (int c=0; c < l->chunks; c++) {
        if (l->chunk_pos[c] >= l->chunk_end[c]) {
            continue;
        }
        if (best_c == -1 || l->sort_func(l->map + l->chunk_pos[c],
            l->map + l->chunk_pos[best_c]) < 0) {
            best_c = c;
        }
    }
    if (best_c == -1) {
        for (int c=0; c<l->chunks; c++) {
            if (l->chunk_pos[c] != l->chunk_end[c]) {
                fs_error(LOG_ERR, "chunk %d was not sorted to end", c);
            }
        }
        if (l->count != l->offset) {
            fs_error(LOG_ERR, "failed to find low row after %lld/%lld rows", (long long)l->count, (long long)l->offset);
        }

        free(l->chunk_pos);
        l->chunk_pos = NULL;
        free(l->chunk_end);
        l->chunk_end = NULL;
        free(l->last);
        l->last = NULL;
        munmap(l->map, l->offset * l->width);
        l->map = NULL;

        return 0;
    }

    if (bcmp(l->last, l->map + l->chunk_pos[best_c], l->width) == 0) {
        /* it's a duplicate */
        l->chunk_pos[best_c] += l->width;
        (l->count)++;

        goto again;
    } else {
        memcpy(out, l->map + l->chunk_pos[best_c], l->width);
        memcpy(l->last, l->map + l->chunk_pos[best_c], l->width);
        l->chunk_pos[best_c] += l->width;
        (l->count)++;

        return 1;
    }
}

/* it does not make sense to have a locking version of this routine
 * since "next" is undefined outside of the context of a lock */
int fs_list_next_value_r(fs_lockable_t *hf, void *out)
{
    fs_list *l = (fs_list *)hf;

    fs_assert(fs_lockable_test(hf, (LOCK_SH|LOCK_EX)));

    int ret = read(l->l_fd, out, l->width);
    if (ret == -1) {
        fs_error(LOG_ERR, "error reading entry from list: %s\n", strerror(errno));
        return 0;
    } else if (ret == 0) {
        return 0;
    } else if (ret != l->width) {
        fs_error(LOG_ERR, "error reading entry from list, got %d bytes instead of %zd\n", ret, l->width);

        return 0;
    }

    return 1;
}

void fs_list_print(fs_lockable_t *hf, FILE *out, int verbosity)
{
    if (fs_lockable_lock(hf, LOCK_SH))
        return;
    fs_list_print_r(hf, out, verbosity);
    fs_lockable_lock(hf, LOCK_UN);
}

void fs_list_print_r(fs_lockable_t *hf, FILE *out, int verbosity)
{
    fs_list *l = (fs_list *)hf;

    fs_assert(fs_lockable_test(hf, (LOCK_SH|LOCK_EX)));

    fprintf(out, "list of %ld entries\n", (long int)(l->offset + l->buffer_pos));
    if (l->buffer_pos) {
        fprintf(out, "   (%d buffered)\n", l->buffer_pos);
    }
    fprintf(out, "  width %zd bytes\n", l->width);
    fprintf(out, "  sort state: ");
    switch (l->sort) {
    case unsorted:
        fprintf(out, "unsorted\n");
        break;
    case chunk_sorted:
        fprintf(out, "chunk sorted (%lld chunks)\n",
                (long long)(l->offset * l->width) / CHUNK_SIZE + 1);
        break;
    case sorted:
        fprintf(out, "sorted\n");
        break;
    }
    if (verbosity > 0) {
        char buffer[l->width];
        fs_list_rewind_r(hf);
        for (int i=0; i<l->offset; i++) {
            if (l->sort == chunk_sorted && i>0 && i % (CHUNK_SIZE/l->width) == 0) {
                fprintf(out, "--- sort chunk boundary ----\n");
            }
            memset(buffer, 0, l->width);
            int ret = read(l->l_fd, buffer, l->width);
            if (ret == -1) {
                fs_error(LOG_ERR, "error reading entry %d from list: %s\n", i, strerror(errno));
            } else if (ret != l->width) {
                fs_error(LOG_ERR, "error reading entry %d from list, got %d bytes instead of %zd\n", i, ret, l->width);
            }
            if (l->width % sizeof(fs_rid) == 0) {
                volatile fs_rid *row = (fs_rid *)buffer;
                fprintf(out, "%08x", i);
                for (int j=0; j<l->width / sizeof(fs_rid); j++) {
                    fprintf(out, " %016llx", row[j]);
                }
                fprintf(out, "\n");
            }
        }
    }
}

int fs_list_truncate(fs_lockable_t *hf)
{
    int ret;
    if (fs_lockable_lock(hf, LOCK_EX))
        return -1;
    ret = fs_list_truncate_r(hf);
    if (fs_lockable_lock(hf, LOCK_UN))
        return -1;
    return ret;
}

int fs_list_truncate_r(fs_lockable_t *hf)
{
    fs_list *l = (fs_list *)hf;

    fs_assert(fs_lockable_test(hf, LOCK_EX));

    if (ftruncate(l->l_fd, 0) == -1) {
        fs_error(LOG_CRIT, "failed to truncate '%s': %s", l->l_filename, strerror(errno));

        return 1;
    }
    l->offset = 0;
    l->buffer_pos = 0;

    return 0;
}

static int fs_list_sort_chunk(fs_list *l, off_t start, off_t length, int (*comp)(const void *, const void *))
{
    /* map the file so we can access it efficiently */
    void *map = mmap(NULL, length * l->width, PROT_READ | PROT_WRITE,
                     MAP_FILE | MAP_SHARED, l->l_fd, start * l->width);
    if (map == (void *)-1) {
        fs_error(LOG_ERR, "failed to map '%s', %lld+%lld for sort: %s",
                 l->l_filename, (long long)(start * l->width),
                 (long long)(length * l->width), strerror(errno));

        return 1;
    }

    qsort(map, length, l->width, comp);

    munmap(map, length * l->width);

    return 0;
}

int fs_list_sort(fs_lockable_t *hf, int (*comp)(const void *, const void *))
{
    int ret;
    if (fs_lockable_lock(hf, LOCK_EX))
        return -1;
    ret = fs_list_sort_r(hf, comp);
    if (fs_lockable_lock(hf, LOCK_UN))
        return -1;
    return ret;
}

int fs_list_sort_r(fs_lockable_t *hf, int (*comp)(const void *, const void *))
{
    fs_list *l = (fs_list *)hf;

    fs_assert(fs_lockable_test(hf, LOCK_EX));

    /* make sure it's flushed to disk */
    fs_list_flush(hf);
    l->sort_func = comp;

    if (fs_list_sort_chunk(l, 0, l->offset, comp)) {
        return 1;
    }
    l->sort = sorted;

    return 0;
}

int fs_list_sort_chunked(fs_lockable_t *hf, int (*comp)(const void *, const void *))
{
    int ret;
    if (fs_lockable_lock(hf, LOCK_EX))
        return -1;
    ret = fs_list_sort_chunked_r(hf, comp);
    if (fs_lockable_lock(hf, LOCK_UN))
        return -1;
    return ret;
}

int fs_list_sort_chunked_r(fs_lockable_t *hf, int (*comp)(const void *, const void *))
{
    fs_list *l = (fs_list *)hf;

    fs_assert(fs_lockable_test(hf, LOCK_EX));

    /* make sure it's flushed to disk */
    fs_list_flush(hf);
    l->sort_func = comp;

    for (int c=0; c < l->offset; c += CHUNK_SIZE/l->width) {
        off_t length = l->offset - c;
        if (length > CHUNK_SIZE/l->width) length = CHUNK_SIZE/l->width;
        int ret = fs_list_sort_chunk(l, c, length, comp);
        if (ret) {
            fs_error(LOG_ERR, "chunked sort failed at chunk %ld", c / (CHUNK_SIZE/l->width));
            return ret;
        }
    }
    if (l->offset <= CHUNK_SIZE/l->width) {
        l->sort = sorted;
    } else {
        l->sort = chunk_sorted;
    }

    return 0;
}

int fs_list_unlink(fs_lockable_t *hf)
{
//    fs_assert(fs_lockable_test(hf, LOCK_EX));
    return unlink(hf->filename);
}

int fs_list_close(fs_lockable_t *hf)
{
    fs_list *l = (fs_list *)hf;
    int fd = l->l_fd;
    g_free(l->l_filename);
    free(l->buffer);
    free(l);

    return close(fd);
}

/* vi:set expandtab sts=4 sw=4: */
