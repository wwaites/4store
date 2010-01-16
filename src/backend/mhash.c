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
#ifndef __APPLE__
#define _XOPEN_SOURCE 500
#endif
#include <stdlib.h>
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>
#include <glib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/file.h>
#include <assert.h>

#include "backend.h"
#include "mhash.h"
#include "tbchain.h"
#include "common/params.h"
#include "common/hash.h"
#include "common/error.h"

#define FS_MHASH_DEFAULT_LENGTH         4096
#define FS_MHASH_DEFAULT_SEARCH_DIST      16

#define FS_MHASH_ID 0x4a584d30

/* maximum distance that we wil allow the resource to be from its hash value */

#define FS_PACKED __attribute__((__packed__))

#define FS_MHASH_ENTRY(mh, rid) ((rid >> 10) & (mh->mh_size - 1))

struct mhash_header {
    int32_t id;             // "JXM0"
    int32_t size;           // size of hashtable, must be power of two
    int32_t count;          // number of models in hashtable
    int32_t search_dist;    // offset to scan up to in table for match
    char padding[496];      // allign to a block
} FS_PACKED;
 
typedef struct _fs_mhash {
    fs_lockable_t hf;
    int32_t mh_size;
    int32_t mh_count;
    int32_t mh_search_dist;
} fs_mhash;
#define mh_fd hf.fd
#define mh_flags hf.flags
#define mh_filename hf.filename
#define mh_read_metadata hf.read_metadata
#define mh_write_metadata hf.write_metadata

typedef struct _fs_mhash_entry {
    fs_rid rid;
    fs_index_node val;        // 0 = unused, 1 = in seperate file, 2+ = in list
} FS_PACKED fs_mhash_entry;

static int double_size(fs_mhash *mh);
static int fs_mhash_write_header(fs_lockable_t *hf);
static int fs_mhash_read_header(fs_lockable_t *hf);

fs_lockable_t *fs_mhash_open(fs_backend *be, const char *label, int flags)
{
    char *filename = g_strdup_printf(FS_MHASH, fs_backend_get_kb(be),
                                     fs_backend_get_segment(be), label);
    fs_lockable_t *hf = fs_mhash_open_filename(filename, flags);
    g_free(filename);

    return hf;
}

fs_lockable_t *fs_mhash_open_filename(const char *filename, int flags)
{
    struct mhash_header header;
    fs_mhash *mh;

    /* sanity checking, probably optimised away by the compiler */
    if (sizeof(header) != 512) {
        fs_error(LOG_CRIT, "incorrect mhash header size %zd, should be 512",
                 sizeof(header));
        return NULL;
    }
    if (sizeof(fs_mhash_entry) != 12) {
        fs_error(LOG_CRIT, "incorrect entry size %zd, should be 12",
                 sizeof(fs_mhash_entry));
        return NULL;
    }

    /* allocate our data structure */
    mh = calloc(1, sizeof(fs_mhash));
    if (!mh) {
        fs_error(LOG_CRIT, "could not allocate memory");
        return NULL;
    }
    mh->mh_filename = g_strdup(filename);
    if (!mh->mh_filename) {
        fs_error(LOG_CRIT, "could not allocate memory");
        free(mh);
        return NULL;
    }
    mh->mh_fd = open(filename, FS_O_NOATIME | flags, FS_FILE_MODE);
    mh->mh_flags = flags;
    if (mh->mh_fd < 0) {
        fs_error(LOG_ERR, "cannot open mhash file '%s': %s", filename, strerror(errno));
        g_free(mh->mh_filename);
        free(mh);
        return NULL;
    }
    mh->mh_size = FS_MHASH_DEFAULT_LENGTH;
    mh->mh_search_dist = FS_MHASH_DEFAULT_SEARCH_DIST;
    mh->mh_read_metadata = fs_mhash_read_header;
    mh->mh_write_metadata = fs_mhash_write_header;

    if (fs_lockable_init((fs_lockable_t *)mh)) {
        g_free(mh->mh_filename);
        free(mh);
        return NULL;
    }

    return (fs_lockable_t *)mh;
}

/* read_metadata method for hashfile */
static int fs_mhash_read_header(fs_lockable_t *hf)
{
    struct mhash_header header;
    fs_mhash *mh = (fs_mhash *)hf;
    size_t n;

    assert(mh);

    if ((n = pread(mh->mh_fd, &header, sizeof(header), 0)) != sizeof(header)) {
        fs_error(LOG_ERR, "%s read %d bytes of header should be %d: %s",
                 mh->mh_filename, (int)n, (int)sizeof(header), strerror(errno));
        return -1;
    }

    if (header.id != FS_MHASH_ID) {
        fs_error(LOG_ERR, "%s does not appear to be a mhash file", mh->mh_filename);
        return -1;
    }

    mh->mh_size = header.size;
    mh->mh_count = header.count;
    mh->mh_search_dist = header.search_dist;

    return 0;
}

/* write_metadata method for hashfile */
static int fs_mhash_write_header(fs_lockable_t *hf)
{
    struct mhash_header header;
    fs_mhash *mh = (fs_mhash *)hf;

    assert(mh);

    header.id = FS_MHASH_ID;
    header.size = mh->mh_size;
    header.count = mh->mh_count;
    header.search_dist = mh->mh_search_dist;
    memset(&header.padding, 0, sizeof(header.padding));
    if (pwrite(mh->mh_fd, &header, sizeof(header), 0) != sizeof(header)) {
        fs_error(LOG_CRIT, "failed to write header on %s: %s",
                 mh->mh_filename, strerror(errno));
        return -1;
    }

    return 0;
}

int fs_mhash_close(fs_lockable_t *hf)
{
    fs_mhash *mh = (fs_mhash *)hf;
    close(mh->mh_fd);
    g_free(mh->mh_filename);
    free(mh);

    return 0;
}

int fs_mhash_put_r(fs_lockable_t *hf, const fs_rid rid, fs_index_node val)
{
    fs_mhash *mh = (fs_mhash *)hf;
    int entry = FS_MHASH_ENTRY(mh, rid);
    fs_mhash_entry e;
    int candidate = -1;
    for (int i=0; 1; i++) {
        e.rid = 0;
        e.val = 0;
        if (pread(mh->mh_fd, &e, sizeof(e), sizeof(struct mhash_header) +
                  entry * sizeof(e)) == -1) {
            fs_error(LOG_CRIT, "read from %s failed: %s", mh->mh_filename,
                     strerror(errno));

            return 1;
        }
        if (e.rid == rid) {
            /* model is allready there, replace value */

            break;
        } else if (e.rid == 0 && candidate == -1) {
            /* we can't break here because there might be a mathcing entry
             * later in the hashtable */
            candidate = entry;
        }
        if ((i == mh->mh_search_dist || entry == mh->mh_size - 1) &&
            candidate != -1) {
            /* we can use the candidate we found earlier */
            entry = candidate;
            if (pread(mh->mh_fd, &e, sizeof(e), sizeof(struct mhash_header) +
                      entry * sizeof(e)) == -1) {
                fs_error(LOG_CRIT, "read from %s failed: %s", mh->mh_filename,
                         strerror(errno));

                return 1;
            }

            break;
        }
        if (i == mh->mh_search_dist || entry == mh->mh_size - 1) {
            /* model hash overful, grow */
            double_size(mh);

            return fs_mhash_put_r(hf, rid, val);
        }
        entry++;
    }

    /* if there's no changes to be made we don't want to write anything */
    if (e.rid == rid && e.val == val) return 0;

    fs_index_node oldval = e.val;

    e.rid = rid;
    e.val = val;
    if (pwrite(mh->mh_fd, &e, sizeof(e), sizeof(struct mhash_header) +
               entry * sizeof(e)) == -1) {
        fs_error(LOG_CRIT, "write to %s failed: %s", mh->mh_filename,
                 strerror(errno));

        return 1;
    }
    if (val) {
        if (!oldval) mh->mh_count++;
    } else {
        if (oldval) mh->mh_count--;
    }

    return 0;
}

int fs_mhash_put(fs_lockable_t *hf, const fs_rid rid, fs_index_node val)
{
    int ret;
    if (fs_lockable_lock(hf, LOCK_EX))
        return -1;
    ret = fs_mhash_put_r(hf, rid, val);
    fs_lockable_sync(hf);
    if (fs_lockable_lock(hf, LOCK_UN))
        return -1;
    return ret;
}

static int double_size(fs_mhash *mh)
{
    int32_t oldsize = mh->mh_size;
    int errs = 0;

    mh->mh_size *= 2;
    mh->mh_search_dist *= 2;
    mh->mh_search_dist++;
    fs_mhash_entry blank;
    memset(&blank, 0, sizeof(blank));
    for (int i=0; i<oldsize; i++) {
        fs_mhash_entry e;
        pread(mh->mh_fd, &e, sizeof(e), sizeof(struct mhash_header) + i * sizeof(e));
        if (e.rid == 0) continue;
        int entry = FS_MHASH_ENTRY(mh, e.rid);
        if (entry >= oldsize) {
            if (pwrite(mh->mh_fd, &blank, sizeof(blank),
                       sizeof(struct mhash_header) + i * sizeof(e)) == -1) {
                fs_error(LOG_CRIT, "failed to write mhash '%s' entry: %s",
                         mh->mh_filename, strerror(errno));
                errs++;
            }
            if (pwrite(mh->mh_fd, &e, sizeof(e),
                       sizeof(struct mhash_header) +
                       (oldsize+i) * sizeof(e)) == -1) {
                fs_error(LOG_CRIT, "failed to write mhash '%s' entry: %s",
                         mh->mh_filename, strerror(errno));
                errs++;
            }
        }
    }

    return errs;
}

int fs_mhash_get_r(fs_lockable_t *hf, const fs_rid rid, fs_index_node *val)
{
    fs_mhash *mh = (fs_mhash *)hf;
    int entry = FS_MHASH_ENTRY(mh, rid);
    fs_mhash_entry e;
    memset(&e, 0, sizeof(e));

    for (int i=0; i<mh->mh_search_dist; i++) {
        if (pread(mh->mh_fd, &e, sizeof(e), sizeof(struct mhash_header) + entry * sizeof(e)) < 0) {
            fs_error(LOG_CRIT, "read from %s failed: %s", mh->mh_filename, strerror(errno));

            return 1;
        }
        if (e.rid == rid) {
            *val = e.val;

            return 0;
        }
        entry = (entry + 1) & (mh->mh_size - 1);
        if (entry == 0) break;
    }

    *val = 0;

    return 0;
}

int fs_mhash_get(fs_lockable_t *hf, const fs_rid rid, fs_index_node *val)
{
    int ret;
    if (fs_lockable_lock(hf, LOCK_SH))
        return -1;
    ret = fs_mhash_get_r(hf, rid, val);
    if (fs_lockable_lock(hf, LOCK_UN))
        return -1;
    return ret;
}

fs_rid_vector *fs_mhash_get_keys_r(fs_lockable_t *hf)
{
    fs_mhash *mh = (fs_mhash *)hf;
    fs_rid_vector *v = fs_rid_vector_new(0);
    fs_mhash_entry e;

    if (!mh) {
        fs_error(LOG_CRIT, "tried to get keys from NULL mhash");

        return NULL;
    }

    v = fs_rid_vector_new(0);
    if (!v)
        return NULL;

    if (lseek(mh->mh_fd, sizeof(struct mhash_header), SEEK_SET) == -1) {
        fs_error(LOG_ERR, "seek error on mhash: %s", strerror(errno));
    }
    while (read(mh->mh_fd, &e, sizeof(e)) == sizeof(e)) {
        if (e.val) fs_rid_vector_append(v, e.rid);
    }

    return v;
}

fs_rid_vector *fs_mhash_get_keys(fs_lockable_t *hf)
{
    fs_rid_vector *ret;
    if (fs_lockable_lock(hf, LOCK_SH))
        return NULL;
    ret = fs_mhash_get_keys_r(hf);
    if (fs_lockable_lock(hf, LOCK_UN)) {
        if (ret)
            fs_rid_vector_free(ret);
        return NULL;
    }
    return ret;
}

void fs_mhash_check_chain_r(fs_lockable_t *hf, fs_tbchain *tbc, FILE *out, int verbosity)
{
    fs_mhash *mh = (fs_mhash *)hf;

    if (!mh) {
        fs_error(LOG_CRIT, "tried to print NULL mhash");

        return;
    }
    fs_mhash_entry e;
    int entry = 0;
    int count = 0;

    lseek(mh->mh_fd, sizeof(struct mhash_header), SEEK_SET);
    while (read(mh->mh_fd, &e, sizeof(e)) == sizeof(e)) {
        if (e.rid && e.val) {
            count++;
            fprintf(out, "%016llx %8d:\n", e.rid, e.val);
            if (verbosity > 0) {
                fs_tbchain_get_stats(tbc, e.val, out);
            }
            if (fs_tbchain_check_consistency(tbc, e.rid, e.val, out)) {
                printf("check failed\n");
            }
        }
        entry++;
    }
    if (count && fs_tbchain_check_leaks(tbc, out)) {
        printf("check failed\n");
    }

    if (mh->mh_count != count) {
        fprintf(out, "ERROR: %s header count %d != scanned count %d\n",
                mh->mh_filename, mh->mh_count, count);
    }
}

void fs_mhash_check_chain(fs_lockable_t *hf, fs_tbchain *tbc, FILE *out, int verbosity)
{
    if (fs_lockable_lock(hf, LOCK_SH))
        return;
    fs_mhash_check_chain_r(hf, tbc, out, verbosity);
    fs_lockable_lock(hf, LOCK_UN);
}

void fs_mhash_print_r(fs_lockable_t *hf, FILE *out, int verbosity)
{
    fs_mhash *mh = (fs_mhash *)hf;
    if (!mh) {
        fs_error(LOG_CRIT, "tried to print NULL mhash");
        return;
    }
    fs_mhash_entry e;
    fs_rid_vector *models = fs_rid_vector_new(0);
    fs_rid last_model = FS_RID_NULL;
    int entry = 0;
    int count = 0;

    fprintf(out, "mhash %s\n", mh->mh_filename);
    fprintf(out, "  count: %d\n", mh->mh_count);
    fprintf(out, "  size: %d\n", mh->mh_size);
    fprintf(out, "\n");

    lseek(mh->mh_fd, sizeof(struct mhash_header), SEEK_SET);
    while (read(mh->mh_fd, &e, sizeof(e)) == sizeof(e)) {
        if (e.val) {
            count++;
            if (verbosity > 0) {
                fprintf(out, "%8d %016llx %8d\n", entry, e.rid, e.val);
            }
            fs_rid_vector_append(models, e.rid);
            if (e.rid == last_model) {
                fprintf(out, "ERROR: %s model %016llx appears multiple times\n",
                        mh->mh_filename, e.rid);
            }
            last_model = e.rid;
        }
        entry++;
    }

    if (mh->mh_count != count) {
        fprintf(out, "ERROR: %s header count %d != scanned count %d\n",
                mh->mh_filename, mh->mh_count, count);
    }

    int oldlength = models->length;
    fs_rid_vector_sort(models);
    fs_rid_vector_uniq(models, 0);
    if (models->length != oldlength) {
        fprintf(out, "ERROR: %s some models appear > 1 time\n",
                mh->mh_filename);
    }
}

void fs_mhash_print(fs_lockable_t *hf, FILE *out, int verbosity)
{
    if (fs_lockable_lock(hf, LOCK_SH))
        return;
    fs_mhash_print_r(hf, out, verbosity);
    fs_lockable_lock(hf, LOCK_UN);
}

int fs_mhash_count(fs_lockable_t *hf)
{
    fs_mhash *mh = (fs_mhash *)hf;
    return mh->mh_count;
}

/* vi:set expandtab sts=4 sw=4: */
