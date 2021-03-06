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
#define _XOPEN_SOURCE 600
#endif
#include <stdlib.h>
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>
#include <glib.h>
#include <string.h>
#include <zlib.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/file.h>

#include "backend.h"
#include "rhash.h"
#include "list.h"
#include "prefix-trie.h"
#include "common/params.h"
#include "common/hash.h"
#include "common/error.h"

#define FS_RHASH_DEFAULT_LENGTH        65536
#define FS_RHASH_DEFAULT_SEARCH_DIST      32
#define FS_RHASH_DEFAULT_BUCKET_SIZE      16
#define FS_MAX_PREFIXES                  256

#define FS_RHASH_ID 0x4a585230

/* maximum distance that we wil allow the resource to be from its hash value */

#define FS_PACKED __attribute__((__packed__))

#define FS_RHASH_ENTRY(rh, rid) (((uint64_t)(rid >> 10) & ((uint64_t)(rh->size - 1)))*rh->header->bucket_size)

#define DISP_I_UTF8         'i'
#define DISP_I_NUMBER       'N'
#define DISP_I_DATE         'D'
#define DISP_I_PREFIX       'p'
#define DISP_F_UTF8         'f'
#define DISP_F_PREFIX       'P'
#define DISP_F_ZCOMP        'Z'

struct rhash_header {
    int32_t id;             // "JXR0"
    uint32_t size;          // size of hashtable in buckets,
                            //    must be power of two
    uint32_t count;         // number of resources in hashtable
    uint32_t search_dist;   // offset to scan to in table for match
    uint32_t bucket_size;   // number of entries per bucket
    uint32_t revision;      // revision of the strucure
                            // rev=1: 32 byte, packed entries
    char padding[488];      // allign to a block
} FS_PACKED;
 
#define INLINE_STR_LEN 15

typedef struct _fs_rhash_entry {
    fs_rid rid;
    union {
        fs_rid attr;    // attribute value, lang tag or datatype
        unsigned char pstr[8]; // prefix code + first 7 chars
    } FS_PACKED aval;
    union {
        int64_t offset; // offset in lex file
        char str[INLINE_STR_LEN];   // inline string data
    } FS_PACKED val;
    char disp;          // disposition of data - lex file or inline
} FS_PACKED fs_rhash_entry;

typedef struct _fs_rhash {
    fs_lockable_t hf;
    uint32_t size;
    struct rhash_header *header;
    fs_rhash_entry *entries;
    FILE *lex_f;
    char *lex_filename;
    fs_prefix_trie *ptrie;
    fs_prefix_trie *prefixes;
    int prefix_count;
    char *prefix_strings[FS_MAX_PREFIXES];
    fs_lockable_t *prefix_file; /* list */
    char *z_buffer;
    int z_buffer_size;

    /* hooks for locking management */
    int (*prefix_read_metadata)(fs_lockable_t *);
    int (*do_lock)(fs_lockable_t *, int);
} fs_rhash;
#define rh_fd hf.fd
#define rh_flags hf.flags
#define rh_mmap_addr hf.mmap_addr
#define rh_mmap_size hf.mmap_size
#define rh_filename hf.filename
#define rh_read_metadata hf.read_metadata
#define rh_write_metadata hf.write_metadata

/* this is much wider than it needs to be to match fs_list requirements */
struct prefix_file_line {
    uint32_t code;
    char     prefix[512-4];
};

static fs_rhash *global_sort_rh = NULL;

static int double_size(fs_rhash *rh);

static int fs_rhash_remap(fs_lockable_t *);
static int fs_rhash_write_header(fs_lockable_t *);
static int fs_rhash_load_prefixes(fs_rhash *);

static int fs_rhash_prefix_read_metadata(fs_lockable_t *);
static int fs_rhash_do_lock(fs_lockable_t *, int);

static int compress_bcd(const char *in, char *out);
static char *uncompress_bcd(unsigned char *bcd);

static int compress_bcdate(const char *in, char *out);
static char *uncompress_bcdate(unsigned char *bcd);

fs_lockable_t *fs_rhash_open(fs_backend *be, const char *label, int flags)
{
    char *filename = g_strdup_printf(FS_RHASH, fs_backend_get_kb(be),
                                     fs_backend_get_segment(be), label);
    fs_lockable_t *rh = fs_rhash_open_filename(filename, flags);
    g_free(filename);

    return rh;
}

fs_lockable_t *fs_rhash_open_filename(const char *filename, int flags)
{
    fs_rhash *rh;
    fs_lockable_t *hf;
    struct rhash_header header;

    if (sizeof(struct rhash_header) != 512) {
        fs_error(LOG_CRIT, "incorrect rhash header size %zd, should be 512",
                 sizeof(header));

        return NULL;
    }
    if (sizeof(fs_rhash_entry) != 32) {
        fs_error(LOG_CRIT, "incorrect entry size %zd, should be 32",
                 sizeof(fs_rhash_entry));

        return NULL;
    }

    rh = calloc(1, sizeof(fs_rhash));
    hf = (fs_lockable_t *)rh;
    rh->rh_flags = flags;
    rh->z_buffer_size = 1024;
    rh->z_buffer = malloc(rh->z_buffer_size);
    rh->rh_filename = g_strdup(filename);
    rh->lex_filename = g_strdup_printf("%s.lex", filename);

    rh->rh_fd = open(filename, FS_O_NOATIME | flags, FS_FILE_MODE);
    if (rh->rh_fd == -1) {
        fs_error(LOG_ERR, "cannot open rhash file '%s': %s", filename, strerror(errno));
        g_free(rh->rh_filename);
        g_free(rh->lex_filename);
        free(rh->z_buffer);
        free(rh);
        return NULL;
    }

    rh->rh_read_metadata = fs_rhash_remap;
    rh->rh_write_metadata = fs_rhash_write_header;

    if (fs_lockable_init(hf)) {
        g_free(rh->rh_filename);
        g_free(rh->lex_filename);
        free(rh->z_buffer);
        free(rh);
        return NULL;
    }

    char *prefix_filename = g_strdup_printf("%s.prefixes", rh->rh_filename);
    rh->prefix_file = fs_list_open_filename(prefix_filename,
                                            sizeof(struct prefix_file_line), flags);
    g_free(prefix_filename);
    if (!rh->prefix_file) {
        g_free(rh->rh_filename);
        g_free(rh->lex_filename);
        free(rh->z_buffer);
        free(rh);
        return NULL;
    }

    /* have to hook the read metadata method on the prefix list
     * file so that if it has changed, we re-read our own prefix list
     * it would probably be more efficient to somehow put the
     * patricia trie directly on disc instead of re-constructing it */
    rh->prefix_read_metadata = rh->prefix_file->read_metadata;
    rh->prefix_file->read_metadata = fs_rhash_prefix_read_metadata;

    /* prefixes won't have been loaded yet, so load them */
    fs_lockable_lock(rh->prefix_file, LOCK_SH);
    fs_rhash_load_prefixes(rh);
    fs_lockable_lock(rh->prefix_file, LOCK_UN);

    /* have to hook our lock routine as well to also lock the prefixes
     * otherwise we risk spending a lot of time locking and unlocking
     * them during data imports */
    rh->do_lock = rh->hf.lock;
    rh->hf.lock = fs_rhash_do_lock;

    rh->ptrie = fs_prefix_trie_new();

    rh->lex_f = fopen(rh->lex_filename, ((flags & (O_WRONLY|O_RDWR)) ? "a+" : "r"));
    if (!rh->lex_f) {
        fs_error(LOG_ERR, "failed to open rhash lex file “%s”: %s",
                 rh->lex_filename, strerror(errno));

        return NULL;
    }

    return hf;
}

static int fs_rhash_do_lock(fs_lockable_t *hf, int operation)
{
    fs_rhash *rh = (fs_rhash *)hf;
    /* first call our original locking routine */
    if ((rh->do_lock)(hf, operation))
        return -1;
    /* great, that worked. now lock our prefix file in the same way */
    if (fs_lockable_lock(rh->prefix_file, operation)) {
        if (operation & (LOCK_SH|LOCK_EX)) /* release any lock we have acquired */
            (rh->do_lock)(hf, LOCK_UN);
        return -1;
    }
    return 0;
}

/* hook function for the prefix list's read_metadata */
static int fs_rhash_prefix_read_metadata(fs_lockable_t *hf)
{
    fs_rhash *rh = (fs_rhash *)hf;

    if ((rh->prefix_read_metadata)(hf))
        return -1;

    return fs_rhash_load_prefixes(rh);
}

/* this is always called with at least a read lock on the
 * prefix_file */
static int fs_rhash_load_prefixes(fs_rhash *rh)
{
    struct prefix_file_line pre;

    if (rh->prefixes)
        fs_prefix_trie_free(rh->prefixes);

    rh->prefixes = fs_prefix_trie_new();
    rh->prefix_count = 0;

    fs_list_rewind_r(rh->prefix_file);
    while (fs_list_next_value_r(rh->prefix_file, &pre)) {
        fs_prefix_trie_add_code(rh->prefixes, pre.prefix, pre.code);
        rh->prefix_strings[pre.code] = g_strdup(pre.prefix);
        (rh->prefix_count)++;
    }

    return 0;
}

static int fs_rhash_remap(fs_lockable_t *hf) {
    fs_rhash *rh = (fs_rhash *)hf;
    size_t size, bucket_size;

    if (rh->header == NULL) { /* first time */
        struct rhash_header header;
        if (pread(rh->rh_fd, &header, sizeof(header), 0) != sizeof(header)) {
            fs_error(LOG_ERR, "pread failed: %s", strerror(errno));
            return -1;
        }
        if (header.id != FS_RHASH_ID) {
            fs_error(LOG_ERR, "%s does not appear to be a rhash file", rh->rh_filename);
            return -1;
        }
        size = header.size;
        bucket_size = header.bucket_size;
    } else {
        size = rh->header->size;
        bucket_size = rh->header->bucket_size;
    }

    if ( (rh->header == NULL) || (rh->size != rh->header->size) ) {
        if (rh->rh_mmap_addr > 0) {
            if (munmap(rh->rh_mmap_addr, rh->rh_mmap_size)) {
                fs_error(LOG_ERR, "munmap(%s): %s", rh->rh_filename, strerror(errno));
                return -1;
            }
        }
        rh->rh_mmap_size = size * bucket_size * sizeof(fs_rhash_entry)
                         + sizeof(struct rhash_header); // otherwise not page aligned
	rh->rh_mmap_addr = rh->header = mmap(NULL, rh->rh_mmap_size, PROT_READ|PROT_WRITE, MAP_SHARED, rh->rh_fd, 0);
        if (rh->rh_mmap_addr == MAP_FAILED) {
            fs_error(LOG_ERR, "mmap(%s): %s", rh->rh_filename, strerror(errno));
            rh->rh_mmap_size = 0;
            return -1;
        }
	rh->entries = (void *)rh->header + sizeof(struct rhash_header);
	rh->size = rh->header->size;
        if (rh->header->bucket_size == 0)
	    rh->header->bucket_size = 1;
    }

    return 0;
}

static void fs_rhash_ensure_size(fs_rhash *rh)
{
    off_t size, bucket_size;
    if (rh->header == NULL) {  /* only the case if initialising a file */
        size = FS_RHASH_DEFAULT_LENGTH;
        bucket_size = FS_RHASH_DEFAULT_BUCKET_SIZE;
    } else {
        size = rh->header->size;
        bucket_size = rh->header->bucket_size;
    }
    /* skip if we're read-only */
    if (!(rh->rh_flags & (O_WRONLY | O_RDWR))) return;

    const off_t len = sizeof(struct rhash_header) + size * bucket_size * sizeof(fs_rhash_entry);

    /* FIXME should use fallocate where it has decent performance,
       in order to avoid fragmentation */

    unsigned char byte = 0;
    /* write one past the end to avoid possibility of overwriting the last RID */
    if (pwrite(rh->rh_fd, &byte, sizeof(byte), len) == -1) {
        fs_error(LOG_ERR, "couldn't pre-allocate for '%s': %s", rh->rh_filename, strerror(errno));
    }
}

static int fs_rhash_write_header(fs_lockable_t *hf)
{
    fs_rhash *rh = (fs_rhash *)hf;
    struct rhash_header header;

    /* only necessary if we are initialising the file */
    if (rh->rh_mmap_addr == NULL) {
        header.id = FS_RHASH_ID;
        header.count = 0;
        header.size = rh->size = FS_RHASH_DEFAULT_LENGTH;
        header.search_dist = FS_RHASH_DEFAULT_SEARCH_DIST;
        header.bucket_size = FS_RHASH_DEFAULT_BUCKET_SIZE;
        header.revision = 1;
        memset(&header.padding, 0, sizeof(header.padding));
        if (pwrite(rh->rh_fd, &header, sizeof(header), 0) == -1) {
            fs_error(LOG_CRIT, "failed to write header on %s: %s",
                     rh->rh_filename, strerror(errno));
            return 1;
        }
        fs_rhash_ensure_size(rh);
    }

    if (rh->lex_f) {
        fflush(rh->lex_f);
        fs_fsync(fileno(rh->lex_f));
    }

    return 0;
}

int fs_rhash_close(fs_lockable_t *hf)
{
    fs_rhash *rh = (fs_rhash *)hf;
    int i;

    fs_list_close(rh->prefix_file);
    fclose(rh->lex_f);

    fs_prefix_trie_free(rh->prefixes);
    fs_prefix_trie_free(rh->ptrie);
    free(rh->z_buffer);
    g_free(rh->rh_filename);

    for (i=0; i<FS_MAX_PREFIXES; i++) {
        if (!rh->prefix_strings[i])
            break;
        g_free(rh->prefix_strings[i]);
    }

    munmap((void *)(rh->entries) - sizeof(struct rhash_header), rh->rh_mmap_size);
    close(rh->rh_fd);
    free(rh);

    return 0;
}

int fs_rhash_put(fs_lockable_t *hf, fs_resource *res)
{
    int ret;
    if (fs_lockable_lock(hf, LOCK_EX))
        return -1;
    ret = fs_rhash_put_r(hf, res);
    if (fs_lockable_lock(hf, LOCK_UN))
        return -1;
    return ret;
}

int fs_rhash_put_r(fs_lockable_t *hf, fs_resource *res)
{
    fs_rhash *rh = (fs_rhash *)hf;
    int entry = FS_RHASH_ENTRY(rh, res->rid);

    fs_assert(fs_lockable_test(hf, LOCK_EX));

    if (entry >= rh->size * rh->header->bucket_size) {
        fs_error(LOG_CRIT, "tried to write into rhash '%s' with bad entry number %d",
                 rh->rh_filename, entry);
        return 1;
    }

    fs_rhash_entry *buffer = rh->entries + entry;

    int new = -1;
    for (int i= 0; i < rh->header->search_dist && entry + i < rh->size * rh->header->bucket_size; i++) {
        if (buffer[i].rid == res->rid) {
            /* resource is already there, we're done */
            // TODO could check for collision
            return 0;
        } else if (buffer[i].rid == 0 && new == -1) {
            new = entry + i;
        }
    }

    if (new == -1) {
        /* hash overfull, grow */
        if (double_size(rh)) {
            fs_error(LOG_CRIT, "failed to correctly double size of rhash");
            return 1;
        }
        return fs_rhash_put_r(hf, res);
    }

    if (new >= rh->size * rh->header->bucket_size) {
        fs_error(LOG_CRIT, "writing RID %016llx past end of rhash '%s'", res->rid, rh->rh_filename);
    }

    fs_rhash_entry e;
    e.rid = res->rid;
    e.aval.attr = res->attr;
    memset(&e.val.str, 0, INLINE_STR_LEN);
    if (strlen(res->lex) <= INLINE_STR_LEN) {
        strncpy(e.val.str, res->lex, INLINE_STR_LEN);
        e.disp = DISP_I_UTF8;
    } else if (compress_bcd(res->lex, NULL) == 0) {
        if (compress_bcd(res->lex, e.val.str)) {
            fs_error(LOG_ERR, "failed to compress '%s' as BCD", res->lex);
        }
        e.disp = DISP_I_NUMBER;
    } else if (compress_bcdate(res->lex, NULL) == 0) {
        if (compress_bcdate(res->lex, e.val.str)) {
            fs_error(LOG_ERR, "failed to compress '%s' as BCDate", res->lex);
        }
        e.disp = DISP_I_DATE;
    } else if (FS_IS_URI(res->rid) &&
               fs_prefix_trie_get_code(rh->prefixes, res->lex, NULL)) {
        int length = 0;
        int code = fs_prefix_trie_get_code(rh->prefixes, res->lex, &length);
        char *suffix = (res->lex)+length;
        const int32_t suffix_len = strlen(suffix);
        e.aval.pstr[0] = (char)code;
        if (suffix_len > 22) {
            /* even with prefix, won't fit inline */
            if (fseek(rh->lex_f, 0, SEEK_END) == -1) {
                fs_error(LOG_CRIT, "failed to fseek to end of '%s': %s",
                    rh->lex_filename, strerror(errno));
                return 1;
            }
            long pos = ftell(rh->lex_f);
            if (fwrite(&suffix_len, sizeof(suffix_len), 1, rh->lex_f) != 1) {
                fs_error(LOG_CRIT, "failed writing to lexical file “%s”",
                         rh->lex_filename);

                return 1;
            }
            if (fputs(suffix, rh->lex_f) == EOF || fputc('\0', rh->lex_f) == EOF) {
                fs_error(LOG_CRIT, "failed writing to lexical file “%s”",
                         rh->lex_filename);
            }
            e.val.offset = pos;
            e.disp = DISP_F_PREFIX;
        } else {
            strncpy((char *)(e.aval.pstr)+1, suffix, 7);
            if (suffix_len > 7) {
                strncpy((char *)e.val.str, suffix+7, INLINE_STR_LEN);
            }
            e.disp = DISP_I_PREFIX;
        }
    } else {
        /* needs to go into external file */
        if (rh->ptrie && FS_IS_URI(res->rid)) {
            if (fs_prefix_trie_add_string(rh->ptrie, res->lex)) {
                /* add_string failed, prefix trie is probably full */
                fs_prefix *pre = fs_prefix_trie_get_prefixes(rh->ptrie, 32);
                int num = 0;
                struct prefix_file_line pfl;
                memset(&pfl, 0, sizeof(struct prefix_file_line));
                for (int i=0; i<32; i++) {
                    if (pre[i].score == 0 || rh->prefix_count == FS_MAX_PREFIXES) {
                        break;
                    }
                    num++;
                    rh->prefix_strings[rh->prefix_count] = strdup(pre[i].prefix);
                    fs_prefix_trie_add_code(rh->prefixes, pre[i].prefix,
                                            rh->prefix_count);
                    fs_error(LOG_INFO, "adding prefix %d <%s>", rh->prefix_count, pre[i].prefix);
                    pfl.code = rh->prefix_count;
                    strcpy(pfl.prefix, pre[i].prefix);
                    fs_list_add_r(rh->prefix_file, &pfl);
                    (rh->prefix_count)++;
                }
                free(pre);
                fs_prefix_trie_free(rh->ptrie);
                rh->ptrie = fs_prefix_trie_new();
            }
        }

        /* check to see if there's any milage in compressing */
        int32_t lex_len = strlen(res->lex);
        /* grow z buffer if neccesary */
        if (rh->z_buffer_size < lex_len * 1.01 + 12) {
            while (rh->z_buffer_size < (lex_len * 1.01 + 12)) {
                rh->z_buffer_size *= 2;
            }
            free(rh->z_buffer);
            rh->z_buffer = malloc(rh->z_buffer_size);
            if (!rh->z_buffer) {
                fs_error(LOG_CRIT, "failed to allocate z buffer (%d bytes)", rh->z_buffer_size);
            }
        }
        unsigned long compsize = rh->z_buffer_size;
        char *data = res->lex;
        int32_t data_len = lex_len;
        char disp = DISP_F_UTF8;
        /* if the lex string is more than 100 chars long, try compressing it */
        if (lex_len > 100) {
            int ret = compress((Bytef *)rh->z_buffer, &compsize, (Bytef *)res->lex, (unsigned long)lex_len);
            if (ret == Z_OK) {
                if (compsize && compsize < lex_len - 4) {
                    data = rh->z_buffer;
                    data_len = compsize;
                    disp = DISP_F_ZCOMP;
                }
            } else {
                if (ret == Z_MEM_ERROR) {
                    fs_error(LOG_ERR, "zlib error: out of memory");
                } else if (ret == Z_BUF_ERROR) {
                    fs_error(LOG_ERR, "zlib error: buffer error");
                } else {
                    fs_error(LOG_ERR, "zlib error %d", ret);
                }
            }
        }
        if (fseek(rh->lex_f, 0, SEEK_END) == -1) {
            fs_error(LOG_CRIT, "failed to fseek to end of '%s': %s",
                rh->lex_filename, strerror(errno));
                return 1;
        }
        long pos = ftell(rh->lex_f);
        e.disp = disp;
        if (fwrite(&data_len, sizeof(data_len), 1, rh->lex_f) == 0) {
            fs_error(LOG_CRIT, "failed writing to lexical file “%s”",
                     rh->lex_filename);

            return 1;
        }
        if (disp == DISP_F_ZCOMP) {
            /* write the length of the uncompressed string too */
            if (fwrite(&lex_len, sizeof(lex_len), 1, rh->lex_f) == 0) {
                fs_error(LOG_CRIT, "failed writing to lexical file “%s”",
                         rh->lex_filename);

                return 1;
            }
        }
        if (fwrite(data, data_len, 1, rh->lex_f) == EOF || fputc('\0', rh->lex_f) == EOF) {
            fs_error(LOG_CRIT, "failed writing to lexical file “%s”",
                     rh->lex_filename);
        }
        e.val.offset = pos;
    }
    rh->entries[new] = e;
    rh->header->count++;

    return 0;
}

static int sort_by_hash(const void *va, const void *vb)
{
    const fs_resource *a = va;
    const fs_resource *b = vb;
    int ea = FS_RHASH_ENTRY(global_sort_rh, a->rid);
    int eb = FS_RHASH_ENTRY(global_sort_rh, b->rid);

    if (ea != eb) return ea - eb;
    if (a->rid < b->rid) return -1;
    if (a->rid > b->rid) return 1;

    return 0;
}

static int double_size(fs_rhash *rh)
{
    long int oldsize = rh->size;
    long int errs = 0;

    fs_error(LOG_INFO, "doubling rhash (%s)", rh->rh_filename);

    /* update the size in the header */
    rh->header->size *= 2;
    fs_rhash_ensure_size(rh);
    
    if (fs_rhash_remap((fs_lockable_t *)rh))
        return -1;

    fs_rhash_entry blank;
    memset(&blank, 0, sizeof(blank));
    fs_rhash_entry buffer_hi[rh->header->bucket_size];

    for (long int i=0; i<oldsize * rh->header->bucket_size; i += rh->header->bucket_size) {
        memset(buffer_hi, 0, sizeof(buffer_hi));
        fs_rhash_entry * const from = rh->entries + i;
        for (int j=0; j < rh->header->bucket_size; j++) {
            if (from[j].rid == 0) continue;

            long int entry = FS_RHASH_ENTRY(rh, from[j].rid);
            if (entry >= oldsize * rh->header->bucket_size) {
                buffer_hi[j] = from[j];
                from[j] = blank;
            }
        }
        memcpy(from + (oldsize * rh->header->bucket_size), buffer_hi, sizeof(buffer_hi));
    }

    return errs;
}

int fs_rhash_put_multi(fs_lockable_t *hf, fs_resource *res, int count)
{
    int ret;
    if (fs_lockable_lock(hf, LOCK_EX))
        return -1;
    ret = fs_rhash_put_multi_r(hf, res, count);
    if (fs_lockable_lock(hf, LOCK_UN))
        return -1;
    return ret;
}

int fs_rhash_put_multi_r(fs_lockable_t *hf, fs_resource *res, int count)
{
    global_sort_rh = (fs_rhash *)hf;
    qsort(res, count, sizeof(fs_resource), sort_by_hash);
    fs_rid last = FS_RID_NULL;

    int ret = 0;
    for (int i=0; i<count; i++) {
        if (res[i].rid == FS_RID_NULL) continue;
        if (res[i].rid == last) continue;
        ret += fs_rhash_put_r(hf, res+i);
        last = res[i].rid;
    }

    return ret;
}

static inline int get_entry(fs_rhash *rh, fs_rhash_entry *e, fs_resource *res)
{
    if (e->disp == DISP_I_UTF8) {
        res->lex = malloc(INLINE_STR_LEN+1);
        res->lex[INLINE_STR_LEN] = '\0';
        res->lex = memcpy(res->lex, e->val.str, INLINE_STR_LEN);
    } else if (e->disp == DISP_I_NUMBER) {
        res->lex = uncompress_bcd((unsigned char *)e->val.str);
    } else if (e->disp == DISP_I_DATE) {
        res->lex = uncompress_bcdate((unsigned char *)e->val.str);
    } else if (e->disp == DISP_I_PREFIX) {
        if (e->aval.pstr[0] >= rh->prefix_count) {
            res->lex = malloc(128);
            sprintf(res->lex, "¡bad prefix %d (max %d)!", e->aval.pstr[0], rh->prefix_count - 1);
            fs_error(LOG_ERR, "prefix %d out of range, count=%d", e->aval.pstr[0], rh->prefix_count);
        } else {
            const int pnum = e->aval.pstr[0];
            int plen = strlen(rh->prefix_strings[pnum]);
            res->lex = calloc(23 + plen, sizeof(char));
            strcpy(res->lex, rh->prefix_strings[pnum]);
            strncpy((res->lex) + plen, (char *)(e->aval.pstr)+1, 7);
            strncat((res->lex) + plen, e->val.str, 15);
            res->attr = 0;
        }
    } else if (e->disp == DISP_F_UTF8) {
        int32_t lex_len;
        if (fseek(rh->lex_f, e->val.offset, SEEK_SET) == -1) {
            fs_error(LOG_ERR, "seek error reading lexical store '%s': %s", rh->lex_filename, strerror(errno));

            return 1;
        }
        if (fread(&lex_len, sizeof(lex_len), 1, rh->lex_f) == 0) {
            fs_error(LOG_ERR, "read error from lexical store '%s', offset %lld: %s", rh->lex_filename, (long long)e->val.offset, strerror(errno));

            return 1;
        }

        res->lex = malloc(lex_len + 1);

        if (fread(res->lex, sizeof(char), lex_len, rh->lex_f) < lex_len) {
            fs_error(LOG_ERR, "partial read %s from lexical store '%s'", ferror(rh->lex_f) ? "error" : "EOF", rh->lex_filename);
            clearerr(rh->lex_f);
            res->lex[0] = '\0';

            return 1;
        }
        res->lex[lex_len] = '\0';
    } else if (e->disp == DISP_F_PREFIX) {
        if (e->aval.pstr[0] >= rh->prefix_count) {
            fs_error(LOG_ERR, "prefix %d out of range, count=%d", e->aval.pstr[0], rh->prefix_count);

            return 1;
        }
        char *prefix = rh->prefix_strings[e->aval.pstr[0]];
        int prefix_len = strlen(prefix);
        int32_t lex_len = 0;
        int32_t suffix_len = 0;
        if (fseek(rh->lex_f, e->val.offset, SEEK_SET) == -1) {
            fs_error(LOG_ERR, "seek error reading lexical store '%s': %s", rh->lex_filename, strerror(errno));

            return 1;
        }
        if (fread(&suffix_len, sizeof(suffix_len), 1, rh->lex_f) == 0) {
            fs_error(LOG_ERR, "read error from lexical store '%s', offset %lld: %s", rh->lex_filename, (long long)e->val.offset, strerror(errno));

            return 1;
        }

        lex_len = suffix_len + prefix_len;
        res->lex = malloc(lex_len + 1);
        strcpy(res->lex, prefix);

        if (fread((res->lex) + prefix_len, sizeof(char), suffix_len, rh->lex_f) < suffix_len) {
            fs_error(LOG_ERR, "partial read %s, of %d bytes (%d+%d) for RID %016llx from lexical store '%s'", ferror(rh->lex_f) ? "error" : "EOF", suffix_len, prefix_len, suffix_len, (long long)e->rid, rh->lex_filename);
            clearerr(rh->lex_f);
            res->lex[0] = '\0';

            return 1;
        }
        res->lex[lex_len] = '\0';
    } else if (e->disp == DISP_F_ZCOMP) {
        int32_t data_len;
        int32_t lex_len;
        if (fseek(rh->lex_f, e->val.offset, SEEK_SET) == -1) {
            fs_error(LOG_ERR, "seek error reading lexical store '%s': %s", rh->lex_filename, strerror(errno));

            return 1;
        }
        if (fread(&data_len, sizeof(data_len), 1, rh->lex_f) == 0) {
            fs_error(LOG_ERR, "read error from lexical store '%s', offset %lld: %s", rh->lex_filename, (long long)e->val.offset, strerror(errno));

            return 1;
        }
        if (fread(&lex_len, sizeof(lex_len), 1, rh->lex_f) == 0) {
            fs_error(LOG_ERR, "read error from lexical store '%s', offset %lld: %s", rh->lex_filename, (long long)e->val.offset, strerror(errno));

            return 1;
        }

        if (rh->z_buffer_size < data_len) {
            while (rh->z_buffer_size < data_len) {
                rh->z_buffer_size *= 2;
            }
            free(rh->z_buffer);
            rh->z_buffer = malloc(rh->z_buffer_size);
        }
        res->lex = malloc(lex_len + 1);
        if (fread(rh->z_buffer, sizeof(char), data_len, rh->lex_f) < data_len) {
            fs_error(LOG_ERR, "partial read %s from lexical store '%s'", ferror(rh->lex_f) ? "error" : "EOF", rh->lex_filename);
            clearerr(rh->lex_f);
            res->lex = strdup("¡read error!");

            return 1;
        }
        unsigned long uncomp_len = lex_len;
        unsigned long dlen = data_len;
        int ret;
        ret = uncompress((Bytef *)res->lex, &uncomp_len, (Bytef *)rh->z_buffer, dlen);
        if (ret == Z_OK) {
            if (uncomp_len != lex_len) {
                fs_error(LOG_ERR, "something went wrong in decompression");
            }
            res->lex[lex_len] = '\0';
        } else {
            if (ret == Z_MEM_ERROR) {
                fs_error(LOG_ERR, "zlib error: out of memory");
            } else if (ret == Z_BUF_ERROR) {
                fs_error(LOG_ERR, "zlib error: buffer error");
            } else {
                fs_error(LOG_ERR, "zlib error %d, uncomp_len = %d (%d), comp_len = %d", ret, (int)uncomp_len, (int)lex_len, (int)dlen);
            }
            res->lex[0] = '\0';

            return 1;
        }
    } else {
        res->lex = g_strdup_printf("error: unknown disposition: %c", e->disp);

        return 1;
    }
    res->attr = e->aval.attr;

    return 0;
}

int fs_rhash_get(fs_lockable_t *hf, fs_resource *res)
{
    int ret;
    if (fs_lockable_lock(hf, LOCK_SH))
        return -1;
    ret = fs_rhash_get_r(hf, res);
    if (fs_lockable_lock(hf, LOCK_UN))
        return -1;

    return ret;
}


int fs_rhash_get_r(fs_lockable_t *hf, fs_resource *res)
{
    fs_rhash *rh = (fs_rhash *)hf;
    const int entry = FS_RHASH_ENTRY(rh, res->rid);
    fs_rhash_entry *buffer = rh->entries + entry;

    fs_assert(fs_lockable_test(hf, (LOCK_SH|LOCK_EX)));

    for (int k = 0; k < rh->header->search_dist; ++k) {
        if (buffer[k].rid == res->rid) {
            return get_entry(rh, &buffer[k], res);
        }
    }

    fs_error(LOG_WARNING, "resource %016llx not found in § 0x%x-0x%x of %s",
             res->rid, entry, entry + rh->header->search_dist - 1, rh->rh_filename);
    res->lex = g_strdup_printf("¡resource %llx not found!", res->rid);
    res->attr = 0;

    return 1;
}

int fs_rhash_get_multi(fs_lockable_t *hf, fs_resource *res, int count)
{
    int ret;
    if (fs_lockable_lock(hf, LOCK_SH))
        return -1;
    ret = fs_rhash_get_multi_r(hf, res, count);
    if (fs_lockable_lock(hf, LOCK_UN))
        return -1;
    return ret;
}

int fs_rhash_get_multi_r(fs_lockable_t *hf, fs_resource *res, int count)
{
    global_sort_rh = (fs_rhash *)hf;
    qsort(res, count, sizeof(fs_resource), sort_by_hash);

    int ret = 0;
    for (int i=0; i<count; i++) {
        res[i].attr = FS_RID_NULL;
        res[i].lex = NULL;
        if (FS_IS_BNODE(res[i].rid)) {
            res[i].lex = g_strdup_printf("_:b%llx", res[i].rid);
            continue;
        }
        ret += fs_rhash_get_r(hf, res+i);
    }

    return ret;
}

void fs_rhash_print(fs_lockable_t *hf, FILE *out, int verbosity)
{
    if (fs_lockable_lock(hf, LOCK_SH))
        return;
    fs_rhash_print_r(hf, out, verbosity);
    fs_lockable_lock(hf, LOCK_UN);
}

void fs_rhash_print_r(fs_lockable_t *hf, FILE *out, int verbosity)
{
    fs_rhash *rh = (fs_rhash *)hf;
    fs_assert(rh);
    fs_assert(fs_lockable_test(hf, (LOCK_SH|LOCK_EX)));

    int disp_freq[128];
    memset(disp_freq, 0, 128);

    fprintf(out, "%s\n", rh->rh_filename);
    fprintf(out, "size:     %d (buckets)\n", rh->size);
    fprintf(out, "bucket:   %d\n", rh->header->bucket_size);
    fprintf(out, "entries:  %d\n", rh->header->count);
    fprintf(out, "prefixes:  %d\n", rh->prefix_count);
    fprintf(out, "revision: %d\n", rh->header->revision);
    fprintf(out, "fill:     %.1f%%\n", 100.0 * (double)rh->header->count / (double)(rh->size * rh->header->bucket_size));

    if (verbosity < 1) {
        return;
    }

    for (int p=0; p<rh->prefix_count; p++) {
        fprintf(out, "prefix %d: %s\n", p, rh->prefix_strings[p]);
    }

    if (verbosity < 2) {
        return;
    }

    fs_rhash_entry e;
    int entry = 0, entries = 0, show_next = 0;

    lseek(rh->rh_fd, sizeof(struct rhash_header), SEEK_SET);
    while (read(rh->rh_fd, &e, sizeof(e)) == sizeof(e)) {
        if (e.rid) {
            char *ent_str = g_strdup_printf("%08d.%02d", entry / rh->header->bucket_size, entry % rh->header->bucket_size);
            fs_resource res = { .lex = NULL };
            int ret = get_entry(rh, &e, &res);
            if (ret) {
                fprintf(out, "ERROR: failed to get entry for %016llx\n", e.rid);
                continue;
            }
            if (e.disp == DISP_F_UTF8 || e.disp == DISP_F_ZCOMP) {
                if (verbosity > 1 || show_next) fprintf(out, "%s %016llx %016llx %c %10lld %s\n", ent_str, e.rid, e.aval.attr, e.disp, (long long)e.val.offset, res.lex);
            } else if (e.disp == DISP_F_PREFIX) {
                if (verbosity > 1 || show_next) fprintf(out, "%s %016llx %16d %c %10lld %s\n", ent_str, e.rid, e.aval.pstr[0], e.disp, (long long)e.val.offset, res.lex);
            } else {
                if (verbosity > 1 || show_next) fprintf(out, "%s %016llx %016llx %c %s\n", ent_str, e.rid, e.aval.attr, e.disp, res.lex);
            }
            disp_freq[(int)e.disp]++;
            entries++;
            show_next = 0;
            free(res.lex);
            g_free(ent_str);
        }
        entry++;
    }
    fprintf(out, "STATS: length: %d, bsize: %d, entries: %d (%+d), %.1f%% full\n", rh->size, rh->header->bucket_size, entries, rh->header->count - entries, 100.0 * (double)entries / (double)(rh->size * rh->header->bucket_size));
    if (rh->header->count != entries) {
        fprintf(out, "ERROR: entry count in header %d != count from scan %d\n",
                rh->header->count, entries);
    }
    fprintf(out, "Disposition frequencies:\n");
    for (int d=0; d<128; d++) {
        if (disp_freq[d] > 0) {
            fprintf(out, "%c: %8d\n", d, disp_freq[d]);
        }
    }
}

int fs_rhash_count(fs_lockable_t *hf)
{
    fs_rhash *rh = (fs_rhash *)hf;
    return rh->header->count;
}

/* literal storage compression functions */

enum bcd {
    bcd_nul = 0,
    bcd_1,
    bcd_2,
    bcd_3,
    bcd_4,
    bcd_5,
    bcd_6,
    bcd_7,
    bcd_8,
    bcd_9,
    bcd_0,
    bcd_dot,
    bcd_plus,
    bcd_minus,
    bcd_e
};

static const char bcd_map[16] = {
    '\0', '1', '2', '3', '4', '5', '6', '7',
    '8', '9', '0', '.', '+', '-', 'e', '?'
};

enum bcdate {
    bcdate_nul = 0,
    bcdate_1,
    bcdate_2,
    bcdate_3,
    bcdate_4,
    bcdate_5,
    bcdate_6,
    bcdate_7,
    bcdate_8,
    bcdate_9,
    bcdate_0,
    bcdate_colon,
    bcdate_plus,
    bcdate_minus,
    bcdate_T,
    bcdate_Z
};

static const char bcdate_map[16] = {
    '\0', '1', '2', '3', '4', '5', '6', '7',
    '8', '9', '0', ':', '+', '-', 'T', 'Z'
};

static inline void write_bcd(char *out, int pos, int val)
{
    out += pos / 2;
    const int offset = (pos % 2) * 4;
    *out |= (val << offset);
}

static int compress_bcd(const char *in, char *out)
{
    if (strlen(in) > INLINE_STR_LEN * 2) {
        /* too long */
        return 1;
    }

    /* zero output buffer */
    if (out) {
        memset(out, 0, INLINE_STR_LEN);
    }
    int outpos = 0;
    for (const char *inp = in; *inp; inp++) {
        switch (*inp) {
        case '1':
            if (out) {
                write_bcd(out, outpos, bcd_1);
                outpos++;
            }
            break;
        case '2':
            if (out) {
                write_bcd(out, outpos, bcd_2);
                outpos++;
            }
            break;
        case '3':
            if (out) {
                write_bcd(out, outpos, bcd_3);
                outpos++;
            }
            break;
        case '4':
            if (out) {
                write_bcd(out, outpos, bcd_4);
                outpos++;
            }
            break;
        case '5':
            if (out) {
                write_bcd(out, outpos, bcd_5);
                outpos++;
            }
            break;
        case '6':
            if (out) {
                write_bcd(out, outpos, bcd_6);
                outpos++;
            }
            break;
        case '7':
            if (out) {
                write_bcd(out, outpos, bcd_7);
                outpos++;
            }
            break;
        case '8':
            if (out) {
                write_bcd(out, outpos, bcd_8);
                outpos++;
            }
            break;
        case '9':
            if (out) {
                write_bcd(out, outpos, bcd_9);
                outpos++;
            }
            break;
        case '0':
            if (out) {
                write_bcd(out, outpos, bcd_0);
                outpos++;
            }
            break;
        case '.':
            if (out) {
                write_bcd(out, outpos, bcd_dot);
                outpos++;
            }
            break;
        case '+':
            if (out) {
                write_bcd(out, outpos, bcd_plus);
                outpos++;
            }
            break;
        case '-':
            if (out) {
                write_bcd(out, outpos, bcd_minus);
                outpos++;
            }
            break;
        case 'e':
            if (out) {
                write_bcd(out, outpos, bcd_e);
                outpos++;
            }
            break;
        default:
            /* character we can't handle */
            return 1;
        }
    }

    /* worked OK */
    return 0;
}

static int compress_bcdate(const char *in, char *out)
{
    if (strlen(in) > INLINE_STR_LEN * 2) {
        /* too long */
        return 1;
    }

    /* zero output buffer */
    if (out) {
        memset(out, 0, INLINE_STR_LEN);
    }
    int outpos = 0;
    for (const char *inp = in; *inp; inp++) {
        switch (*inp) {
        case '1':
            if (out) {
                write_bcd(out, outpos, bcdate_1);
                outpos++;
            }
            break;
        case '2':
            if (out) {
                write_bcd(out, outpos, bcdate_2);
                outpos++;
            }
            break;
        case '3':
            if (out) {
                write_bcd(out, outpos, bcdate_3);
                outpos++;
            }
            break;
        case '4':
            if (out) {
                write_bcd(out, outpos, bcdate_4);
                outpos++;
            }
            break;
        case '5':
            if (out) {
                write_bcd(out, outpos, bcdate_5);
                outpos++;
            }
            break;
        case '6':
            if (out) {
                write_bcd(out, outpos, bcdate_6);
                outpos++;
            }
            break;
        case '7':
            if (out) {
                write_bcd(out, outpos, bcdate_7);
                outpos++;
            }
            break;
        case '8':
            if (out) {
                write_bcd(out, outpos, bcdate_8);
                outpos++;
            }
            break;
        case '9':
            if (out) {
                write_bcd(out, outpos, bcdate_9);
                outpos++;
            }
            break;
        case '0':
            if (out) {
                write_bcd(out, outpos, bcdate_0);
                outpos++;
            }
            break;
        case ':':
            if (out) {
                write_bcd(out, outpos, bcdate_colon);
                outpos++;
            }
            break;
        case '+':
            if (out) {
                write_bcd(out, outpos, bcdate_plus);
                outpos++;
            }
            break;
        case '-':
            if (out) {
                write_bcd(out, outpos, bcdate_minus);
                outpos++;
            }
            break;
        case 'T':
            if (out) {
                write_bcd(out, outpos, bcdate_T);
                outpos++;
            }
            break;
        case 'Z':
            if (out) {
                write_bcd(out, outpos, bcdate_Z);
                outpos++;
            }
            break;
        default:
            /* character we can't handle */
            return 1;
        }
    }

    /* worked OK */
    return 0;
}

static char *uncompress_bcd(unsigned char *bcd)
{
    char *out = calloc(INLINE_STR_LEN*2 + 1, sizeof(char));

    for (int inpos = 0; inpos < INLINE_STR_LEN*2; inpos++) {
        unsigned int code = bcd[inpos/2];
        if (inpos % 2 == 0) {
            code &= 15;
        } else {
            code >>= 4;
        }
        if (code == bcd_nul) {
            break;
        }
        out[inpos] = bcd_map[code];
    }

    return out;
}

static char *uncompress_bcdate(unsigned char *bcd)
{
    char *out = calloc(INLINE_STR_LEN*2 + 1, sizeof(char));

    for (int inpos = 0; inpos < INLINE_STR_LEN*2; inpos++) {
        unsigned int code = bcd[inpos/2];
        if (inpos % 2 == 0) {
            code &= 15;
        } else {
            code >>= 4;
        }
        if (code == bcdate_nul) {
            break;
        }
        out[inpos] = bcdate_map[code];
    }

    return out;
}

/* vi:set expandtab sts=4 sw=4: */
