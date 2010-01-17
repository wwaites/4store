#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <assert.h>
#include <sys/file.h>
#include <sys/stat.h>
#include "lockable.h"
#include "common/error.h"

extern int errno;

/* flush any cached data to disc after writing out the required metadata */
static int fs_lockable_sync(fs_lockable_t *hf)
{
    /* make sure we have a write lock */
    assert(hf->locktype & LOCK_EX);

    /* write out any necessary metadata */
    if (hf->write_metadata && (hf->write_metadata)(hf))
        return -1;
    /* flush data to disc */
    if (fs_fsync(hf->fd)) {
        fs_error(LOG_ERR, "flock(%s): %s", hf->filename, strerror(errno));
        return -1;
    }
    return 0;
}

int fs_lockable_lock_debug(fs_lockable_t *hf, int operation, const char *file, int line)
{
    /* this should not happen */
    fs_assert(hf);

    //fs_error(LOG_INFO, "%s:%d fs_lockable_lock(%s): %01x", file, line, hf->filename, operation);

    /* It is an error to try to upgrade / downgrade locks */
    if ( (operation & LOCK_EX && hf->locktype & LOCK_SH) ||
         (operation & LOCK_SH && hf->locktype & LOCK_EX) ) {
        fs_error(LOG_ERR, "%s:%d fs_lockable_lock(%s): up/downgrading lock not permitted",
                 file, line, hf->filename);
        return -1;
    }

    /* It is an error to request a lock while holding one already */
    if ( (operation & hf->locktype) & (LOCK_SH|LOCK_EX) ) {
        fs_error(LOG_ERR, "%s:%d fs_lockable_lock(%s): double lock",
                 file, line, hf->filename);
        return -1;
    }

    return (hf->lock)(hf, operation);
}

static int fs_lockable_do_lock(fs_lockable_t *hf, int operation)
{
    struct stat stat;

    /* if we are unlocking while holding a write lock, flush data */
    if ( (hf->locktype & LOCK_EX) && (operation & LOCK_UN) ) {
        if (fs_lockable_sync(hf))
            return -1;
        /* update the mtime before releasing the lock */
        if (fstat(hf->fd, &stat) < 0) {
            fs_error(LOG_ERR, "fstat(%s): %s", hf->filename, strerror(errno));
            return -1;
        }
        memcpy(&hf->mtime, &stat.st_mtimespec, sizeof(struct timespec));
    }

    /* release or acquire the lock */
    if (flock(hf->fd, operation)) {
        fs_error(LOG_ERR, "flock(%s): %s", hf->filename, strerror(errno));
        return -1;
    }

    hf->locktype = operation;

    /* if we are acquiring the lock, read any metadata if necessary */
    if ( hf->read_metadata && (operation & (LOCK_EX|LOCK_SH)) ) {
        if (fstat(hf->fd, &stat) < 0) {
            fs_error(LOG_ERR, "fstat(%s): %s", hf->filename, strerror(errno));
            return -1;
        }
        if ( (stat.st_mtimespec.tv_sec > hf->mtime.tv_sec) ||
             ( (stat.st_mtimespec.tv_sec == hf->mtime.tv_sec) &&
               (stat.st_mtimespec.tv_nsec > hf->mtime.tv_nsec) ) ) {
           if ( (hf->read_metadata)(hf) )
               return -1;
        }
    }

    return 0;
}

/*
 * Initialize the hashfile, reading or writing any header
 * or metadata as appropriate. Handles locking. Returns
 * 0 on success, -1 on error. T
 */
int fs_lockable_init(fs_lockable_t *hf)
{
    struct stat stat;
    int file_length;

    /* read or create the file header metadata */
    if ( (hf->flags & O_TRUNC) ) {
        /* we have truncated the file, so write a header */
        if (flock(hf->fd, LOCK_EX)) {
            fs_error(LOG_ERR, "flock(%s): %s", hf->filename, strerror(errno));
            return -1;
        }
        if ( hf->write_metadata && (hf->write_metadata)(hf) ) {
            if (flock(hf->fd, LOCK_UN))
                fs_error(LOG_ERR, "flock(%s): %s", hf->filename, strerror(errno));
            return -1;
        }
        /* flush data to disc */
        fs_fsync(hf->fd);
        /* downgrade the lock */
        if (flock(hf->fd, LOCK_SH)) {
            fs_error(LOG_ERR, "flock(%s): %s", hf->filename, strerror(errno));
            if (flock(hf->fd, LOCK_UN))
                fs_error(LOG_ERR, "flock(%s): %s", hf->filename, strerror(errno));
            return -1;
        }
    } else {
        /* check if the file is empty, we have created it,
         * don't take exclusive lock yet so we don't 
         * unnecessarily block */
        if (flock(hf->fd, LOCK_SH)) {
            fs_error(LOG_ERR, "flock(%s): %s", hf->filename, strerror(errno));
            return -1;
        }
        file_length = lseek(hf->fd, 0, SEEK_END);
        if (file_length < 0) {
            fs_error(LOG_ERR, "lseek(%s, SEEK_END): %s", hf->filename, strerror(errno));
            return -1;
        }
        if (file_length == 0) {
            /* empty file, check again with an upgraded lock */
            flock(hf->fd, LOCK_EX);
            file_length = lseek(hf->fd, 0, SEEK_END);
            if (file_length < 0) {
                fs_error(LOG_ERR, "lseek(%s, SEEK_END): %s", hf->filename, strerror(errno));
                return -1;
            }
            if (file_length == 0) {
                if ( hf->write_metadata && (hf->write_metadata)(hf) ) {
                    flock(hf->fd, LOCK_UN); 
                    return -1;
                }
            }
            /* flush data to disc */
            if (fs_fsync(hf->fd)) {
                fs_error(LOG_ERR, "fsync(%s): %s", hf->filename, strerror(errno));
                return -1;
            }
            /* downgrade the lock */
            if (flock(hf->fd, LOCK_SH)) {
                fs_error(LOG_ERR, "flock(%s): %s", hf->filename, strerror(errno));
                if (flock(hf->fd, LOCK_UN))
                    fs_error(LOG_ERR, "flock(%s): %s", hf->filename, strerror(errno));
                return -1;
            }
        }
    }

    /* we are now holding a read lock, read in the header */
    if ( hf->read_metadata && (hf->read_metadata)(hf) ) {
        if (flock(hf->fd, LOCK_UN))
            fs_error(LOG_ERR, "flock(%s): %s", hf->filename, strerror(errno));
        return -1;
    }

    /* update the mtime */
    if (fstat(hf->fd, &stat)) {
        fs_error(LOG_ERR, "fstat(%s): %s", hf->filename, strerror(errno));
        if (flock(hf->fd, LOCK_UN))
            fs_error(LOG_ERR, "flock(%s): %s", hf->filename, strerror(errno));
        return -1;
    }
    memcpy(&hf->mtime, &stat.st_mtimespec, sizeof(struct timespec));

    /* done, we have consistent state and can release the lock */
    if (flock(hf->fd, LOCK_UN)) {
        fs_error(LOG_ERR, "flock(%s): %s", hf->filename, strerror(errno));
        return -1;
    }

    /* set our flag to keep track of the lock type */
    hf->locktype = LOCK_UN;
    /* and the pointer to the lock function */
    hf->lock = fs_lockable_do_lock;

    return 0;
}

