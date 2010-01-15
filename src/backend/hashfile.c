#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include "hashfile.h"
#include "sync.h"
#include "common/error.h"

extern int errno;

int fs_hashfile_lock(fs_hashfile_t *hf, int operation)
{
    struct stat stat;

    if (flock(hf->fd, operation)) {
        fs_error(LOG_ERR, "flock(%s): %s", hf->filename, strerror(errno));
        return -1;
    }

    if ( (operation & LOCK_EX) || (operation & LOCK_SH) ) {
        if (fstat(hf->fd, &stat) < 0) {
            fs_error(LOG_ERR, "fstat(%s): %s", hf->filename, strerror(errno));
            return -1;
        }
        if ( (stat.st_mtimespec.tv_sec > hf->mtime.tv_sec) ||
             ( (stat.st_mtimespec.tv_sec == hf->mtime.tv_sec) &&
               (stat.st_mtimespec.tv_nsec > hf->mtime.tv_nsec) ) ) {
           if ( (hf->read_metadata)(hf) )
               return -1;
           memcpy(&hf->mtime, &stat.st_mtimespec, sizeof(struct timespec));
        }
    }
    return 0;
}

/*
 * Initialize the hashfile, reading or writing any header
 * or metadata as appropriate. Handles locking. Returns
 * 0 on success, -1 on error. T
 */
int fs_hashfile_init(fs_hashfile_t *hf)
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
        if ( (hf->write_metadata)(hf) ) {
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
                if ( (hf->write_metadata)(hf) ) {
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
    if ( (hf->read_metadata)(hf) ) {
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

    return 0;
}

int fs_hashfile_sync(fs_hashfile_t *hf)
{
    struct stat stat;
    /* write out any necessary metadata */
    if ((hf->write_metadata)(hf))
        return -1;
    /* flush data to disc */
    if (fs_fsync(hf->fd)) {
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
    return 0;
}
