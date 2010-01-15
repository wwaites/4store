#ifndef HASHFILE_H
#define HASHFILE_H

typedef struct _fs_hashfile_t {
    int fd;
    int flags;
    char *filename;
    struct timespec mtime;
    int (*read_metadata)(struct _fs_hashfile_t *hf);
    int (*write_metadata)(struct _fs_hashfile_t *hf);
} fs_hashfile_t;

int fs_hashfile_init(fs_hashfile_t *);
int fs_hashfile_lock(fs_hashfile_t *, int operation);
int fs_hashfile_sync(fs_hashfile_t *);

/*
 * Darwin doesn't have a real implementation of fsync(2). Instead, recent
 * versions provide a fcntl(2) call to actually sync data to disc. Since
 * we want to know that write operations have happened, we need to use that.
 * On other systems we just use fsync as normal.
 * */
#ifdef _DARWIN_C_SOURCE
#ifndef F_FULLFSYNC
#error "No F_FULLFSYNC to reliably get data to disk. Upgrade Darwin to 10.3 or greater."
#endif
#define fs_fsync(fd) fcntl(fd, F_FULLFSYNC, NULL)
#else /* not darwin */
#define fs_fsync(fd) fsync(fd)
#endif

#endif /* HASHFILE_H */
