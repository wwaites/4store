#ifndef LOCKABLE_H
#define LOCKABLE_H

#include <fcntl.h>
#include <sys/stat.h>

typedef struct _fs_lockable_t {
    int fd;
    int flags;
    int locktype;
    char *filename;
    struct timespec mtime;
    int (*lock)(struct _fs_lockable_t *, int operation); // internal, do not use directly
    int (*read_metadata)(struct _fs_lockable_t *);
    int (*write_metadata)(struct _fs_lockable_t *);
} fs_lockable_t;

int fs_lockable_init(fs_lockable_t *);
int fs_lockable_lock_debug(fs_lockable_t *, int operation, const char *, int);

#define fs_lockable_lock(l, op) fs_lockable_lock_debug((fs_lockable_t *)l, op, __FILE__, __LINE__)
/* acquire the lock if we do not already have it */
#define fs_lockable_locka(l, op) ( (((fs_lockable_t *)l)->locktype == op) || \
                                   fs_lockable_lock_debug((fs_lockable_t *)l, op, __FILE__, __LINE__))
/* returns 1 if the type of lock specified by op is held, otherwise 0 */
#define fs_lockable_test(l, op) ((((fs_lockable_t *)l)->locktype & op) ? 1 : 0)

/*
 * Darwin doesn't have a real implementation of fsync(2). Instead, recent
 * versions provide a fcntl(2) call to actually sync data to disc. Since
 * we want to know that write operations have happened, we need to use that.
 * On other systems we just use fsync as normal.
 *
 */
#ifdef _DARWIN_C_SOURCE
#ifndef F_FULLFSYNC
#error "No F_FULLFSYNC to reliably get data to disk. Upgrade Darwin to 10.3 or greater."
#endif
#define fs_fsync(fd) fcntl(fd, F_FULLFSYNC, NULL)
#else /* not darwin */
#define fs_fsync(fd) fsync(fd)
#endif


#endif /* LOCKABLE_H */
