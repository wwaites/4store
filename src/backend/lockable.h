#ifndef LOCKABLE_H
#define LOCKABLE_H

typedef struct _fs_lockable_t {
    int fd;
    int flags;
    int locktype;
    char *filename;
    struct timespec mtime;
    int (*read_metadata)(struct _fs_lockable_t *hf);
    int (*write_metadata)(struct _fs_lockable_t *hf);
} fs_lockable_t;

int fs_lockable_init(fs_lockable_t *);
int fs_lockable_lock(fs_lockable_t *, int operation);

/* returns 1 if the type of lock specified by op is held, otherwise 0 */
#define fs_lockable_test(hf, op) ((hf->locktype & op) ? 1 : 0)

#endif /* LOCKABLE_H */
