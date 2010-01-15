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

#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <glib.h>
#include <sys/stat.h>
#include <sys/file.h>

#include "lock.h"
#include "common/error.h"

int fs_lock_kb(const char *kb)
{
    char *fn = g_strdup_printf(FS_MD_FILE, kb);
    int fd = open(fn, FS_O_NOATIME | O_RDONLY | O_CREAT, 0600);
    if (fd == -1) {
        fs_error(LOG_CRIT, "failed to open metadata file %s for locking: %s",
                 fn, strerror(errno));

        return 1;
    }
    g_free(fn);
    if (flock(fd, LOCK_EX | LOCK_NB) == -1) {
        if (errno == EWOULDBLOCK) {
	    fs_error(LOG_ERR, "cannot get lock for kb “%s”", kb);

            return 1;
        }
        fs_error(LOG_ERR, "failed to get lock: %s", strerror(errno));

        return 1;
    }

    return 0;
}           

int fs_flock_logged(int fd, int op, const char *file, int line)
{
    char opstr[8] = { 0, 0, 0, 0, 0 };
    if (op & LOCK_SH) strcat(opstr, "s");
    else strcat(opstr, "-");
    if (op & LOCK_EX) strcat(opstr, "e");
    else strcat(opstr, "-");
    if (op & LOCK_NB) strcat(opstr, "n");
    else strcat(opstr, "-");
    if (op & LOCK_UN) strcat(opstr, "u");
    else strcat(opstr, "-");
    
    printf("@@L %s %d:%d\t%s:%d\n", opstr, getpid(), fd, file, line);

    return flock(fd, op);
}

/* vi:set expandtab sts=4 sw=4: */
