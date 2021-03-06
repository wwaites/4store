#ifndef UPDATE_H
#define UPDATE_H

#include "common/4store.h"

/* Perform a SPARUL-type update on l.

The UTF-8 text of the update operation should be in update, and any error
messages will be piunted to by *message. Set unsafe to non-0 to enable unsafe
network operations, eg. LOAD */

int fs_update(fsp_link *l, char *update, char **message, int unsafe);

#endif
