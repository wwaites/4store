/*
 *  Copyright (C) 2009 Steve Harris
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  $Id: listtest.c $
 */

#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <glib.h>

#include "list.h"
#include "sort.h"

#define ROWS 20443501
//#define ROWS 3000

int main(int argc, char *argv[])
{
    char *filename = g_strdup_printf("/tmp/test-%d.list", (int)getpid());
    fs_lockable_t *l = fs_list_open_filename(filename, sizeof(fs_rid) * 4, O_CREAT | O_TRUNC | O_RDWR);
    srand(time(NULL));
    fs_lockable_lock(l, LOCK_EX);
    for (int i=0; i<100; i++) {
        fs_rid quad[4] = { i+23, i+23, i+23, i+23 };
        fs_list_add_r(l, quad);
    }
    fs_lockable_lock(l, LOCK_UN);

    fs_lockable_lock(l, LOCK_SH);
    fs_list_rewind_r(l);
    for (int i=0; 1; i++) {
        fs_rid quad[4] = { 0, 0, 0, 0 };
        int got = fs_list_next_value_r(l, quad);
        if (!got && i < 100) {
            printf("ERROR got %d, less than 100 items from list\n", i);
        }
        if (!got) break;
        if (i > 99) {
            printf("ERROR got more than 100 items from list\n");
        }
        if (quad[0] != i+23 || quad[1] != i+23 || quad[2] != i+23 || quad[3] != i+23) {
            printf("ERORR found %016llx %016llx %016llx %016llx, expecting all %ds\n",
                   quad[0], quad[1], quad[2], quad[3], i);
        }
    }
    fs_lockable_lock(l, LOCK_UN);

    fs_lockable_lock(l, LOCK_EX);
    fs_list_rewind_r(l);
    if (fs_list_sort_chunked_r(l, quad_sort_by_mspo)) {
        printf("failed to sort list");
    }
    for (int i=0; 1; i++) {
        fs_rid quad[4] = { 0, 0, 0, 0 };
        int got = fs_list_next_sort_uniqed_r(l, quad);
        if (!got && i < 100) {
            printf("ERROR got %d, less than 100 items from list\n", i);
        }
        if (!got) break;
        if (i > 99) {
            printf("ERROR got more than 100 items from list\n");
        }
        if (quad[0] != i+23 || quad[1] != i+23 || quad[2] != i+23 || quad[3] != i+23) {
            printf("ERORR found %016llx %016llx %016llx %016llx, expecting all %ds\n",
                   quad[0], quad[1], quad[2], quad[3], i);
        }
    }
    fs_lockable_lock(l, LOCK_UN);

    fs_lockable_lock(l, LOCK_EX);
    for (int i=0; i<ROWS; i++) {
        /* 8 x 32bit ints = 4 x RIDs */
        int32_t quad[8] = { rand(), rand(), rand(), rand(),
                            rand(), rand(), rand(), rand() };
        fs_list_add_r(l, quad);
    }
    fs_lockable_lock(l, LOCK_UN);

    double then = fs_time();
    printf("sorting %.1f Mbytes of data\n", (double)(sizeof(fs_rid) * 4 * ROWS)/(1024.0 * 1024.0));
    if (fs_list_sort_chunked(l, quad_sort_by_mspo)) {
        printf("failed to sort list");
    }
    double now = fs_time();
    printf("sort took %.1fs\n", now-then);
    fs_list_print(l, stdout, 0);

    fs_lockable_lock(l, LOCK_EX);
    fs_rid quad[4];
    fs_list_rewind_r(l);
    fs_rid last = 0LL;
    while (fs_list_next_sort_uniqed_r(l, quad)) {
        if (last >= quad[0]) {
            printf("found %016llx after %016llx, not sorted\n", quad[0], last);
        }
        last = quad[0];
    }
    fs_lockable_lock(l, LOCK_UN);

    fs_list_unlink(l);
    fs_list_close(l);

    return 0;
}

/* vi:set expandtab sts=4 sw=4: */
