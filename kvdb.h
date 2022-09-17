#ifndef __kvdb_h__
#define __kvdb_h__

#include <stdint.h>

struct kvdb_s;
typedef struct kvdb_s *kvdb_t;

struct cursor_s;
typedef struct cursor_s *cursor_t;

kvdb_t kvdb_open(char *name);
int kvdb_close(kvdb_t db);
int kvdb_get(kvdb_t db, uint64_t k, uint64_t *v);
int kvdb_put(kvdb_t db, uint64_t k, uint64_t v);
int kvdb_del(kvdb_t db, uint64_t k);

cursor_t kvdb_open_cursor(kvdb_t db, uint64_t start_key, uint64_t end_key);
int kvdb_get_next(kvdb_t db, cursor_t cs, uint64_t *k, uint64_t *v);
int kvdb_del_next(kvdb_t db, cursor_t cs, uint64_t *k, uint64_t *v);
void kvdb_close_cursor(kvdb_t db, cursor_t cs);

void kvdb_dump(kvdb_t d);

#endif 
