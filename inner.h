#ifndef __kvdb_inner_h__
#define __kvdb_inner_h__

#include <stdint.h>
#include <unistd.h>
#include "kvdb.h"

#define PAGE_SIZE		4096ULL  //4kb per page
#define FILE_META_LEN		(2*1024*1024ULL) //2mb for metadata
#define BUSY_PAGE_NUM_POS	(1*1024*1024ULL) /*
											  *1mb for busy page numbers,
											  *4bytes per chunk to declared
											  *the using number of pages 
											  */
#define PAGE_BITMAP_LEN		(64*1024ULL)		//64kb per bitmap 
#define PAGE_BITMAP_PAGES	(PAGE_BITMAP_LEN/PAGE_SIZE) //2bytes set as 1
#define PAGE_NUM_PER_CK		(PAGE_BITMAP_LEN*8) //64*1024*8 page num per ck
#define PAGE_BITMAP_WLEN	(64*1024ULL/8)		//page_num_per_ck/64(uint64_t)
#define MAX_CHUNK_NUM		(256*1024ULL)	//max chunk number
#define CHUNK_DATA_LEN		(PAGE_BITMAP_LEN*8*PAGE_SIZE) //2GB
#define DATA_AREA_LEN		(MAX_CHUNK_NUM*CHUNK_DATA_LEN) //512TB

#define RECORD_NUM_PG		((PAGE_SIZE/sizeof(struct record_s)) - 1)

#define kvdb_assert(cond)	__kvdb_assert(cond, __FUNCTION__, __FILE__, __LINE__)

void __kvdb_assert(int cond, const char *func, char *file, int line);

#define _pl() fprintf(stderr, "func=%s(), at %s:%d\n", __FUNCTION__, __FILE__, __LINE__)

typedef uint64_t gpid_t;	// global page id
#define GPID_NIL	((gpid_t)-1)

struct record_s {
	uint64_t k;
	uint64_t v;
};

#define PAGE_LEAF	(1<<0)

struct page_header_s {
	int32_t  record_num;
	uint32_t flags;
	gpid_t   next;
};

struct page_s {
	struct page_header_s h;
	struct record_s      rec[RECORD_NUM_PG];
};

struct file_header_s {
	uint64_t magic;
	uint64_t file_size;
	uint64_t record_num;
	uint64_t total_pages;
	uint64_t spare_pages;
	uint32_t level;
	uint32_t reserve;
	gpid_t   root_gpid;
};

struct page_bitmap_s {
	uint64_t w[PAGE_BITMAP_WLEN];
};

struct busy_page_num_s {
	uint32_t n[MAX_CHUNK_NUM];
};

struct allocator_s;
struct cache_s;

struct pg_s;
typedef struct pg_s *pg_t;

struct kvdb_s {
	int fd;
	struct file_header_s *h;
	struct allocator_s *alc;
	struct cache_s *ch;
};

struct cursor_s {
	gpid_t	gpid;
	pg_t    pg;
	struct  page_s *p;
	int	pos;
	uint64_t start_key;
	uint64_t end_key;
};

/* allocator */
void init_allocator(kvdb_t db);
void exit_allocator(kvdb_t db);
void sync_allocator(kvdb_t db);
gpid_t alloc_page(kvdb_t db);
void free_page(kvdb_t db, gpid_t pg);
void file_allocate(kvdb_t db, uint64_t pos, uint64_t len);
uint64_t get_page_pos(gpid_t gpid);

/* cache */
void init_cache(kvdb_t db); 
void exit_cache(kvdb_t db);

pg_t get_page(kvdb_t db, gpid_t gpid);
void put_page(kvdb_t db, pg_t pg);
struct page_s *get_page_buf(kvdb_t db, pg_t pg);
void mark_page_dirty(kvdb_t db, pg_t pg);

void sync_all_page(kvdb_t db);

#endif //__kvdb_inner_h__


