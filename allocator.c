#include <stdio.h>
#include <assert.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <assert.h>
#include <string.h>
#include <sys/mman.h>
#include <stdlib.h>

#include "inner.h"

typedef uint32_t ckid_t;	//chunk id
typedef uint32_t lpid_t;	//local page id
#define NULL_CK	((uint64_t)(-1L))

struct allocator_s {
	ckid_t curr_ck;
	struct busy_page_num_s *bpn;
	struct page_bitmap_s *pb;
};

void file_allocate(kvdb_t db, uint64_t pos, uint64_t len)
{
	struct file_header_s *h = db->h;
	int ret;

	if (h->file_size>=pos+len) {
		return;
	}
	ret = posix_fallocate(db->fd, pos, len);
	kvdb_assert(ret==0);
	h->file_size = pos + len;
}

	/* find a chunk which has free pages to allocate 
	 * return r if success
	 * return (ckid_t)-1 if failed 
     */
static ckid_t find_ck(kvdb_t db, ckid_t ck)
{
	ckid_t i, r; 
	struct allocator_s *alc = db->alc;

	for (i=0; i<MAX_CHUNK_NUM; i++) {
		r = (ck + i) % MAX_CHUNK_NUM;
		if (alc->bpn->n[i]<PAGE_NUM_PER_CK) {
			return r;
		}
	}
	return (ckid_t)-1;
}

/* get the page's position */
uint64_t get_page_pos(gpid_t gpid) {
	return (FILE_META_LEN + gpid*PAGE_SIZE);
}

static gpid_t get_gpid(ckid_t ck, lpid_t lpid)
{
	return (((gpid_t)ck)*PAGE_NUM_PER_CK + lpid);
}

uint64_t get_ck_pos(ckid_t ck)
{
	gpid_t gpid;
	uint64_t pos;
	gpid = get_gpid(ck, 0);
	pos = get_page_pos(gpid);
	return pos;
}

static void close_curr_ck(kvdb_t db)
{
	struct allocator_s *alc = db->alc; 
	int ret; 

	kvdb_assert(alc->curr_ck!=(ckid_t)-1);
	kvdb_assert(alc->pb!=NULL);

	ret = msync(alc->pb, PAGE_BITMAP_LEN, MS_SYNC);
	kvdb_assert(ret==0); 

	ret = munmap(alc->pb, PAGE_BITMAP_LEN);
	kvdb_assert(ret==0); 

	alc->curr_ck = (ckid_t)-1;
	alc->pb = NULL;
}

/* to check if the bitmap is set*/
static int pb_isset(kvdb_t db, lpid_t pg)
{
	uint32_t w = pg >> 6;
	uint32_t b = pg & 63;
	
	return (db->alc->pb->w[w] & (1<<b))!=0; 
}

/* set the bitmap's first two bytes all 1  */
static void pb_set(kvdb_t db, lpid_t pg)
{
	uint32_t w = pg >> 6;
	uint32_t b = pg & 63;

	db->alc->pb->w[w] |= (1<<b);
}

/* clear the bitmap */
static void pb_clr(kvdb_t db, lpid_t pg)
{
	uint32_t w = pg >> 6;
	uint32_t b = pg & 63;
	
	db->alc->pb->w[w] &= ~(1<<b);
}

/* 
 * open_ck() -- load a page bitmap into memory. At any moment, there is only 
 * 				one ck could be staying in the memory to provide free pages.
 */
static void open_ck(kvdb_t db, ckid_t ck)
{
	uint64_t pos;
	struct allocator_s *alc = db->alc; 
	int new = 0;
	lpid_t i;
	
	kvdb_assert(alc->curr_ck==(ckid_t)-1);
	kvdb_assert(ck != (ckid_t)-1);
	
	alc->curr_ck = ck; 
	pos = get_ck_pos(ck);
	//check if busy page num == 0 and enough memory to allocate
	if (alc->bpn->n[ck]==0) {
		if (db->h->file_size < pos+PAGE_BITMAP_LEN) {
			file_allocate(db, pos, PAGE_BITMAP_LEN);
		}
		new = 1;
	}
	alc->pb = mmap(NULL, sizeof(struct page_bitmap_s), 
			PROT_READ|PROT_WRITE, MAP_SHARED, 
			db->fd, pos);
	kvdb_assert(alc->bpn!=MAP_FAILED);
	if (new) {
		alc->bpn->n[ck] = PAGE_BITMAP_PAGES;
		for (i=0; i<PAGE_BITMAP_PAGES; i++) {
			pb_set(db, i);
		}
	}
}

gpid_t alloc_page(kvdb_t db)
{
	struct allocator_s *alc = db->alc; 
	ckid_t ck = alc->curr_ck; 
	lpid_t lpid;
	gpid_t gpid; 
	uint64_t pos;

	kvdb_assert(ck!=(ckid_t)-1);

	/* 
	 * If there is not any free page in the chunk, then we find the next one 
	 * and turn to it */
	if (alc->bpn->n[ck]>=PAGE_NUM_PER_CK) {
		close_curr_ck(db);
		ck = find_ck(db, ck);
		/* TODO: reach the maximum length of the file, need to deal with it */
		kvdb_assert(ck!=(ckid_t)-1);	
		open_ck(db, ck);
	}

	/* Find a free page in the chunk */
	for (lpid=PAGE_BITMAP_PAGES; lpid<PAGE_NUM_PER_CK; lpid++) {
		gpid = get_gpid(ck, lpid);
		if (!pb_isset(db, gpid))
			break;
	}
	kvdb_assert(lpid<PAGE_NUM_PER_CK);

	pb_set(db, gpid);
	alc->bpn->n[ck] ++; 

	pos = get_page_pos(gpid);
	if (db->h->file_size < pos + PAGE_SIZE) {
		file_allocate(db, pos, PAGE_SIZE);
	}
	return gpid;
}

void free_page(kvdb_t db, gpid_t gpid)
{
	ckid_t ck = (ckid_t)(gpid/PAGE_NUM_PER_CK);

	kvdb_assert(pb_isset(db, gpid));
	pb_clr(db, gpid);
	db->alc->bpn->n[ck] --;
	db->h->spare_pages ++;
	/* TODO: truncate those free pages at the tail of the database file */
	/* TODO: Do we need to implement some GC things? */
}

void sync_allocator(kvdb_t db)
{
	int ret;

	if (db->alc->pb!=NULL) {
		ret = msync(db->alc->pb, PAGE_BITMAP_LEN, MS_SYNC);
		kvdb_assert(ret==0); 
	}

	kvdb_assert(db->alc->bpn!=NULL);
	ret = msync(db->alc->bpn, sizeof (struct busy_page_num_s), MS_SYNC);
	kvdb_assert(ret==0); 

}

void exit_allocator(kvdb_t db)
{
	int ret;

	sync_allocator(db);

	if (db->alc->pb!=NULL) {
		ret = munmap(db->alc->pb, PAGE_BITMAP_LEN);
		kvdb_assert(ret==0); 
		db->alc->curr_ck = (ckid_t)-1;
		db->alc->pb = NULL;
	}

	ret = munmap(db->alc->bpn, sizeof (struct busy_page_num_s));
	kvdb_assert(ret==0); 
	db->alc->bpn = NULL;
	free(db->alc);
	db->alc = NULL;
}

void init_allocator(kvdb_t db)
{
	struct allocator_s *alc; 
	int new = 0; 
	ckid_t ck;

	alc = (struct allocator_s *)malloc(sizeof(*alc));
	kvdb_assert(alc!=NULL);
	memset(alc, 0, sizeof(*alc));
	db->alc = alc;
	alc->curr_ck = (ckid_t)-1;
	
	/* 
	 * if the file size smaller than the area of busy page number, the file is
	 * a new one, so it is needed to be expanded.
	 */
	if (db->h->file_size < BUSY_PAGE_NUM_POS + sizeof(struct busy_page_num_s)) {
		file_allocate(db, BUSY_PAGE_NUM_POS, sizeof(struct busy_page_num_s));
		new = 1;
	}

	alc->bpn = mmap(NULL, sizeof(struct busy_page_num_s), 
			PROT_READ|PROT_WRITE, MAP_SHARED, 
			db->fd, BUSY_PAGE_NUM_POS);
	kvdb_assert(alc->bpn!=MAP_FAILED);

	if (new)
		memset(alc->bpn, 0, sizeof(struct busy_page_num_s));

	ck = find_ck(db, 0);
	kvdb_assert(ck!=(ckid_t)-1);

	open_ck(db, ck);
}

