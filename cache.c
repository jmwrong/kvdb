#include <string.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <stdio.h>

#include "inner.h"

#define MAX_CACHE_SIZE	(1ULL<<20)		// 1MB for test
#define MAX_MAPPED_PG	(MAX_CACHE_SIZE/PAGE_SIZE)
#define EVECT_NUM	(128)

#define PAGE_HASH_NUM	(MAX_MAPPED_PG)
#define PAGE_HASH_MASK	(MAX_MAPPED_PG - 1)

#define PG_DIRTY	(1<<0)
#define PG_BUSY		(1<<1)

struct node_s {
	struct node_s *prev;
	struct node_s *next;
};

struct pg_s {
	uint32_t flags;		// off:0
	uint32_t reserv;	
	gpid_t gpid;		// off:8
	struct page_s *buf; 	// off:16
	struct node_s hash;	// for hash, off:24
	struct node_s link;	// for lru, off:40
};

struct cache_s {
	uint64_t mapped_num;
	uint64_t busy_num;
	uint64_t free_num;
	struct node_s hash[PAGE_HASH_NUM];
	struct node_s free;			// free list head
	struct node_s busy;			// busy list head
};


void list_init(struct node_s *h)
{
	h->prev = h; 
	h->next = h;
}

void list_add_tail(struct node_s *n, struct node_s *h) //add a node in the end of the linklist
{
	struct node_s *prev = h->prev;
	struct node_s *next = h;
	
	n->prev = prev;
	n->next = next;
	prev->next = n;
	next->prev = n;
}

void list_add(struct node_s *n, struct node_s *h) //add a node in front of the linklist
{
	struct node_s *prev = h;
	struct node_s *next = h->next;
	
	n->prev = prev;
	n->next = next;
	prev->next = n;
	next->prev = n;
}

void list_del(struct node_s *n) //delete a node
{
	struct node_s *prev = n->prev;
	struct node_s *next = n->next;
	
	prev->next = n->next;
	next->prev = n->prev;
	n->prev = n;
	n->next = n;
}

int list_empty(struct node_s *h)
{
	return h->next == h;
}

struct pg_s *link_pg(struct node_s *n)
{
	long off = (long)(char *)(&((struct pg_s *)0)->link); //off:40
	char *ptr = (char *)n - off;
	return (struct pg_s *)ptr;
}

struct pg_s *hash_pg(struct node_s *n)
{
	long off = (long)(char *)(&((struct pg_s *)0)->hash);//off:24
	char *ptr = (char *)n - off;
	return (struct pg_s *)ptr;
}

void dump_cache(kvdb_t d)
{
	int i;
	struct cache_s *ch = d->ch;
	struct node_s *h, *n;
	struct pg_s *p;
	
	fprintf(stderr, "dump_cache(): \n");
	fprintf(stderr, "  mapped_num = %lu\n", ch->mapped_num);
	fprintf(stderr, "  busy_num = %lu\n", ch->busy_num);
	fprintf(stderr, "  free_num = %lu\n", ch->free_num);
	for (i=0; i<PAGE_HASH_NUM; i++) {
		h = &ch->hash[i];
		if (list_empty(h))
			continue;
		for (n=h->next; n!=h; n=n->next) {
			p = hash_pg(n);
			fprintf(stderr, "  i=%4d p=%p  flg=%8x  gpid=%8lu  buf=%p \n", 
				i, p, p->flags, p->gpid, p->buf);
		}
	}
	fprintf(stderr, "\n");
}

void init_cache(kvdb_t db)
{
	struct cache_s *ch; 
	int i;

	ch = (struct cache_s *)malloc(sizeof(*ch));
	kvdb_assert(ch!=NULL);
	
	db->ch = ch;
	ch->mapped_num = 0;
	list_init(&ch->free);
	list_init(&ch->busy);
	for (i=0; i<PAGE_HASH_NUM; i++) {
		list_init(&ch->hash[i]);
	}
}

void walk_link_page(struct node_s *head, void (*fn)(struct pg_s *))
{
	struct node_s *n; 
	struct pg_s *p;

	for (n=head->next; n!=head; n=n->next) {
		p = link_pg(n);
		fn(p);
	}
}

void sync_page(struct pg_s *p) //write in the page
{
	int ret;
	if ((p->flags & PG_DIRTY) == 0)
		return;
	ret = msync(p->buf, PAGE_SIZE, MS_SYNC);
	kvdb_assert(ret==0);
	p->flags &= ~PG_DIRTY;
}

void sync_all_page(kvdb_t db)
{
	walk_link_page(&db->ch->free, sync_page);
	walk_link_page(&db->ch->busy, sync_page);
}

void evict_page(kvdb_t db, struct pg_s *p)
{
	int ret; 

	if ((p->flags & PG_DIRTY) != 0) {
		ret = msync(p->buf, PAGE_SIZE, MS_SYNC);
		kvdb_assert(ret==0);
		p->flags &= ~PG_DIRTY;
	}
	list_del(&p->link);
	list_del(&p->hash);
	ret = munmap(p->buf, PAGE_SIZE);
	kvdb_assert(ret==0);
	db->ch->mapped_num --;
	if (p->flags & PG_BUSY) {
		db->ch->busy_num --;
	} else {
		db->ch->free_num --;
	}
	free(p);
}

void exit_cache(kvdb_t db)
{
	struct pg_s *p;

	while(!list_empty(&db->ch->free)) {
		p = link_pg(db->ch->free.next);
		evict_page(db, p);
	}
	while(!list_empty(&db->ch->busy)) {
		p = link_pg(db->ch->free.next);
		evict_page(db, p);
	}
	free(db->ch);
	db->ch = NULL;
}

uint32_t pg_hash(gpid_t gpid)
{
	uint32_t a, b, c;
	a = (uint32_t)gpid;
	b = (uint32_t)(gpid>>22);
	c = (uint32_t)(gpid>>44);
	return (uint32_t)((a^b^c) & PAGE_HASH_MASK);
}

struct pg_s *find_page(kvdb_t db, gpid_t gpid, uint32_t bucket)
{
	struct node_s *head, *n;
	struct pg_s *p;

	head = &db->ch->hash[bucket]; 
	for (n=head->next; n!=head; n=n->next) {
		p = hash_pg(n);
		if (p->gpid == gpid)
			return p;
	}
	return NULL;
}

pg_t get_page(kvdb_t db, gpid_t gpid)
{
	uint32_t bucket;
	struct pg_s *p;
	//evict half pages while mapped_num >= MAX_MAPPED_PG
	if (db->ch->mapped_num >= MAX_MAPPED_PG) {
		while (!list_empty(&db->ch->free)
			&& db->ch->mapped_num >= (MAX_MAPPED_PG/2)) {
			p = link_pg(db->ch->free.prev);
			evict_page(db, p);
		}
	}

	bucket = pg_hash(gpid);
	p = find_page(db, gpid, bucket);
	if (p!=NULL) {
		kvdb_assert((p->flags & PG_BUSY) == 0);

		list_del(&p->link);
		list_add(&p->link, &db->ch->busy);
		db->ch->free_num --;
	} else {
		uint64_t pos;

		p = malloc(sizeof(*p));
		kvdb_assert(p!=NULL);
		p->flags = 0;
		p->gpid = gpid;
		pos = get_page_pos(gpid);
		p->buf = (struct page_s *)mmap(NULL, PAGE_SIZE, 
						PROT_READ|PROT_WRITE, 
						MAP_SHARED, 
						db->fd, pos);
		kvdb_assert(p->buf!=NULL);
		list_add(&p->hash, &db->ch->hash[bucket]);
		list_add(&p->link, &db->ch->busy);
		db->ch->mapped_num ++;
	}
	kvdb_assert((p->flags & PG_BUSY) == 0);
	p->flags |= PG_BUSY;
	db->ch->busy_num ++;

	return p;
}

void put_page(kvdb_t db, pg_t p)
{
	kvdb_assert((p->flags & PG_BUSY) != 0);
	list_del(&p->link);
	list_add(&p->link, &db->ch->free);
	p->flags &= ~PG_BUSY;
	db->ch->free_num ++;
	db->ch->busy_num --;
}

struct page_s *get_page_buf(kvdb_t db, pg_t pg)
{
	return pg->buf;
}

void mark_page_dirty(kvdb_t db, pg_t pg)
{
	pg->flags |= PG_DIRTY;
}


