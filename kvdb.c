#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <sys/mman.h>
#include <string.h>
#include <errno.h>

#include "kvdb.h"
#include "inner.h"

#define FILE_HEADER_LEN		PAGE_SIZE

#define OK		0
#define PAGE_DELETED	1
#define REC_NOT_FOUND	2
#define PAGE_SPLITED	3
#define REC_REPLACED	4
#define REC_INSERTED	5
#define FOUND_EXACT	6
#define FOUND_GREATER	7


/*
 * TODO: to dump all items in the call stack
 */
void __kvdb_assert(int cond, const char *func, char *file, int line)
{
	if (!cond) {//assert while cond==0
		perror("error");
		fprintf(stderr, "assertion violation: %s:%d, func=%s(), errno=%d\n", 
			file, line, func, errno);
		abort();
	}
}

/* 
 * dump a page header and all records in that page
 */
void _kvdb_dump_page(gpid_t gpid, struct page_s *p)
{
	int i;

	fprintf(stderr, "kvdb_dump_page():\n");
	fprintf(stderr, "gpid = %lu\n", gpid);
	fprintf(stderr, "h.record_num = %u\n", p->h.record_num);
	fprintf(stderr, "h.flags = %x\n", p->h.flags);
	fprintf(stderr, "h.next = %lx\n", p->h.next);
	for (i=0; i<(int)p->h.record_num; i++) {
		fprintf(stderr, "kv: i=%3d, k=%lu, v=%lu\n", i, p->rec[i].k, p->rec[i].v);
	}
	fprintf(stderr, "\n");
}

/*
 * dump a page which did not open before
 * then put it free
 */
void kvdb_dump_page(kvdb_t d, gpid_t gpid)
{
	pg_t pg;
	struct page_s *p;

	pg = get_page(d, gpid);
	p = get_page_buf(d, pg);
	_kvdb_dump_page(gpid, p);
	put_page(d, pg);
}

/*
 * dump the database file header
 */
void kvdb_dump_head(kvdb_t d)
{
	fprintf(stderr, "kvdb header: \n");
#define pr(f) fprintf(stderr, "%10s: %lu\n", #f, (uint64_t)d->h->f)
	pr(record_num);
	pr(root_gpid);
	pr(level);
	pr(total_pages);
	pr(spare_pages);
#undef pr
	fprintf(stderr, "\n");
}

/*
 * dump the whole B+ Tree in the database file
 */
void kvdb_dump_tree(kvdb_t d, gpid_t gpid)
{
	pg_t pg;
	struct page_s *p;
	int i;

	pg = get_page(d, gpid);
	p = get_page_buf(d, pg);
	_kvdb_dump_page(gpid, p);
	if ((p->h.flags & PAGE_LEAF)==0) {
		for (i=0; i<p->h.record_num; i++) {
			kvdb_dump_tree(d, p->rec[i].v);
		}
	}
	put_page(d, pg);
}

/*
 * dump all contents in the database
 */
void kvdb_dump(kvdb_t d)
{
	fprintf(stderr, "\ndump the whole tree\n");
	kvdb_dump_head(d);
	kvdb_dump_tree(d, d->h->root_gpid);
	fprintf(stderr, "\n");
}

/*
 * open the database
 * TODO: we do not discriminate RDONLY and RDWR now, so may we could do it later.
 */
kvdb_t kvdb_open(char *name)
{
	int fd;
	kvdb_t d;
	struct stat st;
	int ret;
	int new = 0;

	fd = open(name, O_CREAT|O_RDWR|__O_DIRECT, 0666); //读写，同步新建或打开name
	if (fd<0) {
		kvdb_assert(0);		// this failure seems most unlikely happens
		return NULL;
	}
	d = (kvdb_t)malloc(sizeof(*d));
	kvdb_assert(d!=NULL);//空间申请失败则终止
	d->fd = fd;
	ret = fstat(d->fd, &st);//将d->fd 所指向的文件状态复制到结构stat中 成功0 失败-1
	kvdb_assert(ret==0);//文件状态复制失败则终止
	if (st.st_size<FILE_HEADER_LEN) {
		ret = posix_fallocate(d->fd, 0, FILE_HEADER_LEN);
		if (ret!=0) {
			printf("posix_fallocate failed, errno = %d\n", ret);
			abort();
		}
		new = 1;
	}

	/* 
	 * mmap the file head and get its pointer saving in d->h 
	 * NOTICE: the flag be MAP_SHARED or modified data cannot be updated 
	 * into the file
	 */
	d->h = (struct file_header_s *)mmap(NULL, FILE_HEADER_LEN, 
		PROT_READ|PROT_WRITE, MAP_SHARED, d->fd, 0);		
	kvdb_assert(d->h!=MAP_FAILED);//MAP_FAILED即指针0xFFFFFFFF 判断是否成功映射

	/* if the database is created right before, we should initialize the header of the file */
	if (new) {
		char *m = (char *)&d->h->magic;
		strcpy(m, "kv@enmo");
		memset(d->h, 0, FILE_HEADER_LEN);
		d->h->record_num = 0;
		d->h->root_gpid = GPID_NIL;
		d->h->level = 0;
		d->h->total_pages = 0;
		d->h->spare_pages = 0;
	}

	d->h->file_size = st.st_size;

	init_allocator(d);
	init_cache(d);
	return d;
}

/*
 * close the database
 */
int kvdb_close(kvdb_t db) 
{
	int ret;

	exit_cache(db);
	exit_allocator(db);

	ret = msync(db->h, PAGE_SIZE, MS_SYNC);//刷新变化函数
	kvdb_assert(ret==0);

	ret = munmap(db->h, PAGE_SIZE); //解除内存映射函数
	kvdb_assert(ret==0);

	ret = fsync(db->fd);//同步内存中所有已修改的文件数据到储存设备
	kvdb_assert(ret==0);

	ret = close(db->fd);//close为linux系统调用函数
	kvdb_assert(ret==0);
	
	return 0;
}

/* 
 * bpt_make_root(): make the root page of the B+ Tree 
 *
 * this functions would be called in two cases: 
 * 1) when the first record is being inserted into the database which 
 *    was created newly
 * 2) when a page is fully filled with records and a new record is
 *    arrived to be inserted into that page, so we should split the 
 *    LEAF page, and it caused the root page full too. we must increase
 *    the level of the tree and create a new root page.
 */
static void bpt_make_root(kvdb_t d, int leaf)
{
	struct page_s *p; 
	pg_t pg;
	gpid_t gpid; 

	gpid = alloc_page(d);//页面分配
	kvdb_assert(gpid!=GPID_NIL);

	d->h->root_gpid = gpid;
	d->h->level ++; 

	pg = get_page(d, gpid);
	p = get_page_buf(d, pg);
	p->h.record_num = 0;
	p->h.flags = (leaf ? PAGE_LEAF : 0);
	p->h.next = GPID_NIL;
	put_page(d, pg);
}

/* 
 * find a key in a page, return the index of the record which
 * rec[index].k == k
 * rec[index].k < k < rec[index+1].k
 */
static int find_key(struct page_s *p, uint64_t k)
{
	int mi, lo, hi; 

	lo = 0; 
	hi = p->h.record_num - 1;

	if (p->h.record_num<=0 || k<p->rec[lo].k) {
		return -1;
	}

	if (k >= p->rec[hi].k) {
		return hi;
	}

	while (lo <= hi) {
		mi = (lo + hi) / 2;
		if (k == p->rec[mi].k) {
			return mi;
		}
		if (k > p->rec[mi].k) {
			if (k < p->rec[mi+1].k) {
				return mi;
			} else {
				lo = mi + 1;
			}
		} else {
			hi = mi - 1;
		}
	}
	kvdb_assert(0);
	return -1;
}

/* insert a record into a page */
static int insert_rec(kvdb_t d, pg_t pg, struct page_s *p, int pos, struct record_s *rec)
{
	int i;
	int ret = OK;
	
	//fprintf(stderr, "insert_rec(): p=%p, (%s) pos=%d, rec=(%lu, %lu)\n", 
	//		p, (p->h.flags&PAGE_LEAF ? "leaf" : "branch"), pos, rec->k, rec->v);
	if (p->h.record_num==0) {
		p->rec[0].k = rec->k;
		p->rec[0].v = rec->v;
		p->h.record_num = 1;
		mark_page_dirty(d, pg);
		return  REC_INSERTED;
	}

	kvdb_assert(p->h.record_num < RECORD_NUM_PG);

	if (rec->k > p->rec[pos].k) {
		kvdb_assert(pos == p->h.record_num-1 || p->rec[pos].k < p->rec[pos+1].k);
		for (i=p->h.record_num; i>pos+1; i--) {
			p->rec[i].k = p->rec[i-1].k;
			p->rec[i].v = p->rec[i-1].v;
		}
		p->rec[pos+1].k = rec->k;
		p->rec[pos+1].v = rec->v;
		p->h.record_num ++;
		ret = REC_INSERTED;
	} else if (rec->k == p->rec[pos].k) {
		/* If the page is a branch in the tree, its record should not be replaced */
		kvdb_assert((p->h.flags & PAGE_LEAF) != 0);
		/* replace the value */
		p->rec[pos].v = rec->v;
		ret = REC_REPLACED;
	} else if (pos==0 && rec->k<p->rec[pos].k){
		for (i=p->h.record_num; i>0; i--) {
			p->rec[i].k = p->rec[i-1].k;
			p->rec[i].v = p->rec[i-1].v;
		}
		p->rec[0].k = rec->k;
		p->rec[0].v = rec->v;
		p->h.record_num ++;
		ret = REC_INSERTED;
	} else {
		/* rec->k should not be less than p->rec[pos].k */
		kvdb_assert(0);
	}
	mark_page_dirty(d, pg);
	return ret;
}


/* 
 * split current page into two pages and insert a record which pointer to the new one 
 * into parent page. This function may be the most complex in the kvdb, so make sure 
 * you have understood it before you try to change it.
 */
static void bpt_split(kvdb_t d, pg_t ppg, struct page_s *parent, int _ppos, struct page_s *curr)
{
	struct page_s *p; 
	gpid_t new_gpid;
	pg_t pg, up_pg;
	struct page_s *up = parent;
	int need_to_put = 0;
	int i, j;
	int ppos = _ppos;
	int half; 
	struct record_s rec;

	/* If the current page is root, then we make a new root page and
	 * reduce the old root to be a inferior to the new one as a leaf.
	 */
	if (parent==NULL) {
		rec.k = curr->rec[0].k;
		rec.v = d->h->root_gpid;

		bpt_make_root(d, 0);
		up_pg = get_page(d, d->h->root_gpid);
		up = get_page_buf(d, up_pg);
		insert_rec(d, up_pg, up, -1, &rec);
		need_to_put = 1;
		ppos = 0;
	} else {
		up_pg = ppg;
		up = parent;
	}
	
	/*
	 * allocate a new page and copy the last half records in the current page to 
	 * the new one.
	 */
	new_gpid = alloc_page(d);
	pg = get_page(d, new_gpid);
	p = get_page_buf(d, pg);
	
	half = curr->h.record_num/2;
	for (i=half; i<curr->h.record_num; i++) {
		j = i - half;
		p->rec[j].k = curr->rec[i].k;
		p->rec[j].v = curr->rec[i].v;
	}
	p->h.flags = curr->h.flags;
	p->h.next = curr->h.next;
	p->h.record_num = curr->h.record_num - half;
	curr->h.record_num = half;
	curr->h.next = new_gpid;

	/* insert new record which pointed to the new page into the parent page */
	rec.k = p->rec[0].k;
	rec.v = (uint64_t)new_gpid;
	insert_rec(d, up_pg, up, ppos, &rec);

	/* release new page */
	put_page(d, pg);		

	if (need_to_put) {
		/* release parent page if it is necessary */
		put_page(d, up_pg);
	}
}

/* 
 * bpt_insert -- insert a record into a B+ Tree
 *
 * ppos - the position of current page record in parent page
 *        if the parent is root, then ppos==-1
 * curr -- the current page's gpid
 * rec -- the record to be inserted
 * 
 * return OK, success
 *
 * return PAGE_SPLITED, means the caller should the function again because the page is full 
 * and it split into two pieces
 */
static int bpt_insert(kvdb_t d, pg_t ppg, struct page_s *parent, int ppos, gpid_t curr, struct record_s *rec)
{
	struct page_s *p;
	pg_t pg;
	int pos;
	int ret;

	//fprintf(stderr, "bpt_insert(): gpid=%lu, ppos=%d, parent=%p, rec=(%lu, %lu)\n", 
	//		curr, ppos, parent, rec->k, rec->v);

	pg = get_page(d, curr);
	p = get_page_buf(d, pg);
	if (p->h.record_num>=RECORD_NUM_PG) {
		bpt_split(d, ppg, parent, ppos, p);
		put_page(d, pg);
		return PAGE_SPLITED;
	}

	if (p->h.flags & PAGE_LEAF) {
		pos = find_key(p, rec->k);
		if (pos<0) {
			pos = 0;
		}
		ret = insert_rec(d, pg, p, pos, rec);
	} else {
		int tries = 0; 

		do {
			pos = find_key(p, rec->k);
			if (pos<0) {
				pos = 0;
			}
			ret = bpt_insert(d, pg, p, pos, (gpid_t)p->rec[pos].v, rec);
			tries ++;
		} while (ret==PAGE_SPLITED);
		kvdb_assert(tries<=2);
	}
	put_page(d, pg);

	return ret==REC_REPLACED ? REC_REPLACED : REC_INSERTED;
}

int kvdb_put(kvdb_t d, uint64_t k, uint64_t v)
{
	int tries = 0; 
	struct record_s rec;
	int ret;

	if (d->h->level==0) {
		bpt_make_root(d, 1);
	}
	rec.k = k;
	rec.v = v;
	do {
		ret = bpt_insert(d, NULL, NULL, -1, d->h->root_gpid, &rec);
		tries ++;
		kvdb_assert(tries<=2);
	} while (ret==PAGE_SPLITED);

	if (ret!=REC_REPLACED) {
		d->h->record_num ++;
	}

	return 0;
}

void delete_rec(struct page_s *p, int pos)
{
	int i;

	if (p->h.record_num == 1) {
		p->h.record_num = 0;
		return;
	}

	for (i=pos; i<p->h.record_num-1; i++) {
		p->rec[i].k = p->rec[i+1].k;
		p->rec[i].v = p->rec[i+1].v;
	}
	p->h.record_num --;
}


/* 
 * return OK            -- success, nothing following step is needed to do .
 *        PAGE_DELETED  -- success, the page is empty and has been deleted, so the entry which 
 *                         pointed to that page is needed to delete.
 *        REC_NOT_FOUND -- there is not the record to be deleted
 *  
 */
int bpt_del(kvdb_t d, gpid_t gpid, uint64_t k)
{
	pg_t pg;
	struct page_s *p;
	int pos, ret = OK;

	pg = get_page(d, gpid);
	p = get_page_buf(d, pg);
	pos = find_key(p, k);

	if ((p->h.flags & PAGE_LEAF) == 0) {
		ret = bpt_del(d, (gpid_t)p->rec[pos].v, k);
		if (ret == PAGE_DELETED) {
			delete_rec(p, pos);
			if (p->h.record_num == 0) {
				goto delete_page;
			}
		}
	} else {
		if (p->rec[pos].k != k) {
			ret = REC_NOT_FOUND;
		} else {
			delete_rec(p, pos);
			if (p->h.record_num == 0) {
				goto delete_page;
			} else {
				ret = OK;
			}
		}
	}

	put_page(d, pg);
	return ret;		/* OK or NOT_FOUND */

delete_page:
	put_page(d, pg);
	free_page(d, gpid);
	return PAGE_DELETED;
}

int kvdb_del(kvdb_t d, uint64_t k)
{
	int ret;

	kvdb_dump_page(d, d->h->root_gpid);

	ret = bpt_del(d, d->h->root_gpid, k);
	if (ret==PAGE_DELETED) {
		d->h->level = 0;
		d->h->root_gpid = GPID_NIL;
	}
	if (ret==PAGE_DELETED || ret==OK) {
		d->h->record_num --;
	}
	return ret==REC_NOT_FOUND ? -1: 0;
}

int bpt_search(kvdb_t d, gpid_t gpid, uint64_t k, struct record_s *rec, struct cursor_s *cs)
{
	pg_t pg;
	struct page_s *p;
	int pos;
	gpid_t next;
	int ret;


	pg = get_page(d, gpid);
	p = get_page_buf(d, pg);
	pos = find_key(p, k);
	/*
	fprintf(stderr, "bpt_search(): gpid=%lu(%s), k=%lu, pos=%d, \n", 
		gpid, (p->h.flags&PAGE_LEAF ? "leaf" : "branch"), k, pos);
	if ((p->h.flags & PAGE_LEAF) == 0) {
		_kvdb_dump_page(gpid, p);
	}*/
	if ((p->h.flags & PAGE_LEAF) != 0) {
		if (pos<0) {
			ret = REC_NOT_FOUND;
			goto end;
		}
		if (rec!=NULL && p->rec[pos].k==k) {
			rec->k = p->rec[pos].k;
			rec->v = p->rec[pos].v;
		}
		ret = (p->rec[pos].k==k ? FOUND_EXACT : FOUND_GREATER);
		goto end;
	}

	if (pos<0) {
		pos = 0;
	}
	next = (gpid_t)p->rec[pos].v;
	put_page(d, pg);
	return bpt_search(d, next, k, rec, cs);

end:
	if (cs!=NULL) {
		cs->gpid = gpid;
		cs->pg = pg;
		cs->p = p;
		cs->pos = pos;
	} else {
		put_page(d, pg);
	}
	return ret;
}

/* 
 * return 0 -- we have found it
 *       -1 -- have not fount it
 */
int kvdb_get(kvdb_t d, uint64_t k, uint64_t *v)
{
	int ret;
	struct record_s rec;
	
	kvdb_dump_page(d, d->h->root_gpid);
	ret = bpt_search(d, d->h->root_gpid, k, &rec, NULL);
	if (ret==FOUND_EXACT) {
		*v = rec.v;
		return 0;
	}
	return -1;
}

void dump_cursor(kvdb_t db, cursor_t cs)
{
	fprintf(stderr, "cursor: cs->gpid=%lu, cs->pg=%p, cs->p=%p, cs->pos=%d\n", 
			cs->gpid, cs->pg, cs->p, cs->pos);
	_kvdb_dump_page(cs->gpid, cs->p);
}

cursor_t kvdb_open_cursor(kvdb_t db, uint64_t start_key, uint64_t end_key)
{
	struct cursor_s *cs;

	cs = malloc(sizeof(*cs));
	kvdb_assert(cs!=NULL);
	
	cs->start_key = start_key;
	cs->end_key = end_key;

	if (db->h->record_num==0 && db->h->root_gpid==GPID_NIL) {
		cs->gpid = GPID_NIL;
		cs->pg = NULL;
		cs->p = NULL;
		cs->pos = -1;
		return cs;
	}
	/* 
	 * the 'pos' in cursor would be -1 if there is not record could be returned, 
	 * so we do not care about the return value in this case.
	 */
	bpt_search(db, db->h->root_gpid, start_key, NULL, cs);

	return cs;
}


int kvdb_get_next(kvdb_t db, cursor_t cs, uint64_t *k, uint64_t *v)
{
	if (cs->gpid == GPID_NIL) {
		return -1;
	}
	if (cs->pos >= cs->p->h.record_num) {
		if (cs->p->h.next == GPID_NIL) {
			return -1;
		}
		put_page(db, cs->pg);
		cs->gpid = cs->p->h.next;
		cs->pg = get_page(db, cs->gpid);
		cs->p = get_page_buf(db, cs->pg);
		cs->pos = 0;
	}
	kvdb_assert((cs->p->h.flags & PAGE_LEAF) != 0);
	kvdb_assert(cs->p->h.record_num > 0);
	kvdb_assert(cs->pos < cs->p->h.record_num);

	if (cs->pos==-1) {
		cs->pos = 0;
	}

	if (cs->p->rec[cs->pos].k>=cs->end_key) {
		return -1;
	}
	
	*k = cs->p->rec[cs->pos].k;
	*v = cs->p->rec[cs->pos].v;
	cs->pos ++;

	return 0;
}

void kvdb_close_cursor(kvdb_t db, cursor_t cs)
{
	if (cs->gpid!=GPID_NIL)
		put_page(db, cs->pg);
	free(cs);
}

