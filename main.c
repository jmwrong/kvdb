#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "kvdb.h"

uint64_t kv_crc64(const unsigned char *buffer, uint64_t length);

void usage(void)
{
	printf(
		"    kv help                   -- this message \n"\
		"    kv get <key>              -- get a key\n"\
		"    kv put <key> <val>        -- set key\n"\
		"    kv del <key>              -- delete a key\n"\
		"    kv list                   -- list all key in the db\n"\
		"    kv ins <start_key> <num>  -- insert records in batch mode\n"\
		"    kv clr                    -- remove all records in the database\n"\
		"    kv verify                 -- get all records and verify them\n"\
		);
}

struct cmd_s {
	char	*cmd;
	int (*func)(kvdb_t kv, int argc, char *argv[]);
};

static void expect(int argc, int expected)
{
	if (argc!=expected) {
		fprintf(stderr, "expected %d arguments, but there are %d.\n", expected, argc);
		usage();
		exit(1);
	}
}

static int fn_get(kvdb_t d, int argc, char *argv[])
{
	uint64_t k, v;
	int ret;

	expect(argc, 3);
	k = strtoul(argv[2], NULL, 10);
	ret = kvdb_get(d, k, &v);
	if (ret==0) {
		printf("found, key = %lu, value = %lu\n", k, v);
	} else {
		printf("record not found\n");
		//printf("argv=%d, argv[2]=%s", argc, argv[2]);
	}
	return 0;
}

static int fn_put(kvdb_t d, int argc, char *argv[])
{
	uint64_t k, v;

	expect(argc, 4);
	k = strtoul(argv[2], NULL, 10);
	v = strtoul(argv[3], NULL, 10);
	kvdb_put(d, k, v);
	return 0;
}

static int fn_del(kvdb_t d, int argc, char *argv[])
{
	uint64_t k;
	int ret;

	expect(argc, 3);
	k = strtoul(argv[2], NULL, 10);
	ret = kvdb_del(d, k);
	if (ret!=0) {
		printf("deletion failed\n");
	} else {
		printf("deletion success\n");
	}
	return 0;
}

static int fn_dump(kvdb_t d, int argc, char *argv[])
{
	expect(argc, 2);
	kvdb_dump(d);
}

static int fn_list(kvdb_t d, int argc, char *argv[])
{
	cursor_t cs;
	uint64_t k, v;
	int i=0; 

	expect(argc, 2);

	cs = kvdb_open_cursor(d, 0, (uint64_t)(-1));
	while (kvdb_get_next(d, cs, &k, &v)==0) {
		printf("%5d, k = %-21lu, v = %-21lu\n", i, k, v);
		i++;
	}
	kvdb_close_cursor(d, cs);
	return 0;
}

static int fn_ins(kvdb_t d, int argc, char *argv[])
{
	uint64_t start_k, seq, k, v, i, n; 
	time_t t0, last, now;
	uint64_t us0, us1, last_i;
	
	expect(argc, 4);
	last = t0 = time(NULL);
	start_k = strtoul(argv[2], NULL, 10);
	n = strtoul(argv[3], NULL, 10);
	last_i = 0;
	for (i=0; i<n; i++) {
		seq = start_k + i;
		k = kv_crc64((const unsigned char *)&seq, sizeof(k));
		v = kv_crc64((const unsigned char *)&k, sizeof(k));
		kvdb_put(d, k, v);
		if ((i%100)==0) {
			now = time(NULL);
			if (now-last>=1) {
				us0 = 1000000UL * (now - last);
				us1 = 1000000UL * (now - t0);
				printf("total: %lu in %lu sec, avarage: %lu us/record\n", 
					i, now-t0, us1/i);
				printf("last %lu sec: %lu, avarage: %lu us/record\n", 
					now-last, i-last_i, us0/(i-last_i+1));
				last = now;
				last_i = i;
			}
		}
	}
	return 0;
}

static int fn_clr(kvdb_t d, int argc, char *argv[])
{
	return 0;
}

static int fn_verify(kvdb_t d, int argc, char *argv[])
{
	return 0;
}

static struct cmd_s cmds[] = {
	{"get", fn_get}, 
	{"put", fn_put}, 
	{"del", fn_del}, 
	{"list", fn_list}, 
	{"dump", fn_dump},
	{"ins", fn_ins}, 
	{"ins", fn_clr}, 
	{"verify", fn_verify}, 
	{NULL, NULL},
};

int main(int argc, char *argv[]) 
{
	kvdb_t kv; 
	struct cmd_s *c;

	if (argc<2) {
		usage();
		return 0;
	}
	kv = kvdb_open("aaa.db");
	for (c=cmds; c->cmd!=NULL; c++) {
		if (strcmp(c->cmd, argv[1])==0) {
			c->func(kv, argc, argv);
			break;
		}
	}
	if (c->cmd==NULL) {
		usage();
	}
	kvdb_close(kv);
	return 0;
}

