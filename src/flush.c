#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "mongo.h"
#include "chopper.h"

void flush_valid(st_http_request * scanned_lines, int countval)
{
    if (globalArgs.host != NULL && globalArgs.collection != NULL) { 
        flush_to_mongo(scanned_lines, countval);
    }else{
	    flush_to_stdout(scanned_lines, countval);
    }
}

void flush_invalid(char **invalid_lines, int countval)
{
    if (globalArgs.outFileNameInvalid != NULL) {
	FILE *pWrite;
	pWrite = fopen(globalArgs.outFileNameInvalid, "a");
	int flush_count;
	for (flush_count = 0; flush_count < countval; flush_count++) {
	    fprintf(pWrite, "%s", invalid_lines[flush_count]);
	}
	fclose(pWrite);
    }
}

int flush_to_mongo(st_http_request * p, int counter)
{
    mongo conn;
    mongo_init(&conn);
    mongo_set_op_timeout(&conn, 10000);
    int status = mongo_client(&conn, globalArgs.host, globalArgs.port);
    if (status != MONGO_OK) {
	switch (conn.err) {
	case MONGO_CONN_SUCCESS:
	    printf("Connected to mongo\n");
	case MONGO_CONN_NO_SOCKET:
	    printf("FAIL: Could not create a socket!\n");
	    break;
	case MONGO_CONN_ADDR_FAIL:
	    printf("FAIL: MONGO_CONN_ADDR_FAIL: %s\n", globalArgs.host);
	    break;
	case MONGO_CONN_NOT_MASTER:
	    printf("FAIL: MONGO_CONN_NOT_MASTER\n");
	    break;
	case MONGO_CONN_BAD_SET_NAME:
	    printf("FAIL: MONGO_CONN_BAD_SET_NAME\n");
	    break;
	case MONGO_CONN_NO_PRIMARY:
	    printf("FAIL: MONGO_CONN_NO_PRIMARY\n");
	    break;
	case MONGO_IO_ERROR:
	    printf("FAIL: MONGO_IO_ERROR\n");
	    break;
	case MONGO_SOCKET_ERROR:
	    printf("FAIL: MONGO_SOCKET_ERROR\n");
	    break;
	case MONGO_READ_SIZE_ERROR:
	    printf("FAIL: MONGO_READ_SIZE_ERROR\n");
	    break;
	case MONGO_COMMAND_FAILED:
	    printf("FAIL: MONGO_COMMAND_FAILED\n");
	    break;
	case MONGO_WRITE_ERROR:
	    printf("FAIL: MONGO_WRITE_ERROR\n");
	    break;
	case MONGO_NS_INVALID:
	    printf("FAIL: MONGO_NS_INVALID\n");
	    break;
	case MONGO_BSON_INVALID:
	    printf("FAIL: MONGO_BSON_INVALIDr\n");
	    break;
	case MONGO_BSON_NOT_FINISHED:
	    printf("FAIL: MONGO_BSON_NOT_FINISHED\n");
	    break;
	case MONGO_BSON_TOO_LARGE:
	    printf("FAIL: MONGO_BSON_TOO_LARGEr\n");
	    break;
	case MONGO_WRITE_CONCERN_INVALID:
	    printf("FAIL: Mongo write concern invalid\n");
	    break;
	case MONGO_CONN_FAIL:
	    printf
		("FAIL: Mongo connection fail. Make sure it's listening at %s:%d\n",
		 globalArgs.host, globalArgs.port);
	    break;
	}

	exit(EXIT_FAILURE);
    }

    bson **bps;
    bps = (bson **) malloc(sizeof(bson *) * counter);

    char *fields = globalArgs.fields;
    int i = 0;
    for (i = 0; i < counter; i++) {
	bson *bp = (bson *) malloc(sizeof(bson));
	bson_init(bp);
	bson_append_new_oid(bp, "_id");
	if (fields == NULL || strstr(fields, "req_ip") != NULL)
	    bson_append_string(bp, "req_ip", p[i].req_ip);
	if (fields == NULL || strstr(fields, "req_ident") != NULL)
	    bson_append_string(bp, "req_ident", p[i].req_ident);
	if (fields == NULL || strstr(fields, "req_user") != NULL)
	    bson_append_string(bp, "req_user", p[i].req_user);
	if (fields == NULL || strstr(fields, "req_datetime") != NULL)
	    bson_append_string(bp, "req_datetime", p[i].req_datetime);
	if (fields == NULL || strstr(fields, "req_method") != NULL)
	    bson_append_string(bp, "req_method", p[i].req_method);
	if (fields == NULL || strstr(fields, "req_uri") != NULL)
	    bson_append_string(bp, "req_uri", p[i].req_uri);
	if (fields == NULL || strstr(fields, "req_proto") != NULL)
	    bson_append_string(bp, "req_proto", p[i].req_proto);
	if (fields == NULL || strstr(fields, "resp_code") != NULL)
	    bson_append_int(bp, "resp_code", p[i].resp_code);
	if (fields == NULL || strstr(fields, "resp_bytes") != NULL)
	    bson_append_int(bp, "resp_bytes", atoi(p[i].resp_bytes));
	if (fields == NULL || strstr(fields, "req_referer") != NULL)
	    bson_append_string(bp, "req_referer", p[i].req_referer);
	if (fields == NULL || strstr(fields, "req_agent") != NULL)
	    bson_append_string(bp, "req_agent", p[i].req_agent);
	bson_finish(bp);
	bps[i] = bp;
    }
    mongo_insert_batch(&conn, globalArgs.collection, (const bson **) bps,
		       counter, 0, MONGO_CONTINUE_ON_ERROR);

    mongo_destroy(&conn);

    int j = 0;
    for (j = 0; j < counter; j++) {
	bson_destroy(bps[j]);
	free(bps[j]);
    }
    free(bps);
    return (0);
}

int flush_to_stdout(st_http_request * p, int counter)
{
    char *fields = globalArgs.fields;
    int flush_count;
    for (flush_count = 0; flush_count < counter; flush_count++) {
	if (fields == NULL || strstr(fields, "req_ip") != NULL)
	    printf("%s ", p[flush_count].req_ip);
	if (fields == NULL || strstr(fields, "req_ident") != NULL)
	    printf("%s ", p[flush_count].req_ident);
	if (fields == NULL || strstr(fields, "req_user") != NULL)
	    printf("%s ", p[flush_count].req_user);
	if (fields == NULL || strstr(fields, "req_datetime") != NULL)
	    printf("[%s] ", p[flush_count].req_datetime);
	if (fields == NULL || strstr(fields, "req_method") != NULL)
	    printf("\"%s ", p[flush_count].req_method);
	if (fields == NULL || strstr(fields, "req_uri") != NULL)
	    printf("%s ", p[flush_count].req_uri);
	if (fields == NULL || strstr(fields, "req_proto") != NULL)
	    printf("%s\" ", p[flush_count].req_proto);
	if (fields == NULL || strstr(fields, "resp_code") != NULL)
	    printf("%d ", p[flush_count].resp_code);
	if (fields == NULL || strstr(fields, "resp_bytes") != NULL)
	    printf("%s ", p[flush_count].resp_bytes);
	if (fields == NULL || strstr(fields, "req_referer") != NULL)
	    printf("\"%s\" ", p[flush_count].req_referer);
	if (fields == NULL || strstr(fields, "req_agent") != NULL)
	    printf("\"%s\"", p[flush_count].req_agent);
	printf("\n");
    }
    return (0);
}
