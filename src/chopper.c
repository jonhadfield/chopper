#include <zlib.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <getopt.h>
#include <string.h>
#include "mongo.h"
#include "chopper.h"

const char chopper_usage_string[] =
    "chopper [-s|--search_string] [-o|--outfile <path>] [-t|--type] [-b|--batchsize <value>] [-h|--host <value>]\n"
    "           [-p|--port <value>] [-c|--collection <db.collection>][-s|--search_string <value>] [-v|--verbose]\n"
    "           [-v|--verbose] [-h|--help]\n"
    "           <command> [<args>]";

typedef struct {
    char req_ip[MAX_IP];
    char req_ident[MAX_IDENT];
    char req_user[MAX_USER];
    char req_datetime[MAX_DATETIME];
    char req_method[MAX_METHOD];
    char req_uri[MAX_LINE_LENGTH];
    char req_proto[MAX_PROTO];
    int resp_code;
    char resp_bytes[MAX_RESP_BYTES];
    char req_referer[MAX_REFERER];
    char req_agent[MAX_AGENT];
} st_http_request;

struct globalArgs_t {
    const char *outFileName;	/* -o option */
    FILE *outFile;
    char *type;			/* -t option */
    char *batch_size;		/* -b option */
    char *host;			/* -h option */
    int port;			/* -p option */
    char *collection;		/* -c option */
    char *search_string;	/* -s option */
    int verbose;		/* verbosity */
    char **inputFiles;		/* input files */
    int numInputFiles;		/* # of input files */
} globalArgs;

static const char *optString = "o:t:b:h:p:c:s:v?";

static const struct option longOpts[] = {
    {"outFileName", required_argument, NULL, 'o'},
    {"type", required_argument, NULL, 't'},
    {"batch_size", required_argument, NULL, 'b'},
    {"host", required_argument, NULL, 'h'},
    {"port", required_argument, NULL, 'p'},
    {"collection", required_argument, NULL, 'c'},
    {"search_string", required_argument, NULL, 's'},
    {"verbose", no_argument, NULL, 'v'},
    {"help", no_argument, NULL, 'h'},
    {NULL, no_argument, NULL, 0}
};

void display_usage(void)
{
    puts(chopper_usage_string);
    exit(EXIT_FAILURE);
}

int flush_to_disk(st_http_request * p, int counter)
{
    FILE *pWrite;
    pWrite = fopen(globalArgs.outFileName, "a+");
    //printf("flushing %d lines to disk\n", counter+1);
    int flush_count;
    for (flush_count = 0; flush_count < counter; flush_count++) {
	fprintf(pWrite, "%s %d %s\n", p[flush_count].req_uri,
		p[flush_count].resp_code, p[flush_count].resp_bytes);
    }
    fclose(pWrite);
    return (0);
}

int flush_to_mongo(st_http_request * p, int counter)
{

    mongo conn;
    if (mongo_connect(&conn, globalArgs.host, globalArgs.port) != MONGO_OK) {
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

	exit(1);
    }

    bson **bps;
    bps = (bson **) malloc(sizeof(bson *) * counter);
    int i = 0;
    bson *bp = (bson *) malloc(sizeof(bson));
    bson_init(bp);
    for (i = 0; i < counter; i++) {
	bson_append_new_oid(bp, "_id");
	bson_append_string(bp, "req_ip", p[i].req_ip);
	bson_append_string(bp, "req_ident", p[i].req_ident);
	bson_append_string(bp, "req_user", p[i].req_user);
	bson_append_string(bp, "req_datetime", p[i].req_datetime);
	bson_append_string(bp, "req_method", p[i].req_method);
	bson_append_string(bp, "req_uri", p[i].req_uri);
	bson_append_string(bp, "req_proto", p[i].req_proto);
	bson_append_int(bp, "resp_code", p[i].resp_code);
	bson_append_int(bp, "resp_bytes", atoi(p[i].resp_bytes));
	bson_append_string(bp, "req_referer", p[i].req_referer);
	bson_append_string(bp, "req_agent", p[i].req_agent);
	bson_finish(bp);
	bps[i] = bp;
    }

    mongo_insert_batch(&conn, globalArgs.collection, (const bson **) bps,
		       counter, NULL, 0);
    bson_destroy(bp);
    free(bp);
    free(bps);
    mongo_destroy(&conn);
    return (0);
}

int flush_to_stdout(st_http_request * p, int counter)
{
    int flush_count;
    for (flush_count = 0; flush_count < counter; flush_count++) {
	printf("%s %d %s\n", p[flush_count].req_uri,
	       p[flush_count].resp_code, p[flush_count].resp_bytes);
    }
    return (0);
}

int chop(void)
{
    printf("outFileName: %s\n", globalArgs.outFileName);
    printf("type: %s\n", globalArgs.type);
    printf("batch_size: %s\n", globalArgs.batch_size);
    printf("host: %s\n", globalArgs.host);
    printf("port: %d\n", globalArgs.port);
    printf("collection: %s\n", globalArgs.collection);
    printf("search_string: %s\n", globalArgs.search_string);
    printf("verbose: %d\n", globalArgs.verbose);
    printf("numInputFiles: %d\n", globalArgs.numInputFiles);

    int running_total = 0;

    int f_count;
    st_http_request *p, *tmp;
    int use_batch_size;
    if (globalArgs.batch_size != '\0') {
	use_batch_size = atoi(globalArgs.batch_size);
    } else {
	use_batch_size = BATCH_SIZE;
    }
    printf("using batch size: %d\n", use_batch_size);
    printf("Size of st_http_request: %lu\n", sizeof(st_http_request));
    printf("Total mem = %lu\n", use_batch_size * sizeof(st_http_request));
    printf("Total mem in MB = %lu\n",
	   use_batch_size * sizeof(st_http_request) / 1024 / 1024);
    tmp =
	(st_http_request *) calloc(use_batch_size,
				   sizeof(st_http_request));
    if (tmp == NULL) {
	printf("Failed to allocate memory.\n");
	free(tmp);
	exit(1);
    } else {
	p = tmp;
	free(tmp);
    }
    p = (st_http_request *) calloc(use_batch_size,
				   sizeof(st_http_request));
    for (f_count = 0; f_count < globalArgs.numInputFiles; f_count++) {
	gzFile pRead = gzopen(globalArgs.inputFiles[f_count], "r");
	char log_line[MAX_LINE_LENGTH];
	int counter = 0;
	int line_length = 0;

	while (gzgets(pRead, log_line, 8192) != NULL) {
	    line_length = strlen(log_line);
	    if (line_length > MAX_LINE_LENGTH - 1) {
		printf
		    ("Encountered a stupidly long line of over 8KB\nLog file must be poop. Exiting.");
		exit(1);
	    } else {
		if ((globalArgs.search_string != NULL)
		    && (strstr(log_line, globalArgs.search_string) ==
			NULL))
		    continue;
		if (strstr(log_line, "\"EOF\""))
		    continue;
		sscanf(log_line,
		       "%s %s %s [%[^]]] \"%s %s %[^\"]\" %d %s \"%[^\"]\" \"%[^\"]\"",
		       p[counter].req_ip, p[counter].req_ident,
		       p[counter].req_user, p[counter].req_datetime,
		       p[counter].req_method, p[counter].req_uri,
		       p[counter].req_proto, &p[counter].resp_code,
		       p[counter].resp_bytes, p[counter].req_referer,
		       p[counter].req_agent);
		running_total++;
		//printf("rt=%d\n",running_total);
	    }
	    if (counter + 1 == (use_batch_size)) {
		if (globalArgs.outFileName != NULL)
		    flush_to_disk(p, counter);
		if (globalArgs.host != NULL
		    && globalArgs.collection != NULL)
		    flush_to_mongo(p, counter);
		if (globalArgs.outFileName == NULL
		    && globalArgs.host == NULL)
		    flush_to_stdout(p, counter);
		counter = 0;
	    } else {
		counter++;
	    }
	}
	if (globalArgs.outFileName != NULL)
	    flush_to_disk(p, counter);
	if (globalArgs.host != NULL && globalArgs.collection != NULL)
	    flush_to_mongo(p, counter);
	if (globalArgs.outFileName == NULL && globalArgs.host == NULL)
	    flush_to_stdout(p, counter);

	printf("Scanned a total of: %d lines.\n", running_total);
	gzclose(pRead);

    }

    free(p);
    return (0);
}

int main(int argc, char *argv[])
{
    int opt = 0;
    int longIndex = 0;
    globalArgs.outFileName = NULL;
    globalArgs.outFile = NULL;
    globalArgs.type = NULL;
    globalArgs.batch_size = NULL;
    globalArgs.host = NULL;
    globalArgs.port = 27017;
    globalArgs.collection = NULL;
    globalArgs.search_string = NULL;
    globalArgs.verbose = 0;
    globalArgs.inputFiles = NULL;
    globalArgs.numInputFiles = 0;

    opt = getopt_long(argc, argv, optString, longOpts, &longIndex);
    while (opt != -1) {
	switch (opt) {
	case 'o':
	    globalArgs.outFileName = optarg;
	    break;
	case 't':
	    globalArgs.type = optarg;
	    break;
	case 'b':
	    globalArgs.batch_size = optarg;
	    break;
	case 'h':
	    globalArgs.host = optarg;
	    break;
	case 'p':
	    globalArgs.port = atoi(optarg);
	    break;
	case 'c':
	    globalArgs.collection = optarg;
	    break;
	case 's':
	    globalArgs.search_string = optarg;
	    break;
	case 'v':
	    globalArgs.verbose = 1;
	    break;
	case '?':
	    display_usage();
	    break;
	case 0:
	    display_usage();
	    break;
	default:
	    display_usage();
	    break;
	}

	opt = getopt_long(argc, argv, optString, longOpts, &longIndex);
    }

    globalArgs.inputFiles = argv + optind;
    globalArgs.numInputFiles = argc - optind;
    if (globalArgs.numInputFiles <= 0)
	display_usage();

    chop();
    exit(0);
}
