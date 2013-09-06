#include <zlib.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <getopt.h>
#include <string.h>
#include <arpa/inet.h>
#include "mongo.h"
#include "chopper.h"

/* short options:
 * *
 * * -o outfile name - write to file rather than stdout
 * * -t output type
 * * -b batch size - number of lines to process before flushing - debug only
 * * -h db host
 * * -p port of db server
 * * -c mongo db and collection
 * * -s search string
 * * -O outfile name for invalid - write lines that cannot be processed to a file
 * * -v verbose output
 * * additional file names are used as input files
 * *
 * *
 * * long options:
 * *
 * * --outfile
 * * --type
 * * --batchsize
 * * --host
 * * --port
 * * --collection
 * * --search_string
 * * --outfile-invalid
 * * --verbose
 * *
 * */

const char chopper_usage_string[] =
    "chopper [-s|--search_string] [-o|--outfile <path>] [-t|--type] [-b|--batchsize <value>] [-h|--host <value>]\n"
    "           [-p|--port <value>] [-c|--collection <db.collection>] [-s|--search_string <value>] [-O|--outfile-invalid] [-v|--verbose]\n"
    "           [-v|--verbose] [-h|--help]\n"
    "           <command> [<args>]";

static const char *optString = "o:t:b:h:p:c:s:O:v?";

static const struct option longOpts[] = {
    {"outFileName", required_argument, NULL, 'o'},
    {"type", required_argument, NULL, 't'},
    {"batch_size", required_argument, NULL, 'b'},
    {"host", required_argument, NULL, 'h'},
    {"port", required_argument, NULL, 'p'},
    {"collection", required_argument, NULL, 'c'},
    {"search_string", required_argument, NULL, 's'},
    {"outFileNameInvalid", required_argument, NULL, 'O'},
    {"verbose", no_argument, NULL, 'v'},
    {"help", no_argument, NULL, 'h'},
    {NULL, no_argument, NULL, 0}
};

void display_usage(void)
{
    puts(chopper_usage_string);
    exit(EXIT_FAILURE);
}

_Bool is_ipv4_address(char *ipAddress)
{
    struct sockaddr_in sa;
    int result = inet_pton(AF_INET, ipAddress, &(sa.sin_addr));
    return result != 0;
}

size_t num_spaces(char *string)
{
    size_t s_length = strlen(string);
    size_t i = 0;
    size_t space_count = 0;
    while( i < s_length)  {
        if(string[i++] == ' ')
        space_count++; 
    }
    return space_count;
}

_Bool is_utf8(const char * string)
{
    if(!string)
        return 0;

    const unsigned char * bytes = (const unsigned char *)string;
    while(*bytes)
    {
        if( (// ASCII
             // use bytes[0] <= 0x7F to allow ASCII control characters
                bytes[0] == 0x09 ||
                bytes[0] == 0x0A ||
                bytes[0] == 0x0D ||
                (0x20 <= bytes[0] && bytes[0] <= 0x7E)
            )
        ) {
            bytes += 1;
            continue;
        }

        if( (// non-overlong 2-byte
                (0xC2 <= bytes[0] && bytes[0] <= 0xDF) &&
                (0x80 <= bytes[1] && bytes[1] <= 0xBF)
            )
        ) {
            bytes += 2;
            continue;
        }

        if( (// excluding overlongs
                bytes[0] == 0xE0 &&
                (0xA0 <= bytes[1] && bytes[1] <= 0xBF) &&
                (0x80 <= bytes[2] && bytes[2] <= 0xBF)
            ) ||
            (// straight 3-byte
                ((0xE1 <= bytes[0] && bytes[0] <= 0xEC) ||
                    bytes[0] == 0xEE ||
                    bytes[0] == 0xEF) &&
                (0x80 <= bytes[1] && bytes[1] <= 0xBF) &&
                (0x80 <= bytes[2] && bytes[2] <= 0xBF)
            ) ||
            (// excluding surrogates
                bytes[0] == 0xED &&
                (0x80 <= bytes[1] && bytes[1] <= 0x9F) &&
                (0x80 <= bytes[2] && bytes[2] <= 0xBF)
            )
        ) {
            bytes += 3;
            continue;
        }

        if( (// planes 1-3
                bytes[0] == 0xF0 &&
                (0x90 <= bytes[1] && bytes[1] <= 0xBF) &&
                (0x80 <= bytes[2] && bytes[2] <= 0xBF) &&
                (0x80 <= bytes[3] && bytes[3] <= 0xBF)
            ) ||
            (// planes 4-15
                (0xF1 <= bytes[0] && bytes[0] <= 0xF3) &&
                (0x80 <= bytes[1] && bytes[1] <= 0xBF) &&
                (0x80 <= bytes[2] && bytes[2] <= 0xBF) &&
                (0x80 <= bytes[3] && bytes[3] <= 0xBF)
            ) ||
            (// plane 16
                bytes[0] == 0xF4 &&
                (0x80 <= bytes[1] && bytes[1] <= 0x8F) &&
                (0x80 <= bytes[2] && bytes[2] <= 0xBF) &&
                (0x80 <= bytes[3] && bytes[3] <= 0xBF)
            )
        ) {
            bytes += 4;
            continue;
        }

        return 0;
    }
    return 1;
}

int main(int argc, char *argv[])
{
    int opt = 0;
    int longIndex = 0;
    globalArgs.outFileName = NULL;
    globalArgs.outFileNameInvalid = NULL;
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
	case 'O':
	    globalArgs.outFileNameInvalid = optarg;
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
    clock_t start, end;
    double cpu_time_used;
    start = clock();
    int f_count;
    st_http_request *p, *tmp;
    int use_batch_size;
    if (globalArgs.batch_size != '\0') {
	use_batch_size = atoi(globalArgs.batch_size);
    } else {
	use_batch_size = BATCH_SIZE;
    }
    //fprintf(stderr, "\n_____ Summary _____\n\n");
    //fprintf(stderr, "using batch size: %d\n", use_batch_size);
    //fprintf(stderr, "Size of st_http_request: %lu\n",
	//    sizeof(st_http_request));
    //fprintf(stderr, "Total mem = %lu\n",
	//    use_batch_size * sizeof(st_http_request));
    //fprintf(stderr, "Total mem in MB = %lu\n",
	//    use_batch_size * sizeof(st_http_request) / 1024 / 1024);
    tmp =
	(st_http_request *) calloc(use_batch_size,
				   sizeof(st_http_request));
    if (tmp == NULL) {
	fprintf(stderr, "Failed to allocate memory.\n");
	free(tmp);
	exit(1);
    } else {
	p = tmp;
	free(tmp);
    }
    p = (st_http_request *) calloc(use_batch_size,
				   sizeof(st_http_request));

    char **invalidLines;
    invalidLines = malloc((use_batch_size/100) * MAX_LINE_LENGTH); 
    size_t invalidLinesCount = 0;

    int total_lines_scanned = 0, files_processed = 0;
    const char *f_combined =
	"%s %s %s [%[^]]] \"%s %s %[^\"]\" %d %s \"%[^\"]\" \"%[^\"]\"";
    char log_line[MAX_LINE_LENGTH];

    int isValid(char *log_line) {
	if (is_utf8(log_line) != 1 || log_line == NULL || strlen(log_line) <= 1
	    || log_line[0] == '\0'
	    || strlen(log_line) > MAX_LINE_LENGTH - 1) {
        printf("This line is invalid: %s\n", log_line);
	    return (1);
	} else {
	    return (0);
	}
    }

    void process_invalid(char *log_line) {
        invalidLines[invalidLinesCount] = log_line;
        printf("%zu. %s\n",invalidLinesCount, invalidLines[invalidLinesCount]);
        invalidLinesCount++;
    }

    void call_flush(st_http_request * p, int countval) {
	if (globalArgs.outFileName != NULL)
	    flush_to_disk(p, countval);
	if (globalArgs.host != NULL && globalArgs.collection != NULL)
	    flush_to_mongo(p, countval);
	if (globalArgs.outFileName == NULL && globalArgs.host == NULL)
	    flush_to_stdout(p, countval);
    }

    if (globalArgs.numInputFiles > 0) {
	FILE *pRead;
	for (f_count = 0; f_count < globalArgs.numInputFiles; f_count++) {
	    pRead = fopen(globalArgs.inputFiles[f_count], "r");
	    int counter = 0;
	    while (fgets(log_line, 8192, pRead) != NULL) {
		total_lines_scanned++;
		if (isValid(log_line)) {
            process_invalid(log_line);
		    continue;
		} else {
		    if ((globalArgs.search_string != NULL)
			&& (strstr(log_line, globalArgs.search_string) ==
			    NULL))
			continue;
		    sscanf(log_line,
			   f_combined,
			   p[counter].req_ip, p[counter].req_ident,
			   p[counter].req_user, p[counter].req_datetime,
			   p[counter].req_method, p[counter].req_uri,
			   p[counter].req_proto, &p[counter].resp_code,
			   p[counter].resp_bytes, p[counter].req_referer,
			   p[counter].req_agent);
		}
		if (counter + 1 == (use_batch_size)) {
		    call_flush(p, counter + 1);
		    counter = 0;
		} else {
            
            _Bool invalid = 0;
            if (is_ipv4_address(p[counter].req_ip) == 0){
                invalid = 1;
            }
            if (num_spaces(p[counter].req_ident) > 0){
                invalid = 1;
            }
            if (num_spaces(p[counter].req_user) > 0){
                invalid = 1;
            }
            if (num_spaces(p[counter].req_datetime) != 1){
                invalid = 1;
            }
            if (num_spaces(p[counter].req_method) > 0){
                invalid = 1;
            }
            if (num_spaces(p[counter].req_uri) > 0){
                invalid = 1;
            }
            if (num_spaces(p[counter].req_proto) > 0){
                invalid = 1;
            }

            if(invalid){
              process_invalid(log_line);
            }else{
		      counter++;
            }
		}
	    }
        // call if counter > 0?
	    call_flush(p, counter);
	    fclose(pRead);
	    files_processed++;
	}
    } else {
	int counter = 0;
	while (fgets(log_line, 8192, stdin) != NULL) {
	    total_lines_scanned++;
	    if (isValid(log_line)) {
        process_invalid(log_line);
		continue;
	    } else {
		if ((globalArgs.search_string != NULL)
		    && (strstr(log_line, globalArgs.search_string) ==
			NULL))
		    continue;
		sscanf(log_line,
		       f_combined,
		       p[counter].req_ip, p[counter].req_ident,
		       p[counter].req_user, p[counter].req_datetime,
		       p[counter].req_method, p[counter].req_uri,
		       p[counter].req_proto, &p[counter].resp_code,
		       p[counter].resp_bytes, p[counter].req_referer,
		       p[counter].req_agent);
	    }
	    if (counter + 1 == (use_batch_size)) {
		call_flush(p, counter + 1);
		counter = 0;
	    } else {
		counter++;
	    }
	}
	call_flush(p, counter);
    }
    free(p);
    end = clock();
    cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;
    fprintf(stderr, "\n_____ Summary _____\n\n");
    fprintf(stderr, "Files read:\t%d\n", files_processed);
    fprintf(stderr, "Lines read:\t%d\n", total_lines_scanned);
    fprintf(stderr, "   valid:\t%zu\n",
	    total_lines_scanned - invalidLinesCount);
    fprintf(stderr, "   invalid:\t%zu\n", invalidLinesCount);
    fprintf(stderr, "Batch size:\t%d\n", use_batch_size);
    fprintf(stderr, "Search string:\t%s\n", globalArgs.search_string);
    fprintf(stderr, "Output file:\t%s\n", globalArgs.outFileName);
    fprintf(stderr, "Host:\t%s\n", globalArgs.host);
    fprintf(stderr, "Port:\t%d\n", globalArgs.port);
    fprintf(stderr, "Collection:\t%s\n", globalArgs.collection);
    fprintf(stderr, "Time taken:\t%5.2fs\n", cpu_time_used);

    //printf("invalidLinesCount: %zu\n", invalidLinesCount);
    //int index_inv = 0;
    //for (index_inv = 0; index_inv < invalidLinesCount; index_inv++)
    //{
    //    printf("invalid line: %s\n", invalidLines[index_inv]);
    //}
    exit(0);
}
