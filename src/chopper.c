#include <zlib.h>
#include <stdio.h>
#include <stdlib.h>
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
    size_t f_count;
    st_http_request *scanned_lines;
    size_t use_batch_size;
    if (globalArgs.batch_size != '\0') {
	use_batch_size = atoi(globalArgs.batch_size);
    } else {
	use_batch_size = BATCH_SIZE;
    }

    const char *f_combined =
	"%s %s %s [%[^]]] \"%s %s %[^\"]\" %d %s \"%[^\"]\" \"%[^\"]\"";

    scanned_lines = (st_http_request *) calloc(use_batch_size,
					       sizeof(st_http_request));
    if (scanned_lines == NULL) {
	fprintf(stderr, "Failed to allocate memory.\n");
	free(scanned_lines);
	exit(EXIT_FAILURE);
    }

    char **invalid_lines;
    invalid_lines = malloc(use_batch_size * sizeof(char *));

    size_t total_lines_invalid = 0;
    size_t total_lines_read = 0;
    size_t files_processed = 0;
    char log_line[MAX_LINE_LENGTH];

    if (globalArgs.numInputFiles > 0) {
	FILE *input_file;
	for (f_count = 0; f_count < globalArgs.numInputFiles; f_count++) {
	    printf("Processing file [%zu/%d]: %s\n",
		   f_count+1, globalArgs.numInputFiles, globalArgs.inputFiles[f_count]);
	    input_file = fopen(globalArgs.inputFiles[f_count], "r");
	    size_t line_index = 0;
	    size_t invalid_batch_counter = 0;
	    while (fgets(log_line, 8192, input_file) != NULL) {
		total_lines_read++;

        if(!is_unscanned_line_valid(log_line)){
		    total_lines_invalid++;
		    invalid_lines[invalid_batch_counter] =
			malloc(strlen(log_line) + 1 * (sizeof(char)));
		    strcpy(invalid_lines[invalid_batch_counter], log_line);
		    if ((invalid_batch_counter + 1) == use_batch_size) {
			flush_invalid(invalid_lines,
				      invalid_batch_counter + 1);
			size_t reset_counter;
			for (reset_counter = 0;
			     reset_counter < invalid_batch_counter;
			     reset_counter++) {
			    free(invalid_lines[reset_counter]);
			}
			invalid_batch_counter = 0;
		    } else {
			invalid_batch_counter++;
		    }
            continue;
        }

		if ((globalArgs.search_string != NULL)
		    && (strstr(log_line, globalArgs.search_string) ==
			NULL))
		    continue;
		sscanf(log_line,
		       f_combined,
		       scanned_lines[line_index].req_ip,
		       scanned_lines[line_index].req_ident,
		       scanned_lines[line_index].req_user,
		       scanned_lines[line_index].req_datetime,
		       scanned_lines[line_index].req_method,
		       scanned_lines[line_index].req_uri,
		       scanned_lines[line_index].req_proto,
		       &scanned_lines[line_index].resp_code,
		       scanned_lines[line_index].resp_bytes,
		       scanned_lines[line_index].req_referer,
		       scanned_lines[line_index].req_agent);
		if (is_scanned_line_valid(scanned_lines[line_index])) {
		    if ((line_index + 1) == use_batch_size) {
			flush_valid(scanned_lines, line_index + 1);
			line_index = 0;
		    } else {
			line_index++;
		    }
		} else {
		    total_lines_invalid++;
		    invalid_lines[invalid_batch_counter] =
			malloc(strlen(log_line) + 1 * (sizeof(char)));
		    strcpy(invalid_lines[invalid_batch_counter], log_line);
		    if ((invalid_batch_counter + 1) == use_batch_size) {
			flush_invalid(invalid_lines,
				      invalid_batch_counter + 1);
			size_t reset_counter;
			for (reset_counter = 0;
			     reset_counter < invalid_batch_counter;
			     reset_counter++) {
			    free(invalid_lines[reset_counter]);
			}
			invalid_batch_counter = 0;
		    } else {
			invalid_batch_counter++;
		    }

		}
	    }
	    flush_valid(scanned_lines, line_index);
	    flush_invalid(invalid_lines, invalid_batch_counter);
	    size_t reset_counter;
	    for (reset_counter = 0; reset_counter < invalid_batch_counter;
		 reset_counter++) {
		free(invalid_lines[reset_counter]);
	    }
	    fclose(input_file);
	    files_processed++;
	}

    }
    free(scanned_lines);
    free(invalid_lines);
    end = clock();
    cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;
    fprintf(stderr, "\n_____ Summary _____\n\n");
    fprintf(stderr, "Files read:\t%zu\n", files_processed);
    fprintf(stderr, "Lines read:\t%zu\n", total_lines_read);
    fprintf(stderr, "   valid:\t%zu\n",
	    total_lines_read - total_lines_invalid);
    fprintf(stderr, "   invalid:\t%zu\n", total_lines_invalid);
    fprintf(stderr, "Batch size:\t%zu\n", use_batch_size);
    fprintf(stderr, "Search string:\t%s\n", globalArgs.search_string);
    fprintf(stderr, "Output file:\t%s\n", globalArgs.outFileName);
    fprintf(stderr, "Output invalid:\t%s\n",
	    globalArgs.outFileNameInvalid);
    fprintf(stderr, "Host:\t%s\n", globalArgs.host);
    fprintf(stderr, "Port:\t%d\n", globalArgs.port);
    fprintf(stderr, "Collection:\t%s\n", globalArgs.collection);
    fprintf(stderr, "Time taken:\t%5.2fs\n", cpu_time_used);

    exit(EXIT_SUCCESS);
}
