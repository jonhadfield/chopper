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

void flush_invalid(char invalid_lines[][MAX_LINE_LENGTH], int flush_size) {
    printf("\n--- in flush_invalid ---\n");
    //printf("total_lines_invalid = %d\n", total_lines_invalid);
    printf("flush_size = %d\n", flush_size);
    if (globalArgs.outFileNameInvalid)
    {
        FILE *pWriteInvalid;
        pWriteInvalid = fopen(globalArgs.outFileNameInvalid, "a");
        if (pWriteInvalid != NULL){
            //int loop_start = total_lines_invalid - (flush_size);
            int loop_start = 0;
            printf("loop_start = %d\n", loop_start);
            //size_t loop_end = loop_start + flush_size;
            //int loop_end = total_lines_invalid;
            int loop_end = flush_size;
            printf("loop_end = %d\n", loop_end);
            int index;
            for (index = loop_start; index < flush_size; index++){
                //printf("index: %d\n", index);
                printf("at index %d writing line... %s\n", index, invalid_lines[index]);
                fputs(invalid_lines[index], pWriteInvalid);
            }
        }
        fclose(pWriteInvalid);
        printf("closed file for writing\n------------\n");
    }
}
void call_flush(st_http_request * p, int countval) {
	if (globalArgs.outFileName != NULL)
	    flush_to_disk(p, countval);
	if (globalArgs.host != NULL && globalArgs.collection != NULL)
	    flush_to_mongo(p, countval);
	if (globalArgs.outFileName == NULL && globalArgs.host == NULL)
	    flush_to_stdout(p, countval);
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
    //int use_batch_size;
    //if (globalArgs.batch_size != '\0') {
	//use_batch_size = atoi(globalArgs.batch_size);
    //} else {
	//use_batch_size = BATCH_SIZE;
    //}
    //fprintf(stderr, "\n_____ Summary _____\n\n");
    //fprintf(stderr, "using batch size: %d\n", use_batch_size);
    //fprintf(stderr, "Size of st_http_request: %lu\n",
	//    sizeof(st_http_request));
    //fprintf(stderr, "Total mem = %lu\n",
	//    use_batch_size * sizeof(st_http_request));
    //fprintf(stderr, "Total mem in MB = %lu\n",
	//    use_batch_size * sizeof(st_http_request) / 1024 / 1024);
    
    const char *f_combined =
	"%s %s %s [%[^]]] \"%s %s %[^\"]\" %d %s \"%[^\"]\" \"%[^\"]\"";
    int use_batch_size = BATCH_SIZE;

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

    int use_invalid_batch_size = 5;
    int invalid_batch_counter = 0;
    int total_lines_invalid = 0;
    char pinvalid_lines[use_invalid_batch_size][MAX_LINE_LENGTH];
    int total_lines_scanned = 0, files_processed = 0;
    char log_line[MAX_LINE_LENGTH];

    if (globalArgs.numInputFiles > 0) {
	FILE *pRead;
	for (f_count = 0; f_count < globalArgs.numInputFiles; f_count++) {
	    pRead = fopen(globalArgs.inputFiles[f_count], "r");
	    int counter = 0;
	    while (fgets(log_line, 8192, pRead) != NULL) {
		    total_lines_scanned++;
		    if ((globalArgs.search_string != NULL) && (strstr(log_line, globalArgs.search_string) == NULL)) continue;
		    sscanf(log_line,
		       f_combined,
		       p[counter].req_ip, p[counter].req_ident,
		       p[counter].req_user, p[counter].req_datetime,
		       p[counter].req_method, p[counter].req_uri,
		       p[counter].req_proto, &p[counter].resp_code,
		       p[counter].resp_bytes, p[counter].req_referer,
		       p[counter].req_agent);

            //START
            _Bool valid = 1;
            if (is_ipv4_address(p[counter].req_ip) == 0){ valid = 0; }
            if (num_spaces(p[counter].req_ident) > 0){ valid = 0; }
            if (num_spaces(p[counter].req_user) > 0){ valid = 0; }
            if (num_spaces(p[counter].req_datetime) != 1){ valid = 0; }
            if (num_spaces(p[counter].req_method) > 0){ valid = 0; }
            if (num_spaces(p[counter].req_uri) > 0){ valid = 0; }
            if (num_spaces(p[counter].req_proto) > 0){ valid = 0; }

            if(valid == 1){ //valid
		      if ((counter + 1) == use_batch_size) {
		        call_flush(p, counter + 1);
		        counter = 0;
              }else{
		        counter++;
              }
            }else{ //invalid
              total_lines_invalid++;
              invalid_batch_counter++;
              printf("invalid batch counter = %d\n", invalid_batch_counter);
              strncpy(pinvalid_lines[invalid_batch_counter-1], log_line, strlen(log_line));
              printf("---\nAT INVALID LINES BATCH COUNT: %d JUST ADDED: %s\n", invalid_batch_counter, pinvalid_lines[invalid_batch_counter-1]);
              if(invalid_batch_counter % use_invalid_batch_size == 0) {
                printf("reached it --- invalid batch size = %d\n", use_invalid_batch_size);
                printf("invalid batch counter = %d\n", invalid_batch_counter);
                printf("total invalid lines count = %d\n", total_lines_invalid);
                flush_invalid(pinvalid_lines, invalid_batch_counter-1);
                invalid_batch_counter = 0;
              }
              printf("invalid_batch_counter after processing invalid line: %d\n", invalid_batch_counter);
            }

           //END
	    }
        // call if counter > 0?
        printf("outputting final invalid with %d\n", invalid_batch_counter);
        if(invalid_batch_counter > 0) { flush_invalid(pinvalid_lines, invalid_batch_counter-1);}
	    call_flush(p, counter);
	    fclose(pRead);
	    files_processed++;
	}

    }
    printf("calling final process invalid.\n");
    //process_invalid(invalidLines, NULL,1);
    //flush remaining invalid
    free(p);
    //free(invalidLines);
    //invalidLines = 0;
    end = clock();
    cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;
    fprintf(stderr, "\n_____ Summary _____\n\n");
    fprintf(stderr, "Files read:\t%d\n", files_processed);
    fprintf(stderr, "Lines read:\t%d\n", total_lines_scanned);
    fprintf(stderr, "   valid:\t%d\n",
	    total_lines_scanned - total_lines_invalid);
    fprintf(stderr, "   invalid:\t%d\n", total_lines_invalid);
    fprintf(stderr, "Batch size:\t%d\n", use_batch_size);
    fprintf(stderr, "Search string:\t%s\n", globalArgs.search_string);
    fprintf(stderr, "Output file:\t%s\n", globalArgs.outFileName);
    fprintf(stderr, "Host:\t%s\n", globalArgs.host);
    fprintf(stderr, "Port:\t%d\n", globalArgs.port);
    fprintf(stderr, "Collection:\t%s\n", globalArgs.collection);
    fprintf(stderr, "Time taken:\t%5.2fs\n", cpu_time_used);

    //printf("total_lines_invalid: %d\n", total_lines_invalid);
    //int index_inv = 0;
    //for (index_inv = 0; index_inv < total_lines_invalid; index_inv++)
    //{
    //    printf("invalid line: %s\n", invalidLines[index_inv]);
    //}
    exit(0);
}
