#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <getopt.h>
#include <string.h>
#include "mongo.h"
#include "chopper.h"

int chop(void)
{
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
    int total_lines_scanned = 0, invalid_lines = 0, files_processed = 0;
    FILE *pRead;
    for (f_count = 0; f_count < globalArgs.numInputFiles; f_count++) {
	pRead = fopen(globalArgs.inputFiles[f_count], "r");
	char log_line[MAX_LINE_LENGTH];
	int counter = 0;
	while (fgets(log_line, 8192, pRead) != NULL) {
	    total_lines_scanned++;
	    if (strlen(log_line) > MAX_LINE_LENGTH - 1) {
		invalid_lines++;
		continue;
	    } else {
		if ((globalArgs.search_string != NULL)
		    && (strstr(log_line, globalArgs.search_string) ==
			NULL))
		    continue;
		if (strstr(log_line, "\"EOF\"")) {
		    invalid_lines++;
		    continue;
		}
		sscanf(log_line,
		       "%s %s %s [%[^]]] \"%s %s %[^\"]\" %d %s \"%[^\"]\" \"%[^\"]\"",
		       p[counter].req_ip, p[counter].req_ident,
		       p[counter].req_user, p[counter].req_datetime,
		       p[counter].req_method, p[counter].req_uri,
		       p[counter].req_proto, &p[counter].resp_code,
		       p[counter].resp_bytes, p[counter].req_referer,
		       p[counter].req_agent);
	    }
	    if (counter + 1 == (use_batch_size)) {
		if (globalArgs.outFileName != NULL)
		    flush_to_disk(p, counter + 1);
		if (globalArgs.host != NULL
		    && globalArgs.collection != NULL)
		    flush_to_mongo(p, counter + 1);
		if (globalArgs.outFileName == NULL
		    && globalArgs.host == NULL)
		    flush_to_stdout(p, counter + 1);
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
	fclose(pRead);
	files_processed++;
    }
    free(p);
    end = clock();
    cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;
    printf("\n_____ Summary _____\n\n");
    printf("Files read:\t%d\n", files_processed);
    printf("Lines read:\t%d\n", total_lines_scanned);
    printf("   valid:\t%d\n", total_lines_scanned - invalid_lines);
    printf("   invalid:\t%d\n", invalid_lines);
    printf("Batch size:\t%d\n", use_batch_size);
    printf("Search string:\t%s\n", globalArgs.search_string);
    printf("Output file:\t%s\n", globalArgs.outFileName);
    printf("Host:\t%s\n", globalArgs.host);
    printf("Port:\t%d\n", globalArgs.port);
    printf("Collection:\t%s\n", globalArgs.collection);
    printf("Time taken:\t%5.2fs\n", cpu_time_used);
    return (0);
}
