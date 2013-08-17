#include <zlib.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <getopt.h>
#include <string.h>
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
 * * --verbose
 * *
 * */

const char chopper_usage_string[] =
  "chopper [-s|--search_string] [-o|--outfile <path>] [-t|--type] [-b|--batchsize <value>] [-h|--host <value>]\n"
  "           [-p|--port <value>] [-c|--collection <db.collection>][-s|--search_string <value>] [-v|--verbose]\n"
  "           [-v|--verbose] [-h|--help]\n" "           <command> [<args>]";

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

void
display_usage (void)
{
  puts (chopper_usage_string);
  exit (EXIT_FAILURE);
}

int
main (int argc, char *argv[])
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

  opt = getopt_long (argc, argv, optString, longOpts, &longIndex);
  while (opt != -1)
    {
      switch (opt)
	{
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
	  globalArgs.port = atoi (optarg);
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
	  display_usage ();
	  break;
	case 0:
	  display_usage ();
	  break;
	default:
	  display_usage ();
	  break;
	}

      opt = getopt_long (argc, argv, optString, longOpts, &longIndex);
    }

  globalArgs.inputFiles = argv + optind;
  globalArgs.numInputFiles = argc - optind;
  clock_t start, end;
  double cpu_time_used;
  start = clock ();
  int f_count;
  st_http_request *p, *tmp;
  int use_batch_size;
  if (globalArgs.batch_size != '\0')
    {
      use_batch_size = atoi (globalArgs.batch_size);
    }
  else
    {
      use_batch_size = BATCH_SIZE;
    }
  fprintf (stderr, "\n_____ Summary _____\n\n");
  fprintf (stderr, "using batch size: %d\n", use_batch_size);
  fprintf (stderr, "Size of st_http_request: %lu\n", sizeof (st_http_request));
  fprintf (stderr, "Total mem = %lu\n", use_batch_size * sizeof (st_http_request));
  fprintf (stderr, "Total mem in MB = %lu\n",
	  use_batch_size * sizeof (st_http_request) / 1024 / 1024);
  tmp = (st_http_request *) calloc (use_batch_size, sizeof (st_http_request));
  if (tmp == NULL)
    {
      fprintf (stderr, "Failed to allocate memory.\n");
      free (tmp);
      exit (1);
    }
  else
    {
      p = tmp;
      free (tmp);
    }
  p = (st_http_request *) calloc (use_batch_size, sizeof (st_http_request));
  int total_lines_scanned = 0, invalid_lines = 0, files_processed = 0;
  const char *f_combined =
	"%s %s %s [%[^]]] \"%s %s %[^\"]\" %d %s \"%[^\"]\" \"%[^\"]\"";
  char log_line[MAX_LINE_LENGTH];

  if (globalArgs.numInputFiles > 0)
    {
      FILE *pRead;
      for (f_count = 0; f_count < globalArgs.numInputFiles; f_count++)
	{
	  pRead = fopen (globalArgs.inputFiles[f_count], "r");
	  int counter = 0;
	  while (fgets (log_line, 8192, pRead) != NULL)
	    {
	      total_lines_scanned++;
	      if (strlen (log_line) > MAX_LINE_LENGTH - 1)
		{
		  invalid_lines++;
		  continue;
		}
	      else
		{
		  if ((globalArgs.search_string != NULL)
		      && (strstr (log_line, globalArgs.search_string) ==
			  NULL))
		    continue;
		  if (strstr (log_line, "\"EOF\""))
		    {
		      invalid_lines++;
		      continue;
		    }
		  sscanf (log_line,
			  f_combined,
			  p[counter].req_ip, p[counter].req_ident,
			  p[counter].req_user, p[counter].req_datetime,
			  p[counter].req_method, p[counter].req_uri,
			  p[counter].req_proto, &p[counter].resp_code,
			  p[counter].resp_bytes, p[counter].req_referer,
			  p[counter].req_agent);
		}
	      if (counter + 1 == (use_batch_size))
		{
		  if (globalArgs.outFileName != NULL)
		    flush_to_disk (p, counter + 1);
		  if (globalArgs.host != NULL
		      && globalArgs.collection != NULL)
		    flush_to_mongo (p, counter + 1);
		  if (globalArgs.outFileName == NULL
		      && globalArgs.host == NULL)
		    flush_to_stdout (p, counter + 1);
		  counter = 0;
		}
	      else
		{
		  counter++;
		}
	    }

	  if (globalArgs.outFileName != NULL)
	    flush_to_disk (p, counter);
	  if (globalArgs.host != NULL && globalArgs.collection != NULL)
	    flush_to_mongo (p, counter);
	  if (globalArgs.outFileName == NULL && globalArgs.host == NULL)
	    flush_to_stdout (p, counter);
	  fclose (pRead);
	  files_processed++;
	}
    }
  else
    {
      int counter = 0;
      while (fgets (log_line, 8192, stdin) != NULL)
	{
	  total_lines_scanned++;
	  if (strlen (log_line) > MAX_LINE_LENGTH - 1)
	    {
	      invalid_lines++;
	      continue;
	    }
	  else
	    {
	      if ((globalArgs.search_string != NULL)
		  && (strstr (log_line, globalArgs.search_string) == NULL))
		continue;
	      if (strstr (log_line, "\"EOF\""))
		{
		  invalid_lines++;
		  continue;
		}
	      sscanf (log_line,
		      f_combined,
		      p[counter].req_ip, p[counter].req_ident,
		      p[counter].req_user, p[counter].req_datetime,
		      p[counter].req_method, p[counter].req_uri,
		      p[counter].req_proto, &p[counter].resp_code,
		      p[counter].resp_bytes, p[counter].req_referer,
		      p[counter].req_agent);
	    }
	  if (counter + 1 == (use_batch_size))
	    {
	      if (globalArgs.outFileName != NULL)
		flush_to_disk (p, counter + 1);
	      if (globalArgs.host != NULL && globalArgs.collection != NULL)
		flush_to_mongo (p, counter + 1);
	      if (globalArgs.outFileName == NULL && globalArgs.host == NULL)
		flush_to_stdout (p, counter + 1);
	      counter = 0;
	    }
	  else
	    {
	      counter++;
	    }
	}

      if (globalArgs.outFileName != NULL)
	flush_to_disk (p, counter);
      if (globalArgs.host != NULL && globalArgs.collection != NULL)
	flush_to_mongo (p, counter);
      if (globalArgs.outFileName == NULL && globalArgs.host == NULL)
	flush_to_stdout (p, counter);
    }
  free (p);
  end = clock ();
  cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;
  fprintf (stderr, "\n_____ Summary _____\n\n");
  fprintf (stderr, "Files read:\t%d\n", files_processed);
  fprintf (stderr, "Lines read:\t%d\n", total_lines_scanned);
  fprintf (stderr, "   valid:\t%d\n", total_lines_scanned - invalid_lines);
  fprintf (stderr, "   invalid:\t%d\n", invalid_lines);
  fprintf (stderr, "Batch size:\t%d\n", use_batch_size);
  fprintf (stderr, "Search string:\t%s\n", globalArgs.search_string);
  fprintf (stderr, "Output file:\t%s\n", globalArgs.outFileName);
  fprintf (stderr, "Host:\t%s\n", globalArgs.host);
  fprintf (stderr, "Port:\t%d\n", globalArgs.port);
  fprintf (stderr, "Collection:\t%s\n", globalArgs.collection);
  fprintf (stderr, "Time taken:\t%5.2fs\n", cpu_time_used);
  exit (0);
}
