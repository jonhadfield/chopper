#define  MAX_LINE_LENGTH    8192
#define  MAX_IP               16
#define  MAX_IDENT           513
#define  MAX_USER            513
#define  MAX_DATETIME         27
#define  MAX_METHOD            8
#define  MAX_URI            2038	//Safe limit?
#define  MAX_PROTO             9
#define  MAX_RESP_BYTES       20
#define  MAX_REFERER        2038	//Safe limit?
#define  MAX_AGENT          1024	//Safe limit?

#define  BATCH_SIZE         20000
#define  MAX_BSON_SIZE  16777216	//16MB

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
    char *type;			/* -t option */
    char *fields;		/* -f option */
    char *batch_size;		/* -b option */
    char *host;			/* -h option */
    int port;			/* -p option */
    char *collection;		/* -c option */
    char *search_string;	/* -s option */
    char *outFileNameInvalid;	/* -O option */
    int verbose;		/* verbosity */
    char **inputFiles;		/* input files */
    int numInputFiles;		/* # of input files */
} globalArgs;

void flush_valid(st_http_request *, int);
void flush_invalid(char **, int);
_Bool is_scanned_line_valid(st_http_request);
_Bool is_unscanned_line_valid(char *);
int flush_to_mongo(st_http_request *, int);
int flush_to_stdout(st_http_request *, int);
int chop();
_Bool is_ipv4_address(char *ipAddress);
int isValid(char *log_line);
size_t num_spaces(char *string);
