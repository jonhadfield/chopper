Process common and combined formatted logs
* Output to stdout or mongodb
* Select fields to output
* Filter invalid entries
* Basic search

__Usage__

    chopper [-s|--search_string] [-o|--outfile <path>] [-t|--type] [-f|--fields] [-b|--batchsize <value>] 
            [-h|--host <value>] [-p|--port <value>] [-c|--collection <db.collection>] 
            [-s|--search_string <value>] [-O|--outfile-invalid] [-v|--verbose][-h|--help]
            <command> [<args>]
