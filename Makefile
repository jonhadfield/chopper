PROG_NAME=chopper
OBJECTS=flush.o chopper.o
CFLAGS=-static-libgcc -O2 -Wall --std=c99
LDFLAGS=
LDLIBS=-Lext/mongo/lib -Iext/mongo/include -lz -lmongoc
CC=gcc

$(PROG_NAME) : $(OBJECTS)
	$(CC) $(OBJECTS) -o $(PROG_NAME) $(LDLIBS)

chopper.o : src/chopper.c src/chopper.h 
	$(CC) $(CFLAGS) $(LDLIBS) -c ./src/chopper.c 

flush.o : src/flush.c src/chopper.h
	$(CC) $(CFLAGS) $(LDLIBS) -c ./src/flush.c 

clean:
	@- $(RM) $(PROG_NAME)
	@- $(RM) $(OBJECTS)
