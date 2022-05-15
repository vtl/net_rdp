CC = gcc
CFLAGS =-Wall -Wextra -Wpedantic -Werror -std=c99 -Wfatal-errors -g -Ofast

all: net_rdp tcp

net_rdp: net_rdp.o
	$(CC) -o $@ $^ $(CFLAGS) -lrt

tcp: tcp.o
	$(CC) -o $@ $^ $(CFLAGS)

.PHONY: clean

clean:
	rm -f *.o *~ net_rdp tcp
