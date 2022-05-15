#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/time.h>
#include <sys/types.h>          /* See NOTES */
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <time.h>
#include <netinet/tcp.h>
#include <poll.h>

int ring_size;
int pkt_size;
int ack_batch_size;

#define VERBOSE 0

/* rdtsc */
extern __inline unsigned long long
__attribute__((__gnu_inline__, __always_inline__, __artificial__))
__rdtsc (void)
{
	return __builtin_ia32_rdtsc ();
}

uint64_t time_us()
{
	struct timeval tv;
	uint64_t us = -1;
	int ret;

	ret = gettimeofday(&tv, NULL);
	if (ret < 0) {
		printf("%s: gettimeofday %d (%s)\n", __func__, errno, strerror(-errno));
		goto out;
	}
	us = tv.tv_sec * 1e6 + tv.tv_usec;
out:
	return us;
}

char buf[32768];

int main(int argc, char *argv[])
{
	struct sockaddr_in saddr;
	int fd, fd2;
	int ret;
	int i;
	in_addr_t addr;
	int client;
	uint64_t _start, start, end, n, diff, pkts;

	if (argc < 6) {
		printf("%s <ip addr> <0 - server, 1 - client> <ring size> <pkt size> <ack batch size>\n", argv[0]);
		return 1;
	}
	addr = inet_addr(argv[1]);
	client = strtoul(argv[2], NULL, 10);
	ring_size = strtoul(argv[3], NULL, 10);
	pkt_size = strtoul(argv[4], NULL, 10);
	ack_batch_size = strtoul(argv[5], NULL, 10);

	printf("I'm %s, remote addr %x %s\n", client ? "client" : "server", addr, argv[1]);
	printf("ring size %d, pkt size %d, ack batch size %d\n", ring_size, pkt_size, ack_batch_size);

	fd = socket(AF_INET, SOCK_STREAM, 0);//IPPROTO_TCP);
	if (fd < 0) {
		printf("socket %d (%s)\n", errno, strerror(errno));
		goto out;
	}
	saddr.sin_family = AF_INET;
	saddr.sin_port = htons(12345);

	if (!client)
		goto server;

	saddr.sin_addr.s_addr = addr;

	ret = connect(fd, (struct sockaddr *)&saddr, sizeof(saddr));
	if (ret < 0) {
		printf("connect %d (%s)\n", errno, strerror(errno));
		goto out;
	}

	if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &(int){1}, sizeof(int)) < 0) {
		printf("setsockopt TCP_NODELAY %d (%s)\n", errno, strerror(errno));
		goto out;
	}

	n = 0;
	start = _start = time_us();
	for (i = 1; ; i++) {
	again:
/*
		struct pollfd fds = { .fd = fd, .events = POLLOUT | POLLHUP | POLLERR};
	again:
		ret = poll(&fds, 1, 1000);
		if (ret < 0) {
			printf("poll %d %s\n", errno, strerror(errno));
			goto out;
		}
		if (ret == 0)
			goto again;
		if (fds.revents != POLLOUT) {
			printf("poll revents %x\n", fds.revents);
		}
*/
		ret = send(fd, buf, pkt_size, MSG_DONTWAIT);
		end = time_us();

		if ((end - start) > 1e6) {
			printf("%3.2f pkt/s\n", n * 1e6 / (end - start));
			start = end;
			n = 0;
		}
		if (ret < 0) {
			if (errno == EAGAIN) {
				goto again;
			}
			printf("[%i] send ret=%d errno=%d (%s)\n", i, ret, errno, strerror(errno));
			goto out;
		}
		if ((i % ack_batch_size) == 0) {
//			do {
				ret = recv(fd, buf, 16, 0);
//			} while (ret > 0);
		}
		n++;

		/* run for 10 seconds max */
		if (end - _start > 10e6)
			break;
	}

	i--;
	diff = time_us() - _start;
	pkts = 1e6 * i / diff;
	printf("done %d pkts, in %3.2f s, %ld pkt/s, %3.2f MB/s\n", i, diff / 1e6, pkts, 1.0 * pkts * pkt_size / (1UL << 20));
	ret = 0;
	printf("RET=%lu\n", pkts);
	goto out;

server:
	saddr.sin_addr.s_addr = INADDR_ANY;
	if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &(int){1}, sizeof(int)) < 0) {
		printf("setsockopt SO_REUSEADDR %d (%s)\n", errno, strerror(errno));
		goto out;
	}

	ret = bind(fd, (struct sockaddr *)&saddr , sizeof(saddr));
	if (ret < 0) {
		printf("bind %d (%s)\n", errno, strerror(errno));
		goto out;
	}
	ret = listen(fd, 1);
	if (ret < 0) {
		printf("listen %d (%s)\n", errno, strerror(errno));
		goto out;
	}
restart:
	printf("listen\n");
	fd2 = accept(fd, NULL, NULL);
	if (fd2 < 0) {
		printf("accept %d (%s)\n", errno, strerror(errno));
		goto out;
	}
	printf("accepted\n");

	for (i = 1;; i++) {
	again1:
		ret = recv(fd2, buf, pkt_size, MSG_DONTWAIT);
		if (ret < 0) {
			if (errno != EAGAIN) {
				printf("[%i] recv ret=%d errno=%d (%s)\n", i, ret, errno, strerror(errno));
				close(fd2);
				goto restart;
			}
			goto again1;
		}

		if (ret == 0) {
			close(fd2);
			goto out;
		}

		if ((i % ack_batch_size) == 0) /* simulate ACKs every X pkts */
			ret = send(fd2, buf, 16, 0);
	}

out:
	close(fd);
	ret = 0;
	return ret;
}
