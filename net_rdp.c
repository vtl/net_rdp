#define _POSIX_C_SOURCE 199309L
#define _XOPEN_SOURCE 500
#include <arpa/inet.h>
#include <assert.h>
#include <errno.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#define VERBOSE 0

#define ARRAY_SIZE(arr) (sizeof(arr) / sizeof((arr)[0]))

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
		printf("%s: gettimeofday %d (%s)\n", __func__, errno, strerror(errno));
		goto out;
	}
	us = tv.tv_sec * 1e6 + tv.tv_usec;
out:
	return us;
}

static int __verbose = VERBOSE;

#ifdef VERBOSE
static int __dprintf_n = 0;
#define dprintf(N, ...) do {					\
		int __i;						\
		if (__verbose <= 0) break;					\
		if ((N) < 0) __dprintf_n += (N);			\
		for (__i = 0; (N) != 100 && __dprintf_n >= 0 && __i < __dprintf_n; __i++) \
			printf(" ");					\
		printf(__VA_ARGS__);					\
		if ((N) > 0 && (N) != 100) __dprintf_n += (N);		\
	} while (0)

#else
#define dprintf(...) {}
#endif

#define NET_RDP_STRIPE_HEAD(state) ((state)->head / (state)->stripe_size)
#define NET_RDP_STRIPE_TAIL(state) ((state)->tail / (state)->stripe_size)
#define NET_RDP_STRIPE_THIS(state, id) ((id) / (state)->stripe_size)

struct net_rdp_stripe {
	uint16_t gen_id;
	uint16_t idx;
	uint64_t bits;
};

struct net_rdp_hdr {
	uint16_t gen_id;
	uint16_t id;
	uint8_t type;
	uint8_t stripe_cnt;
	uint16_t recv_tail;
};

#define BUF_IOV 64

struct net_rdp_buffer {
	uint64_t submitted;
	uint64_t first_submitted;
	struct sockaddr_in peer;
	struct net_rdp_hdr hdr;
	int len;
	struct msghdr msg;
	struct net_rdp_stripe stripes[BUF_IOV]; /* FIXME */
	struct iovec iov[BUF_IOV];              /* FIXME */
};

struct net_rdp_state {
	char name[32];
	int local;			/* 1 - local source (track sequentially)
					 * 0 - remote source (track randomly) */
	int size;			/* max number of packets */
	int mask;
	uint32_t expt_size;
	uint32_t expt_mask;
	int nr_stripes;			/* size / bits in stripe->bits */
	int stripe_size;
	int stripe_mask;
	int stripe_shift;
	uint16_t gen_id;
	int head;			/* head of the tracked pkts ring */
	int tail;			/* tail of the tracker pkts ring */
	int cnt;			/* number of tracked pkts */
	int span;			/* head - tail */
	int watermark;			/* max packets to track */
	int data_watermark;		/* max non-ACK pkts to track */
	int ack_batch_size;		/* send ACK after that many recvs */
	uint32_t expt_tail;		/* expected tail of rnd fill ring */
	uint32_t expt_head;		/* expected head of rnd fill ring */
	struct net_rdp_stripe *stripes;	/* tracked pkts */
	struct net_rdp_stripe *seen;	/* Seen in this gen_id*/
	struct net_rdp_stripe *tbs;	/* To Be Seen */
	int was_modified;		/* state has tracked pkts */
	int *modified_stripes;		/* stripes w/ tracked pkts */
	void (*free_buffer)(struct net_rdp_buffer *);
	struct net_rdp_buffer *buffers;
	int drop_past;			/* cnt of pkts w/ id before tail */
	int drop_dup;			/* cnt of already seen pkts */
	int drop_future;		/* cnt of pkts too far in future */
	int data_pkts;			/* data pkts queued */
	int queued_ack_pkts;		/* ack pkts queued */
	uint64_t processed_ack_cnt;	/* sent pkts retired */
	int stalled;			/* 1 - pipeline stalled */
	uint64_t stalls;		/* how many times */

	uint64_t avg_rtt;		/* average RTT */
	int rtt_idx;			/* current slot in rtt array */
	int timeout_adds_rtt;		/* 1 - each rexmit adds up to avg_rtt */
	unsigned int rtt[128];		/* RTT of last N packets */

	int collect_lost_runs;
	uint64_t collect_lost_runtime;
	uint64_t tail_time_submitted;
	int lost_cnt;
	int lost_idx;
	int lost[128];
};

enum {
	NET_RDP_CURRENT_GEN_ID = -1
};

enum {
	NET_RDP_DATA = 1,
	NET_RDP_ACK,
	NET_RDP_INIT
};

struct net_rdp_socket {
	uint16_t tag;
	int fd;
	uint32_t dst;
	int port;
	uint64_t recv_time;
	uint64_t sent_time;
	uint64_t sent_cnt;
	uint64_t recv_cnt;
	uint64_t rexmit_cnt;
	uint64_t sent_acks_cnt;
	uint64_t recv_acks_cnt;
	timer_t keep_alive_timer;
	int keep_alive_tick;
	struct net_rdp_state recv_state;
	struct net_rdp_state sent_state;
};

#define UNUSED(x) (void)(x);

int net_rdp_socket_drive_state(struct net_rdp_socket *sock);

void net_rdp_stripe_print(char *msg, struct net_rdp_stripe *stripe)
{
	UNUSED(msg);
	UNUSED(stripe);
#if VERBOSE
	int i;
	uint64_t bits;

	dprintf(0, "%s [%05d:%02d] ", msg, stripe->gen_id, stripe->idx);
	bits = stripe->bits;

	for (i = 0; i < (int)(8 * sizeof(stripe->bits)); i++, bits >>= 1) {
		if (i && !(i & 3))
			dprintf(100, "_");
		dprintf(100, "%d", !!(bits & 1));
	}
#endif
}

void net_rdp_state_print(struct net_rdp_state *state)
{
	UNUSED(state);
#if VERBOSE
	int i, ii;
	int stripe_head = NET_RDP_STRIPE_HEAD(state);
	int stripe_tail = NET_RDP_STRIPE_TAIL(state);

	dprintf(1, "> %s\n", __func__);

	dprintf(0, "state %p %s: size %d, gen_id %d, tail %d, head %d, cnt %d, span %d\n",
		(void *)state, state->name, state->size, state->gen_id, state->tail, state->head, state->cnt, state->span);

	i = stripe_tail;
	do {
		net_rdp_stripe_print(state->name, &state->stripes[i]);
		if (i == stripe_tail)
			dprintf(100, " <= tail ");
		if (i == stripe_head)
			dprintf(100, " <= head ");
		dprintf(100, "\n");
		net_rdp_stripe_print("seen", &state->seen[i]);
		dprintf(100, "\n");
		net_rdp_stripe_print("tbs ", &state->tbs[i]);
		dprintf(100, "\n");
		ii = i;
		i = (i + 1) % state->nr_stripes;
	} while (ii != stripe_head);

	dprintf(-1, "< %s\n", __func__);
#endif
}

void net_rdp_recalc_rtt(struct net_rdp_state *state)
{
	int i, n;
	uint64_t new_avg_rtt = 0;

	/* recalc RTT */
	/* FIXME need it to be more robust, time-dependent */
	for (i = n = 0; i < (int)ARRAY_SIZE(state->rtt); i++) {
		if (state->rtt[i]) {
			new_avg_rtt += state->rtt[i];
			n++;
		}
	}
	if (n)
		new_avg_rtt /= n;
	dprintf(0, "avg_rtt %lu -> %lu\n", state->avg_rtt, new_avg_rtt);
	state->avg_rtt = new_avg_rtt;
}


void net_rdp_state_init(struct net_rdp_state *state, int local, uint16_t cnt,
			uint8_t ack_batch_size, int watermark, char *name)
{
	int i;
	int stripes;
	int stripe_size = 8 * sizeof(state->stripes[0].bits);

/* FIXME
	if (!is_power_2(cnt))
		abort();
*/

	stripes = cnt / stripe_size;
	memset(state, 0, sizeof(*state));
	state->local = local;
	state->size = cnt;
	state->mask = state->size - 1;
	state->expt_size = state->size * (1UL << (8 * sizeof(state->gen_id)));
	state->expt_mask = state->expt_size - 1;
	state->stripe_size = stripe_size;
	state->stripe_mask = state->stripe_size - 1;
	state->stripe_shift = __builtin_ffs(state->stripe_size) - 1;
	state->tail = 0;
	state->head = 0;
	state->nr_stripes = stripes;
	state->stripes = calloc(state->nr_stripes, sizeof(struct net_rdp_stripe));
	state->tbs = calloc(state->nr_stripes, sizeof(struct net_rdp_stripe));
	state->seen = calloc(state->nr_stripes, sizeof(struct net_rdp_stripe));
	state->modified_stripes = calloc(state->nr_stripes, sizeof(*state->modified_stripes));
	if (!state->stripes || !state->tbs || !state->seen || !state->modified_stripes)
		abort();
	state->watermark = watermark;
	state->ack_batch_size = ack_batch_size;
	/* FIXME How many slots does it really need for ACKs?  An
	 * appropriate ACK reserve is critical for the current design
	 * (ACKs are a separate packet flavor). This has to be changed
	 * soon: ACK will be embedded into DATA packets.
	 */
	state->data_watermark = state->watermark - (2 * state->watermark / state->ack_batch_size);
	if (name)
		strncpy(state->name, name, sizeof(state->name) - 1);
	else
		snprintf(state->name, sizeof(state->name), "s_%p\n", (void *)state);
	for (i = 0; i < state->nr_stripes; i++) {
		state->stripes[i].idx = i;
		state->tbs[i].idx = i;
		state->seen[i].idx = i;
	}
	state->buffers = calloc(cnt, sizeof(struct net_rdp_buffer));
	if (!state->buffers)
		abort();
	state->free_buffer = NULL; /* FIXME need a real fn */
	state->avg_rtt = 10000;
	state->timeout_adds_rtt = 1;
	net_rdp_state_print(state);
	/* state->cur_gen_id = random(); */
	/* FIXME randomize all indexes as well */
}

int net_rdp_track_tbs(struct net_rdp_state *state, uint16_t pkt)
{
	int ret, stripe = 0;
	int bit;
	uint64_t old_bits;

	dprintf(1, "> %s %p %s %d\n", __func__, (void *)state, state->name, pkt);

	stripe = pkt >> state->stripe_shift;
	bit = pkt & state->stripe_mask;
	old_bits = state->tbs[stripe].bits;
	/* check for dup */
	assert((state->tbs[stripe].bits & (1UL << bit)) == 0);
	state->tbs[stripe].bits |= 1UL << bit;
	ret = pkt;

	dprintf(-1, "< %s %p %s %d 0x%lx -> 0x%lx\n", __func__, (void *)state, state->name, ret, old_bits, state->tbs[stripe].bits);
	return ret;
}

int net_rdp_track_seen(struct net_rdp_state *state, uint16_t pkt)
{
	int ret, stripe = 0;
	int bit;

	dprintf(1, "> %s %p %s %d\n", __func__, (void *)state, state->name, pkt);

	stripe = pkt >> state->stripe_shift;
	bit = pkt & state->stripe_mask;
	/* check for dup */
	assert((state->seen[stripe].bits & (1UL << bit)) == 0);
	state->seen[stripe].bits |= 1UL << bit;
	ret = pkt;

	dprintf(-1, "< %s %p %s %d\n", __func__, (void *)state, state->name, ret);
	return ret;
}

int net_rdp_track_sequential(struct net_rdp_state *state, int *stripep)
{
	int ret, stripe = 0;
	int pkt, bit, old_head;
	struct net_rdp_stripe *l;

	dprintf(1, "> %s %p %s\n", __func__, (void *)state, state->name);

	pkt = state->head;
	stripe = pkt >> state->stripe_shift;
	bit = pkt & state->stripe_mask;
	l = &state->stripes[stripe];

	if (state->seen[stripe].bits & (1UL << bit)) {
		state->drop_dup++;
		ret = -EEXIST;
		goto out;
	}

	if ((state->cnt >= state->watermark) ||
	    (state->span >= state->watermark)) {
		dprintf(0, "hit watermark, won't track\n");
		net_rdp_state_print(state);
		ret = -EBUSY;
		goto out;
	}

	l->bits |= 1UL << bit;

	old_head = state->head;

	state->head = (state->head + 1) & state->mask;
	state->expt_head = (state->expt_head + 1) & state->expt_mask;

	if (old_head > state->head)
		state->gen_id++;

	state->cnt++;
	state->span++;

	state->modified_stripes[stripe]++;
	/* FIXME debug code, remove */
	assert(state->modified_stripes[stripe] != state->stripe_size + 1);

	state->was_modified++;
	ret = pkt;

out:
	if (stripep)
		*stripep = stripe;
	dprintf(-1, "< %s %p %s %d\n", __func__, (void *)state, state->name, ret);
	return ret;
}

int net_rdp_span(struct net_rdp_state *state, uint32_t head, uint32_t tail)
{
	dprintf(0, "%s:%d state %s, head %u, tail %u\n", __func__, __LINE__, state->name, head, tail);
	if (head < tail)
		head += state->expt_size;

	dprintf(0, "%s:%d state %s, head %u, tail %u, span %u\n", __func__, __LINE__, state->name, head, tail, head - tail);
	return head - tail;
}

int net_rdp_is_past(struct net_rdp_state *state, uint32_t id)
{
	/* Handle gen_id wrap. ID too much in past is not stale */
	if (id + 0x100 * state->size < state->expt_tail)
		return 0;

	if (state->expt_head < state->expt_tail) {
		return (id > state->expt_head) &&
			(id < state->expt_tail);
	} else {
		return id < state->expt_tail;
	}
}

int net_rdp_is_future(struct net_rdp_state *state, uint32_t id, uint32_t head)
{
	if (head < state->expt_tail) {
		return (id < state->expt_tail) &&
			(id > head);
	} else {
		if ((id < (uint32_t)state->size) &&
		    ((state->expt_size - head) < (uint32_t)state->size))
			id += state->expt_size;
		return id > head;
	}
}

int net_rdp_track_random(struct net_rdp_state *state, uint16_t gen_id, int id, int *stripep)
{
	int ret, stripe = 0, bit;
	uint32_t ext_id;

	dprintf(1, "> %s %p %s gen_id %d, id %d\n", __func__, (void *)state, state->name, gen_id, id);

	if (id < 0 || id > state->size) {
		printf("bad id %d (state->size %d)\n", id, state->size);
		ret = -EINVAL;
		goto out;
	}

	ext_id = gen_id * state->size + id;

	if (net_rdp_is_past(state, ext_id)) {
		dprintf(0, "       !!!!!! gen_id %d id %d is from past\n", gen_id, id);
		state->drop_past++;
		ret = -EINVAL;
		goto out;
	}

	if (net_rdp_is_future(state, ext_id, (state->expt_head + state->watermark + 1) % state->expt_mask)) {
		dprintf(0, "       !!!!!! gen_id %d id %d is too far in future\n", gen_id, id);
		state->drop_future++;
		ret = -EINVAL;
		goto out;
	}

	if ((state->span >= state->watermark) ||
	     (state->cnt >= state->watermark)) {
		ret = -EBUSY;
		goto out;
	}

	stripe = id >> state->stripe_shift;
	bit    = id & state->stripe_mask;

	if (state->seen[stripe].bits & (1UL << bit)) {
		dprintf(0, "       !!!!!! gen_id %d id %d is dup (has been seen)\n", gen_id, id);
		state->drop_dup++;
		ret = -EEXIST;
		goto out;
	}

	state->stripes[stripe].bits |= 1UL << bit;

	state->cnt++;

	if (net_rdp_is_future(state, (ext_id + 1) & state->expt_mask, state->expt_head)) {
		int old_head;

		old_head = state->head;
		state->head = (id + 1) & state->mask;
		if (state->head < old_head)
			state->gen_id++;

		state->expt_head = (ext_id + 1) & state->expt_mask;
		state->span = net_rdp_span(state, state->expt_head, state->expt_tail);
	}

	state->modified_stripes[stripe]++;
	state->was_modified++;
	ret = id;
out:
	if (stripep)
		*stripep = stripe;
	dprintf(-1, "< %s %p %s %d\n", __func__, (void *)state, state->name, ret);
	return ret;
}

int net_rdp_track(struct net_rdp_state *state, int gen_id, int id, int *stripep)
{
	int ret, i = -1;
	int old_head, h;

	old_head = state->head;

	net_rdp_state_print(state);

	if (state->local) {
		assert(id == NET_RDP_CURRENT_GEN_ID);
		ret = id = net_rdp_track_sequential(state, &i);
	} else {
		ret = net_rdp_track_random(state, gen_id, id, &i);
	}
	if (ret < 0)
		goto out;

	if (stripep)
		*stripep = i;

	if (!state->local)
		net_rdp_track_seen(state, id);

	for (h = old_head; h != state->head; h = (h + 1) & state->mask)
		net_rdp_track_tbs(state, h);

	if (id == state->tail)
		state->tail_time_submitted = state->buffers[state->tail].submitted;
out:
	net_rdp_state_print(state);

	return ret;
}

void net_rdp_untrack_tbs(struct net_rdp_state *state, uint16_t id)
{
	int i, bit;
	uint64_t old_bits;

	dprintf(1, "> %s %p %s %d\n", __func__, (void *)state, state->name, id);
	i   = id >> state->stripe_shift;
	bit = id & state->stripe_mask;
	old_bits = state->tbs[i].bits;
	state->tbs[i].bits &= ~(1UL << bit);
	dprintf(-1, "< %s %p %s %d, 0x%lx -> 0x%lx\n", __func__, (void *)state, state->name, id, old_bits, state->tbs[i].bits);
}

int net_rdp_untrack(struct net_rdp_state *state, uint16_t id)
{
	struct net_rdp_stripe *stripe;
	int i, bit;
	int ret;
	uint64_t bits, pos;
	uint32_t ext_id;

	dprintf(1, "> %s %p %s %d\n", __func__, (void *)state, state->name, id);

	i   = id >> state->stripe_shift;
	bit = id & state->stripe_mask;
	if (i >= state->nr_stripes) {
		printf("Uhhuh, stripe %d >= %d\n", i, state->nr_stripes);
		ret = -EINVAL;
		goto out;
	}
	pos = 1UL << bit;
	stripe = &state->stripes[i];

	/* can't untrack what's not been tracked */
	assert(stripe->bits & pos);

	stripe->bits &= ~pos;

	ext_id = stripe->gen_id * state->size + id;

	if (state->free_buffer)
		state->free_buffer(&state->buffers[id]);
	else {
		state->buffers[id].submitted = 0;
	}

	assert(state->cnt);
	state->cnt--;

	net_rdp_untrack_tbs(state, id);

	state->was_modified--;
	state->modified_stripes[i]--;

	if (ext_id == state->expt_tail) {
		/* walk up the chain and find a new tail */
		bits = state->tbs[i].bits;
		do {
			if (bits & (1UL << bit))
				break;

			state->expt_tail = (state->expt_tail + 1) & state->expt_mask;
			state->tail = (state->tail + 1) & state->mask;
			bit++;
			if (bit == state->stripe_size) {
				state->stripes[i].gen_id++;
				/* make sure SEEN can be cleared */
				assert(!state->stripes[i].bits);
				state->seen[i].bits = 0;
				i = (i + 1) % state->nr_stripes;
				bit = 0;
				bits = state->tbs[i].bits;
			}
		} while (state->expt_tail != state->expt_head);
		dprintf(0, "new tail %d, expt_tail %d, expt_head %d\n", state->tail, state->expt_tail, state->expt_head);
	}

	state->span = net_rdp_span(state, state->expt_head, state->expt_tail);
	assert(state->cnt <= state->span);

	if (id == state->tail)
		state->tail_time_submitted = state->buffers[state->tail].submitted;

	net_rdp_state_print(state);
	ret = id;

out:
	dprintf(-1, "< %s %p %s %d %d\n", __func__, (void *)state, state->name, id, ret);
	return ret;
}

int net_sendmsg(int fd, struct msghdr *msg, int flags)
{
	int ret;

again:
	ret = sendmsg(fd, msg, flags);
	if (ret < 0) {
		if ((errno == EINTR) || (errno == EAGAIN))
			goto again;
		else
			dprintf(0, "ret %d, errno %d\n", ret, errno);
	}
	return ret;
}

int net_rdp_state_collect_lost_buffers(struct net_rdp_state *state, struct net_rdp_buffer *bufs, int *lost, int max_cnt)
{
	struct net_rdp_stripe *stripe;
	int i, ii, lost_cnt = 0;
	uint64_t bits, now;
	uint64_t timeout;
	int j, pkt;

	/* guesstimate timeout to be 1.2 of avg RTT */
	timeout = 120 * state->avg_rtt / 100;
	now = time_us();

	if ((state->tail_time_submitted + timeout) > now)
		goto out;

	/* walk from tail to head and collect stale packets, up to max_cnt */
	i = NET_RDP_STRIPE_TAIL(state);
	do {
		stripe = &state->stripes[i];
		pkt = i * state->stripe_size;
		for (j = 0, bits = stripe->bits; bits && (lost_cnt < max_cnt); bits >>= 1, pkt++, j++) {
			struct net_rdp_buffer *buf;
			uint64_t diff;

			if (!(bits & 1))
				continue;

			buf = &bufs[pkt];
			if (!buf->submitted)
				continue;

			diff = now - buf->submitted;

			timeout += 10000 * VERBOSE;

			if (diff > timeout) {
				dprintf(0, "[%d] now %lu, pkt %lu, diff %lu, timeout %lu, avg_rtt %lu\n",
					pkt, now, state->buffers[pkt].submitted,
					diff, timeout, (uint64_t)state->avg_rtt);
				dprintf(0, "lost pkt ext_id %d id %d, gen_id %d\n",
					buf->hdr.gen_id * state->size + pkt, pkt,
					buf->hdr.gen_id);
				if (lost_cnt < max_cnt) {
					lost[lost_cnt++] = pkt;
				}

				if (state->timeout_adds_rtt) {
					state->rtt[state->rtt_idx] = diff;
					state->rtt_idx = (state->rtt_idx + 1) & (ARRAY_SIZE(state->rtt) - 1);
					net_rdp_recalc_rtt(state);
				}
			}
		}
		ii = i;
		i = (i + 1) % state->nr_stripes;
	} while (ii != NET_RDP_STRIPE_HEAD(state) &&
		 (lost_cnt < max_cnt));

out:
	state->collect_lost_runs++;
	state->collect_lost_runtime += time_us() - now;

	return lost_cnt;
}

int net_rdp_state_collect_lost(struct net_rdp_state *state, int *lost, int max_cnt)
{
	int ret = 0;

	/* dprintf(1, "> %s %p\n", __func__, state); */

	if (!state->cnt)
		goto out;

	ret = net_rdp_state_collect_lost_buffers(state, state->buffers, lost, max_cnt);

out:
	/* dprintf(-1, "< %s %d\n", __func__, ret); */
	return ret;
}

void net_rdp_retransmit_lost(struct net_rdp_socket *sock)
{
	int cnt;
	int ret;
	int i;
	struct net_rdp_state *state = &sock->sent_state;
	uint64_t now;

	if (!state->lost_cnt) {
		state->lost_cnt = net_rdp_state_collect_lost(state, state->lost, ARRAY_SIZE(state->lost));
		state->lost_idx = 0;
	}

	if (!state->lost_cnt)
		return;

	cnt = 1;	 /* one at a time */

	now = time_us();
	dprintf(1, " ====== RESEND LOST ========\n");
	for (i = 0; i < cnt; i++) {
		int pkt = state->lost[state->lost_idx + i];
		struct net_rdp_buffer *buf = &sock->sent_state.buffers[pkt];

		if (!buf->submitted)
			continue;

		dprintf(0, "resending lost pkt %d:%d, type %d\n", buf->hdr.gen_id, buf->hdr.id, buf->hdr.type);

		sock->rexmit_cnt++;
		ret = net_sendmsg(sock->fd, &buf->msg, MSG_DONTWAIT);
		if (ret == buf->len) {
			buf->submitted = now;
			/* if (buf->hdr.id == state->tail) */
			/* 	state->tail_time_submitted = buf->submitted; */
		} else {
			dprintf(0, "%s sendmsg ret=%d %d (%s)\n", __func__, ret, errno, strerror(errno));
		}
	}
	dprintf(-1, " ====== RESEND LOST ========\n");

	state->lost_idx++;
	state->lost_cnt--;
}

struct net_rdp_state s;

void net_rdp_timer_handler(int sig, siginfo_t *si, void *uc)
{
	UNUSED(sig);
	UNUSED(uc);
	struct net_rdp_socket *sock = (struct net_rdp_socket *)si->si_value.sival_ptr;

	sock->keep_alive_tick++;
}

void net_rdp_state_uninit(struct net_rdp_state *state)
{
	int i;

	for (i = 0; i < state->size; i++) {
		struct net_rdp_buffer *buf = &state->buffers[i];
		if (buf->submitted) {
			if (state->free_buffer)
				state->free_buffer(buf);
			else
				buf->submitted = 0;
		}
	}

	free(state->buffers);
	free(state->stripes);
	free(state->tbs);
	free(state->seen);
	free(state->modified_stripes);
}

void net_rdp_socket_init(struct net_rdp_socket *sock, uint16_t tag,
			 uint16_t ring_size, uint8_t ack_batch_size)
{
	/* FIXME shut the timer */

	if ((sock->tag == tag) && sock->tag)
		return;

	printf("reinit with tag %x (was %x), ring_size %d, ack_batch_size %d\n",
	       tag, sock->tag, ring_size, ack_batch_size);

	sock->tag = tag;

	net_rdp_state_uninit(&sock->recv_state);
	net_rdp_state_uninit(&sock->sent_state);

	net_rdp_state_init(&sock->recv_state, 0, ring_size, ack_batch_size, ring_size / 2, "recv");
	net_rdp_state_init(&sock->sent_state, 1, ring_size, ack_batch_size, ring_size / 2, "sent");
}

int net_rdp_socket_open(struct net_rdp_socket *sock, unsigned int dst, int port,
			uint16_t ring_size, uint8_t ack_batch_size)
{
	int ret;
	struct sockaddr_in addr;
	struct sigevent sev;
	struct itimerspec its;
	struct sigaction sa;

	memset(sock, 0, sizeof(*sock));
	sock->fd = socket(AF_INET, SOCK_DGRAM, 17);
	if (sock->fd < 0) {
		printf("socket %d (%s)\n", errno, strerror(errno));
		ret = errno;
		goto out;
	}

	sock->dst = dst;
	sock->port = port;

	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = INADDR_ANY;
	addr.sin_port = htons(6666 + sock->port);
	ret = bind(sock->fd, (struct sockaddr *)&addr, sizeof(addr));
	if (ret < 0) {
		printf("bind %d (%s)\n", errno, strerror(errno));
		ret = errno;
		goto out_socket;
	}

	net_rdp_socket_init(sock, 0, ring_size, ack_batch_size);

	sa.sa_flags = SA_SIGINFO;
	sa.sa_sigaction = net_rdp_timer_handler;
	sigemptyset(&sa.sa_mask);
	if (sigaction(SIGRTMIN, &sa, NULL) < 0) {
		printf("sigaction %d (%s)\n", errno, strerror(errno));
		goto out_socket;
	}

	sev.sigev_notify = SIGEV_SIGNAL;
	sev.sigev_signo = SIGRTMIN;
	sev.sigev_value.sival_ptr = sock;
	if (timer_create(CLOCK_REALTIME, &sev, &sock->keep_alive_timer) < 0) {
		printf("timer_create %d (%s)\n", errno, strerror(errno));
		goto out_socket;
	}

	/* every 100 ms */
	its.it_value.tv_sec = 0;
	its.it_value.tv_nsec = 100000000;
	its.it_interval.tv_sec = 0;
	its.it_interval.tv_nsec = 100000000;

	if (timer_settime(sock->keep_alive_timer, 0, &its, NULL) < 0) {
		printf("timer_settime %d (%s)\n", errno, strerror(errno));
		goto out_timer;
	}

	return 0;

out_timer:
	timer_delete(sock->keep_alive_timer);
out_socket:
	close(sock->fd);
out:
	return ret;
}

int net_rdp_socket_recv(struct net_rdp_socket *sock, void *_buf, int len);
int net_rdp_socket_send_iov(struct net_rdp_socket *sock, int type, struct iovec *iov_, int iov_len, int id, int *);
void net_rdp_buffer_init(struct net_rdp_socket *sock, struct net_rdp_buffer *buf);

void net_rdp_socket_peer_send_init(struct net_rdp_socket *sock)
{
	struct net_rdp_buffer buf;
	int i, ret;

	/* FIXME negotiate ring/ack params */
	memset(&buf, 0, sizeof(buf));
	net_rdp_buffer_init(sock, &buf);
	buf.hdr.type = NET_RDP_INIT;
	buf.hdr.id = random();
	buf.hdr.stripe_cnt = sock->sent_state.ack_batch_size;
	buf.hdr.recv_tail = sock->sent_state.size;

	printf("send reinit request with tag %x, ring size %d, ack_batch_size %d\n",
	       buf.hdr.id, buf.hdr.recv_tail, buf.hdr.stripe_cnt);

	for (i = 0; i < 3; i++) {
		ret = net_sendmsg(sock->fd, &buf.msg, 0);

		if (ret < 0) {
			printf("%s sendmsg %d (%s)\n", __func__, errno, strerror(errno));
		}
	}
	/* FIXME bad way of solving possible reordering */
	sleep(1);
}

int net_rdp_socket_send_ack(struct net_rdp_socket *sock)
{
	int i, j, ret = 0;
	int cnt = 0;
	int len = 0;
	int id = -1;
	struct iovec iov[256];

	dprintf(1, "> %s sock %p\n", __func__, (void *)sock);

	/* FIXME start from tail */
	for (i = j = 0; (j < (BUF_IOV - 1)) && (i < sock->recv_state.nr_stripes); i++) {
		if (sock->recv_state.modified_stripes[i]) {
			net_rdp_stripe_print(sock->recv_state.name, &sock->recv_state.stripes[i]);
			iov[j].iov_base = &sock->recv_state.stripes[i];
			iov[j].iov_len = sizeof(sock->recv_state.stripes[i]);
			len += iov[j].iov_len;
			j++;
			dprintf(0, "\n");
		}
	}

	if (!j) {
		dprintf(0, "recv has nothing to ack:\n");
		__dprintf_n++;
		net_rdp_state_print(&sock->recv_state);
		__dprintf_n--;
		goto out;
	}

	dprintf(0, "was mod %d, modified stipes cnt: %d\n", sock->recv_state.was_modified, j);
	ret = net_rdp_socket_send_iov(sock, NET_RDP_ACK, (struct iovec *)&iov, j, -1, &id);

	/* wasn't sent, don't zero the tracking info */
	if (ret < len)
		goto out;

	dprintf(0, "untracking acked pkts, j = %d\n", j);

	for (i = 0; j && (i < sock->recv_state.nr_stripes); i++) {
		struct net_rdp_stripe *stripe;
		uint32_t pkt;
		uint64_t bits;

		if (!sock->recv_state.modified_stripes[i])
			continue;

		j--;

		stripe = &sock->recv_state.stripes[i];
		bits = stripe->bits;
		pkt = stripe->idx * sock->recv_state.stripe_size;
		for (int k = 0; k < sock->recv_state.stripe_size; k++, bits >>= 1, pkt++) {
			if (bits & 1) {
				net_rdp_untrack(&sock->recv_state, pkt);
				cnt++;
			}
		}
	}

	dprintf(0, "%d pkts untracked\n", cnt);

	sock->sent_acks_cnt++;
	sock->sent_state.queued_ack_pkts++;

out:
	dprintf(-1, "< %s %p %d\n", __func__, (void *)sock, ret);
	return ret;
}

void net_rdp_process_ack(struct net_rdp_state *state, char *buf, int len, struct net_rdp_hdr *hdr)
{
	int i;
	struct net_rdp_stripe *stripes = (struct net_rdp_stripe *)buf;
	int n = hdr->stripe_cnt;
	uint64_t now = time_us();

	dprintf(1, "> %s %p buf %p len %d\n", __func__, (void *)state, (void *)buf, len);
	net_rdp_state_print(state);
	dprintf(0, "got %d acks:\n", n);

	for (i = 0; i < n; i ++) {
		struct net_rdp_stripe *rem_stripe;
		struct net_rdp_stripe *loc_stripe;
		int idx, pkt;
		uint64_t bits;
		int j;
		/* __verbose++; */
		rem_stripe = &stripes[i];
		net_rdp_stripe_print("rem", rem_stripe);
		idx = rem_stripe->idx;
		/* FIXME verify boundaries, etc */
		dprintf(0, "completing packets in local stripe %d...\n", idx);
		loc_stripe = &state->tbs[idx];
		net_rdp_stripe_print("tbs", loc_stripe);
		dprintf(0, "result:\n");

		bits = state->tbs[idx].bits & rem_stripe->bits;
		pkt = idx * state->stripe_size;

		for (j = 0; bits && (j < state->stripe_size); j++, bits >>= 1, pkt++) {
			if (bits & 1) {
				uint64_t diff = now - state->buffers[pkt].first_submitted;
				state->rtt[state->rtt_idx] = diff;
				state->rtt_idx = (state->rtt_idx + 1) & (ARRAY_SIZE(state->rtt) - 1);
				if (state->buffers[pkt].hdr.type == NET_RDP_ACK)
					state->queued_ack_pkts--;

				if (state->free_buffer)
					state->free_buffer(&state->buffers[pkt]);
				else
					state->buffers[pkt].submitted = 0;

				state->processed_ack_cnt++;

				net_rdp_track_seen(state, pkt);
				net_rdp_untrack(state, pkt);
			}
		}

		net_rdp_stripe_print("mod", loc_stripe);
		dprintf(0, "\n");
		/* __verbose--; */
	}
	net_rdp_recalc_rtt(state);
	net_rdp_state_print(state);
	dprintf(-1, "< %s %p\n", __func__, (void *)state);
}

int net_rdp_socket_recv(struct net_rdp_socket *sock, void *_buf, int _len)
{
	int ret, len;
	struct sockaddr_in peer = { 0 };
	socklen_t peer_len = 0;
	struct net_rdp_hdr *hdr;
	int stripe;
	char *p;
	char buf[8192];

	/* dprintf(1, "> %s\n", __func__); */

	ret = recvfrom(sock->fd, buf, sizeof(buf), MSG_DONTWAIT, (struct sockaddr *)&peer, &peer_len);
	if (ret < 0) {
		if (errno == EAGAIN) {
			goto out;
		} else {
			printf("recvfrom fd %d %d (%s)\n", sock->fd, errno, strerror(errno));
			ret = errno;
			goto out;
		}
	}

	len = ret;
	sock->recv_time = time_us();
	sock->recv_cnt++;
	dprintf(0, "received %d bytes\n", len);
	if (len == 0)
		goto out;
	hdr = (struct net_rdp_hdr *)buf;
	dprintf(0, "recv hdr: gen_id %d id %d type %d\n", hdr->gen_id, hdr->id, hdr->type);

	if (hdr->type == NET_RDP_INIT) {
		net_rdp_socket_init(sock, hdr->id, hdr->recv_tail, hdr->stripe_cnt);
		ret = 0;
		goto out;
	}

	ret = net_rdp_track(&sock->recv_state, hdr->gen_id, hdr->id, &stripe);
	if (ret < 0) {
		/* no recv_state space? then send acks sooner and try again */
		net_rdp_socket_drive_state(sock);
		ret = net_rdp_track(&sock->recv_state, hdr->gen_id, hdr->id, &stripe);
	}

	if (ret < 0)
		goto out;

	p = buf + sizeof(*hdr);
	len -= sizeof(*hdr);

	if (hdr->type == NET_RDP_ACK) {
		sock->recv_acks_cnt++;
		net_rdp_process_ack(&sock->sent_state, p, len, hdr);
		ret = 0;
		goto out;
	}

	if (len)
		sock->recv_state.data_pkts++;

	if (len > _len)
		len = _len;

	if (_buf)
		memcpy(_buf, p, len);

	ret = len;

out:
	/* dprintf(-1, "< %s ret %d\n", __func__, ret); */

	return ret;
}

void net_rdp_buffer_init(struct net_rdp_socket *sock, struct net_rdp_buffer *buf)
{
	buf->peer.sin_family = AF_INET;
	buf->peer.sin_addr.s_addr = sock->dst;
	buf->peer.sin_port = htons(6666 + (sock->port ? 0 : 1));

	buf->msg.msg_name = &buf->peer;
	buf->msg.msg_namelen = sizeof(buf->peer);
	buf->msg.msg_iov = buf->iov;
	buf->msg.msg_iovlen = 1;
	buf->msg.msg_flags = 0;

	buf->iov[0].iov_base = (void *)&buf->hdr;
	buf->iov[0].iov_len = sizeof(buf->hdr);
}

int net_rdp_socket_send(struct net_rdp_socket *sock, void *buf, int len)
{
	struct iovec iov;

	iov.iov_base = buf;
	iov.iov_len = len;
	return net_rdp_socket_send_iov(sock, NET_RDP_DATA, &iov, 1, -1, NULL);
}

int net_rdp_socket_send_iov(struct net_rdp_socket *sock, int type, struct iovec *iov_, int iov_len, int id, int *idp)
{
	struct net_rdp_buffer *buf;
	int stripe;
	int ret;
	int tracker_idx;
	int i;

	/* dprintf(1, "> %s %p %d\n", __func__, iov_, iov_len); */

	/* reserve slots for ACKs */
	if ((type != NET_RDP_ACK) &&
	    ((sock->sent_state.span >= (sock->sent_state.data_watermark)) ||
	     (sock->sent_state.cnt >= (sock->sent_state.data_watermark)))) {
		ret = -EBUSY;
		goto out;
	}

	tracker_idx = net_rdp_track(&sock->sent_state, NET_RDP_CURRENT_GEN_ID, id, &stripe);

	if (tracker_idx < 0) {
		dprintf(0, "net_rdp_track %d (%s)\n", tracker_idx, strerror(-tracker_idx));
		ret = tracker_idx;
		goto out;
	}
	sock->sent_state.stalled = 0;

	buf = &sock->sent_state.buffers[tracker_idx];

	net_rdp_buffer_init(sock, buf);

	memset(&buf->hdr, 0, sizeof(buf->hdr));

	buf->hdr.gen_id = sock->sent_state.stripes[stripe].gen_id;
	buf->hdr.id = tracker_idx;
	buf->hdr.recv_tail = sock->recv_state.tail;
	/* FIXME multiple hdrs in pkt */
	buf->hdr.type = type;
	buf->len = buf->iov[0].iov_len;

	if (type == NET_RDP_ACK) {
		buf->hdr.stripe_cnt = iov_len;
		/* copy stripes */
		for (i = 0; i < iov_len; i++) {
			buf->stripes[i] = *(struct net_rdp_stripe *)iov_[i].iov_base;
			buf->iov[i + 1].iov_base = &buf->stripes[i];
			buf->iov[i + 1].iov_len = sizeof(buf->stripes[i]);
			buf->msg.msg_iovlen++;
			buf->len += buf->iov[i + 1].iov_len;
		}
	} else {
		for (i = 0; i < iov_len; i++) {
			buf->iov[i + 1] = iov_[i];
			buf->msg.msg_iovlen++;
			buf->len += buf->iov[i + 1].iov_len;
		}
	}

	sock->sent_time = buf->submitted = buf->first_submitted = time_us();

	ret = net_sendmsg(sock->fd, &buf->msg, 0);
	if (ret < buf->len) {
		dprintf(0, "sendmsg ret=%d %d (%s)\n", ret, errno, strerror(errno));
	}
	sock->sent_cnt++;

	if (idp)
		*idp = tracker_idx;
out:
	/* dprintf(-1, "< %s %d written %d bytes\n", __func__, ret, total_len); */

	/* FIXME need to send ack along the way. Or embed it in
	 * NET_RDP_DATA packet, otherwise recv_state watermark stall
	 * happens */

	if ((ret == -EBUSY) & !sock->sent_state.stalled) {
		sock->sent_state.stalls++;
		sock->sent_state.stalled++;
	}

	return ret;
}

int net_rdp_socket_drive_state(struct net_rdp_socket *sock)
{
	int ret;

	/* TX - retransmit lost packets */

	/* if sender can't track more packets */
	if (sock->sent_state.stalled ||
	    /* there's a gap */
	    (sock->sent_state.span != sock->sent_state.cnt) ||
	    /* or every second */
	    sock->keep_alive_tick)
		net_rdp_retransmit_lost(sock);

	/* RX - send ACKs for received packets */

	/* more than 8 packets */
	if (sock->recv_state.cnt >= sock->recv_state.ack_batch_size) {
		dprintf(0, "send ack because recv_state.cnt (%d) >= recv_state.ack_batch_size (%d)\n",
			sock->recv_state.cnt, sock->recv_state.ack_batch_size);
		goto send_ack;
	}

	    /* or more than 8 processed acks (could come in 1 packet) */
	if (sock->recv_state.processed_ack_cnt >= (uint64_t)sock->recv_state.ack_batch_size) {
		dprintf(0, "send ack because recv_state.processed_ack_cnt (%ld) >= recv_state.ack_batch_size (%d)\n",
			sock->recv_state.processed_ack_cnt, sock->recv_state.ack_batch_size);
		goto send_ack;
	}

	    /* or recveiver can't track more packets */
	if (sock->recv_state.stalled) {
		dprintf(0, "send ack because recv_state.stalled (%d)\n", sock->recv_state.stalled);
		goto send_ack;
	}

	/* send any ACKs we can */
	if (sock->sent_state.stalled) {
		dprintf(0, "send ack because sent_state.stalled (%d)\n", sock->sent_state.stalled);
		goto send_ack;
	}

	    /* or every second */
	if (sock->keep_alive_tick) {
		dprintf(0, "send ack, because keep_alive_tick (%d)\n", sock->keep_alive_tick);
		goto send_ack;
	}

	return 0;

send_ack:
	if (net_rdp_socket_send_ack(sock)) {
		sock->recv_state.processed_ack_cnt = 0;
	}

	sock->keep_alive_tick = 0;

	/* 0 for now. !0 would mean "bad peer, terminate" */
	ret = 0;
	return ret;
}

void net_rdp_dump_buffers(struct net_rdp_state *state)
{
	int i, j, k;
	struct net_rdp_buffer *buf;
	char name[256];

	for (i = k = 0; i < state->size; i++) {
		buf = &state->buffers[i];
		if (buf->submitted) {
			snprintf(name, sizeof(name), "%s[%d:%d]", state->name, i, k);
			k++;
			dprintf(0, "%s type %d cnt %d\n", name, buf->hdr.type, buf->hdr.stripe_cnt);
			if (buf->hdr.type == NET_RDP_ACK) {
				for (j = 0; j < buf->hdr.stripe_cnt; j++) {
					net_rdp_stripe_print(name, (struct net_rdp_stripe *)buf->iov[j + 1].iov_base);
					dprintf(100, "\n");
				}
			}
		}
	}
}

char buf[32768];

int main(int argc, char *argv[])
{
	struct net_rdp_socket sock;
	int ret;
	long i;
	in_addr_t addr;
	int client;
	uint64_t _start, start, end, n, diff;
	int pkts;
	int ring_size      = 128;
	int pkt_size       = 512;
	int ack_batch_size = 32;

	if (argc < 3) {
		printf("%s <ip addr> <0 - server, 1 - client> [ring size] [ack batch size] [pkt size]\n", argv[0]);
		return 1;
	}
	addr = inet_addr(argv[1]);
	client = strtoul(argv[2], NULL, 10);

	printf("argc = %d\n", argc);
	switch (argc) {
	case 6:	pkt_size = strtoul(argv[5], NULL, 10); /* fall through */
	case 5: ack_batch_size = strtoul(argv[4], NULL, 10); /* fall through */
	case 4: ring_size = strtoul(argv[3], NULL, 10); /* fall through */
		break;
	default:
		break;
	}

	printf("I'm %s, sock %p, remote addr %x %s\n", client ? "client" : "server", (void *)&sock, addr, argv[1]);
	printf("ring size %d, pkt size %d, ack batch size %d\n", ring_size, pkt_size, ack_batch_size);

	srandom(time(NULL));

	ret = net_rdp_socket_open(&sock, addr, client, ring_size, ack_batch_size);

	if (!client)
		goto server;

	net_rdp_socket_peer_send_init(&sock);

	n = 0;
	start = _start = time_us();
	for (i = 0; ; i++) {
		do {
			int j;
			ret = 0;
			net_rdp_socket_drive_state(&sock);
			for (j = 0; ret >= 0 && j < sock.sent_state.ack_batch_size; j++) {
				ret = net_rdp_socket_recv(&sock, buf, pkt_size);
			}
		} while (ret > 0);
		ret = net_rdp_socket_send(&sock, buf, pkt_size);

		end = time_us();

		if ((end - start) > 1e6) {
			diff = end - start;
			pkts = 1e6 * n / diff;
			printf("%d pkt/s, rxmt:%lu ovhd:%3.2f rgenid:%u sgenid:%u sack:%lu rack:%lu p/a: %3.2f "
			       "st:%lu rtt:%lu\n",
			       pkts, sock.rexmit_cnt, sock.rexmit_cnt * 100.0 / i,
			       sock.recv_state.gen_id, sock.sent_state.gen_id,
			       sock.sent_acks_cnt, sock.recv_acks_cnt, (double)i / sock.recv_acks_cnt,
			       sock.sent_state.stalls, sock.sent_state.avg_rtt);
			start = end;
			n = 0;
		}
		if (ret < 0) {
			i--;
			continue;
		}
		n++;
		/* run for 10 seconds max */
		if (end - _start > 10e6)
			break;
	}
	printf("finalizing the rest %d pkts\n", sock.sent_state.cnt);
	while (sock.sent_state.cnt > 1) {
		net_rdp_socket_drive_state(&sock);
		net_rdp_socket_recv(&sock, NULL, 0);
	}
	net_rdp_state_print(&sock.sent_state);
	diff = time_us() - _start;
	pkts = 1e6 * i / diff;
	printf("done %ld pkts, in %3.2f s, %d pkt/s, %3.2f MB/s\n", i, diff / 1e6, pkts, (double)pkts * pkt_size / (1UL << 20));
	printf("rexmit_cnt %lu, overhead %3.2f%%\n", sock.rexmit_cnt, sock.rexmit_cnt * 100.0 / i);
	printf("sent_acks_cnt %lu, recv_acks_cnt %lu, pkt/acks %3.2f\n", sock.sent_acks_cnt, sock.recv_acks_cnt, (double)i / sock.recv_acks_cnt);
	printf("stalls %lu\n", sock.sent_state.stalls);
	printf("collect_lost_runs %d, runtime %lu us\n", sock.sent_state.collect_lost_runs, sock.sent_state.collect_lost_runtime);
	printf("avg_rtt %lu us\n", sock.sent_state.avg_rtt);
	printf("RET=%d\n", pkts);
	goto out;
server:
	for (;;) {
		net_rdp_socket_drive_state(&sock);
		net_rdp_socket_recv(&sock, buf, pkt_size);
	}
out:
	return 0;
}
