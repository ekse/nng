#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>
#include <pthread.h>
#include <assert.h>

#include <nng/nng.h>
#include <nng/protocol/reqrep0/rep.h>
#include <nng/protocol/reqrep0/req.h>
#include <nng/supplemental/util/platform.h>

#ifndef PARALLEL
#define PARALLEL 128
#endif

struct work {
	enum { INIT, RECV, PROCESS, SEND, DONE } state;
	nng_aio *aio;
	nng_msg *msg;
	nng_ctx  ctx;
    pthread_mutex_t lock;
    uint32_t id;
};

void
fatal(const char *func, int rv)
{
	fprintf(stderr, "%s: %s\n", func, nng_strerror(rv));
	exit(1);
}

void
server_cb(void *arg)
{
	struct work *work = arg;
	nng_msg *    msg;
	int          rv;
	uint32_t     id;

	switch (work->state) {
	case INIT:
		work->state = RECV;
		nng_ctx_recv(work->ctx, work->aio);
		break;
	case RECV:
		if ((rv = nng_aio_result(work->aio)) != 0) {
			fatal("nng_ctx_recv", rv);
		}
		msg = nng_aio_get_msg(work->aio);
		if ((rv = nng_msg_trim_u32(msg, &id)) != 0) {
			// bad message, just ignore it.
			nng_msg_free(msg);
			nng_ctx_recv(work->ctx, work->aio);
			return;
		}
		
        if ((rv = nng_msg_append_u32(msg, id)) != 0) {
			fatal("nng_msg_append_u32", rv);
		}

		work->msg = msg;
		work->state = PROCESS;
		nng_sleep_aio(id * 100, work->aio);
		break;
	case PROCESS:
		work->state = SEND;
		nng_ctx_send(work->ctx, work->aio);
		break;
    case SEND:
		if ((rv = nng_aio_result(work->aio)) != 0) {
			nng_msg_free(work->msg);
			fatal("nng_ctx_send", rv);
		}
		work->state = RECV;
		nng_ctx_recv(work->ctx, work->aio);
		break;
	default:
		fatal("bad state!", NNG_ESTATE);
		break;
	}
}

struct work* alloc_work(nng_socket sock)
{
	struct work *w;
	int          rv;

	if ((w = nng_alloc(sizeof(*w))) == NULL) {
		fatal("nng_alloc", NNG_ENOMEM);
	}
	if ((rv = nng_aio_alloc(&w->aio, server_cb, w)) != 0) {
		fatal("nng_aio_alloc", rv);
	}
	if ((rv = nng_ctx_open(&w->ctx, sock)) != 0) {
		fatal("nng_ctx_open", rv);
	}
	w->state = INIT;
	return (w);
}

// The server runs forever.
int server(const char *url)
{
	nng_socket   sock;
	struct work *works[PARALLEL];
	int          rv;
	int          i;

	/*  Create the socket. */
	rv = nng_rep0_open(&sock);
	if (rv != 0) {
		fatal("nng_rep0_open", rv);
	}

	for (i = 0; i < PARALLEL; i++) {
		works[i] = alloc_work(sock);
	}

	if ((rv = nng_listen(sock, url, NULL, 0)) != 0) {
		fatal("nng_listen", rv);
	}

	for (i = 0; i < PARALLEL; i++) {
		server_cb(works[i]); // this starts them going (INIT state)
	}

	for (;;) {
		nng_msleep(3600000); // neither pause() nor sleep() portable
	}
}

void client_cb(void *arg)
{
	struct work* work = arg;
	nng_msg* msg;
	int rv;

	switch (work->state) {
	case INIT:
		work->state = SEND;
		pthread_mutex_lock(&work->lock);

		nng_msg_alloc(&msg, 0);

		if ((rv = nng_msg_append_u32(msg, work->id)) != 0) {
			fatal("nng_msg_append_u32", rv);
		}

		work->msg = msg;
		nng_aio_set_msg(work->aio, work->msg);
		printf("client_cb: send_aio\n");
		nng_ctx_send(work->ctx, work->aio);
		break;
	case SEND:
		if ((rv = nng_aio_result(work->aio)) != 0) {
			nng_msg_free(work->msg);
			fatal("nng_send_aio", rv);
		}
		work->state = RECV;
		nng_ctx_recv(work->ctx, work->aio);
		break;
	case RECV:
		if ((rv = nng_aio_result(work->aio)) != 0) {
			fatal("nng_recv_aio", rv);
		}
		msg = nng_aio_get_msg(work->aio);
		printf("msg = %p\n", msg);
		
        if ((rv = nng_msg_trim_u32(msg, &work->id)) != 0) {
			fatal("bad message", rv);
		}

		printf("received answer, releasing lock..\n");
		nng_msg_free(msg);
		pthread_mutex_unlock(&work->lock);
		printf("lock released\n");
		work->state = DONE;
		break;
	case DONE:
		break;
	default:
		fatal("bad state!", NNG_ESTATE);
		break;
	}
}

struct work* client_alloc_work(nng_socket sock)
{
	struct work *w;
	int rv;

	if ((w = nng_alloc(sizeof(*w))) == NULL) {
		fatal("nng_alloc", NNG_ENOMEM);
	}
	if ((rv = nng_aio_alloc(&w->aio, client_cb, w)) != 0) {
		fatal("nng_aio_alloc", rv);
	}
	
    if ((rv = nng_ctx_open(&w->ctx, sock)) != 0) {
		fatal("nng_ctx_open", rv);
	}

	w->state = INIT;
	return w;
}


uint32_t send_irp(uint32_t id, nng_socket sock)
{
	struct work* w;
    uint32_t received_id;

	w = client_alloc_work(sock);
    w->id = id;
	
	pthread_mutex_init(&w->lock, NULL);
	client_cb(w);
	pthread_mutex_lock(&w->lock);
	received_id = w->id;

    pthread_mutex_destroy(&w->lock);
    nng_aio_free(w->aio);
    
    return received_id;
}

void* client_thread1(void* arg)
{
    nng_socket* sock = arg;
    uint32_t next_irp_id = 0;
    uint32_t id;
    uint32_t received_id;

    while(1)
	{
		id = next_irp_id++;
		received_id = send_irp(id, *sock);
        assert(received_id == id);
		printf("got response: id = %d.\n", id);
		nng_msleep(100);
	}

    return NULL;
}

void* client_thread2(void* arg)
{
    nng_socket* sock = arg;
    uint32_t next_irp_id = 100;
    uint32_t id;
    uint32_t received_id;

    while(1)
	{
		id = next_irp_id++;
		received_id = send_irp(id, *sock);
        assert(received_id == id);
		printf("got response: id = %d.\n", id);
		nng_msleep(100);
	}

    return NULL;
}

int client(const char *url)
{
	nng_socket sock;
	int rv;
    pthread_t client1;
    pthread_t client2;

	if ((rv = nng_req0_open(&sock)) != 0) {
		fatal("nng_req0_open", rv);
	}

	if ((rv = nng_dial(sock, url, NULL, 0)) < 0) {
		fatal("nng_dial", rv);
	}

    pthread_create(&client1, NULL, client_thread1, &sock);
    nng_msleep(1000);
    pthread_create(&client2, NULL, client_thread2, &sock);
    nng_msleep(1000 * 60 * 60);

	nng_close(sock);

	return 0;
}

int main(int argc, char **argv)
{
	int rc;

	if (argc < 2) {
		fprintf(stderr, "Usage: %s <url> [-s]\n", argv[0]);
		exit(EXIT_FAILURE);
	}

	if (argc == 2) {
		rc = client(argv[1]);
	} else {
		rc = server(argv[1]);
	}

	exit(rc == 0 ? EXIT_SUCCESS : EXIT_FAILURE);
}
