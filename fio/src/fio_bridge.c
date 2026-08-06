#include <errno.h>
#include <stdint.h>
#include <stdlib.h>
#include "fio.h"
#include "optgroup.h"

struct onyx_options { void *pad; char *socket; char *volume; };

extern void *onyx_rs_init(const char *, const char *, unsigned int, int *);
extern int onyx_rs_queue(void *, void *, int, uint64_t, void *, uint32_t, int *);
extern int onyx_rs_getevents(void *, unsigned int, unsigned int,
			     const struct timespec *);
extern int onyx_rs_commit(void *);
extern void *onyx_rs_event(void *, int);
extern void onyx_rs_cleanup(void *);

static struct fio_option options[] = {
	{ .name = "onyx_socket", .lname = "Onyx Direct IO socket",
	  .type = FIO_OPT_STR_STORE, .off1 = offsetof(struct onyx_options, socket),
	  .help = "Control socket path; the engine appends .io",
	  .category = FIO_OPT_C_ENGINE, .group = FIO_OPT_G_INVALID },
	{ .name = "onyx_volume", .lname = "Onyx volume",
	  .type = FIO_OPT_STR_STORE, .off1 = offsetof(struct onyx_options, volume),
	  .help = "Onyx volume name",
	  .category = FIO_OPT_C_ENGINE, .group = FIO_OPT_G_INVALID },
	{ .name = NULL },
};

static int onyx_init(struct thread_data *td)
{
	struct onyx_options *o = td->eo;
	int error = 0;
	if (!o->socket || !o->volume) {
		log_err("onyx: onyx_socket and onyx_volume are required\n");
		return 1;
	}
	td->io_ops_data = onyx_rs_init(o->socket, o->volume, td->o.iodepth, &error);
	if (!td->io_ops_data) {
		log_err("onyx: initialization failed: %s\n", strerror(error));
		return 1;
	}
	return 0;
}

static enum fio_q_status onyx_queue(struct thread_data *td, struct io_u *io_u)
{
	int error = 0;
	fio_ro_check(td, io_u);
	int status = onyx_rs_queue(td->io_ops_data, io_u, io_u->ddir,
				   io_u->offset, io_u->xfer_buf,
				   io_u->xfer_buflen, &error);
	io_u->error = error;
	return status;
}

/* `queue` only stages requests, so fio must be told to flush them. Without this
 * hook every IO paid its own header + payload write syscalls, unbatched. */
static int onyx_commit(struct thread_data *td)
{
	return onyx_rs_commit(td->io_ops_data);
}

static int onyx_getevents(struct thread_data *td, unsigned int min,
			  unsigned int max, const struct timespec *timeout)
{
	return onyx_rs_getevents(td->io_ops_data, min, max, timeout);
}

static struct io_u *onyx_event(struct thread_data *td, int event)
{
	return onyx_rs_event(td->io_ops_data, event);
}

static void onyx_cleanup(struct thread_data *td)
{
	onyx_rs_cleanup(td->io_ops_data);
	td->io_ops_data = NULL;
}

static int onyx_open_file(struct thread_data fio_unused *td,
			  struct fio_file fio_unused *f) { return 0; }

static struct ioengine_ops ioengine = {
	.name = "onyx", .version = FIO_IOOPS_VERSION,
	.options = options, .option_struct_size = sizeof(struct onyx_options),
	.queue = onyx_queue, .commit = onyx_commit,
	.getevents = onyx_getevents, .event = onyx_event,
	.init = onyx_init, .cleanup = onyx_cleanup, .open_file = onyx_open_file,
	.flags = FIO_DISKLESSIO,
};

void get_ioengine(struct ioengine_ops **ops) { *ops = &ioengine; }
