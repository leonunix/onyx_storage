#include <errno.h>
#include <stdint.h>
#include <stdlib.h>
#include "fio.h"
#include "optgroup.h"

struct onyx_options { void *pad; char *socket; char *volume; };

/* Operation codes the Rust side speaks.
 *
 * fio's `enum fio_ddir` numbering is part of its private ABI, so it is
 * translated HERE — the only file that includes fio.h — and the protocol
 * client never depends on fio's values. */
#define ONYX_OP_READ  0
#define ONYX_OP_WRITE 1
#define ONYX_OP_TRIM  2
#define ONYX_OP_SYNC  3
#define ONYX_OP_NONE  (-1)

extern void *onyx_rs_init(const char *, const char *, unsigned int, uint32_t,
			  uint32_t, int *);
extern int onyx_rs_queue(void *, void *, int, uint64_t, void *, uint32_t, int *);
extern int onyx_rs_getevents(void *, unsigned int, unsigned int,
			     const struct timespec *);
extern int onyx_rs_commit(void *);
extern void *onyx_rs_event(void *, int);
extern void onyx_rs_cleanup(void *);
extern uint64_t onyx_rs_volume_size(void *);
extern uint64_t onyx_rs_probe_volume_size(const char *, const char *);
extern uint32_t onyx_rs_max_io_bytes(void);

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

static uint32_t onyx_max_u32(uint32_t a, uint32_t b) { return a > b ? a : b; }

/* Largest block this job may ever hand us, across every direction it will
 * issue. Buffers on the Rust side are sized from this, so a bs=4k job does
 * not pay for a bs=128k job's frames. */
static uint32_t onyx_job_max_bs(struct thread_data *td)
{
	uint32_t max_bs = 0;
	for (int ddir = 0; ddir < DDIR_RWDIR_CNT; ddir++)
		max_bs = onyx_max_u32(max_bs, (uint32_t) td->o.max_bs[ddir]);
	return max_bs;
}

/* Coarsest guaranteed offset alignment. fio defaults `ba` to `min_bs`, so
 * --bs=4k-32k is already 4 KiB-aligned; an explicit finer --ba is what this
 * exists to catch. A zero `ba` means fio will fall back to the block size. */
static uint32_t onyx_job_offset_align(struct thread_data *td)
{
	uint32_t align = 0;
	for (int ddir = 0; ddir < DDIR_RWDIR_CNT; ddir++) {
		uint32_t ba = (uint32_t) td->o.ba[ddir];
		if (!ba)
			ba = (uint32_t) td->o.min_bs[ddir];
		if (!ba)
			continue;
		if (!align || ba < align)
			align = ba;
	}
	return align;
}

static int onyx_op_from_ddir(enum fio_ddir ddir)
{
	switch (ddir) {
	case DDIR_READ:		return ONYX_OP_READ;
	case DDIR_WRITE:	return ONYX_OP_WRITE;
	case DDIR_TRIM:		return ONYX_OP_TRIM;
	case DDIR_SYNC:
	case DDIR_DATASYNC:	return ONYX_OP_SYNC;
	default:		return ONYX_OP_NONE;
	}
}

static int onyx_init(struct thread_data *td)
{
	struct onyx_options *o = td->eo;
	uint32_t max_bs, align, ceiling;
	int error = 0;

	if (!o->socket || !o->volume) {
		log_err("onyx: onyx_socket and onyx_volume are required\n");
		return 1;
	}

	/* Checked here rather than per-IO: a bad bs would otherwise surface as
	 * an EINVAL on the first IO, which fio reports as a device error. */
	max_bs = onyx_job_max_bs(td);
	align = onyx_job_offset_align(td);
	ceiling = onyx_rs_max_io_bytes();
	if (!max_bs || max_bs % 4096u) {
		log_err("onyx: bs must be a multiple of 4096 (job max is %u)\n", max_bs);
		return 1;
	}
	if (max_bs > ceiling) {
		log_err("onyx: bs %u exceeds the protocol maximum of %u\n", max_bs, ceiling);
		return 1;
	}
	if (!align || align % 4096u) {
		log_err("onyx: offsets must be 4096-aligned; this job aligns to %u "
			"(check --ba / --bs_unaligned)\n", align);
		return 1;
	}

	td->io_ops_data = onyx_rs_init(o->socket, o->volume, td->o.iodepth,
				       max_bs, align, &error);
	if (!td->io_ops_data) {
		log_err("onyx: initialization failed: %s\n", strerror(error));
		return 1;
	}
	return 0;
}

static enum fio_q_status onyx_queue(struct thread_data *td, struct io_u *io_u)
{
	int error = 0;
	int op = onyx_op_from_ddir(io_u->ddir);
	int status;

	fio_ro_check(td, io_u);
	if (op == ONYX_OP_NONE) {
		io_u->error = EOPNOTSUPP;
		return FIO_Q_COMPLETED;
	}
	status = onyx_rs_queue(td->io_ops_data, io_u, op, io_u->offset,
			       io_u->xfer_buf, io_u->xfer_buflen, &error);
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

/* Report the volume size the engine gives us at HELLO, so a job file does not
 * need `size=`.
 *
 * ⚠ This can run in EITHER order relative to ->init. With the default
 * create_serialize=1, fio runs setup_files (and so this) in the parent before
 * the job thread reaches ->init, so io_ops_data is still NULL; with
 * create_serialize=0 the session already exists. Both are handled: the probe
 * opens its own throwaway session only when there is no client to ask.
 * A 0 result is not an error — it just puts fio back to requiring `size=`. */
static int onyx_get_file_size(struct thread_data *td, struct fio_file *f)
{
	struct onyx_options *o = td->eo;

	if (fio_file_size_known(f))
		return 0;

	if (td->io_ops_data)
		f->real_file_size = onyx_rs_volume_size(td->io_ops_data);
	else if (o && o->socket && o->volume)
		f->real_file_size = onyx_rs_probe_volume_size(o->socket, o->volume);
	else
		return 0;

	if (!f->real_file_size)
		return 0;
	fio_file_set_size_known(f);
	return 0;
}

static struct ioengine_ops ioengine = {
	.name = "onyx", .version = FIO_IOOPS_VERSION,
	.options = options, .option_struct_size = sizeof(struct onyx_options),
	.queue = onyx_queue, .commit = onyx_commit,
	.getevents = onyx_getevents, .event = onyx_event,
	.init = onyx_init, .cleanup = onyx_cleanup, .open_file = onyx_open_file,
	.get_file_size = onyx_get_file_size,
	.flags = FIO_DISKLESSIO,
};

void get_ioengine(struct ioengine_ops **ops) { *ops = &ioengine; }
