#include <twemcache/bb_setting.h>
#include <twemcache/bb_stats.h>

#include <storage/slab/bb_item.h>
#include <storage/slab/bb_slab.h>
#include <time/bb_time.h>
#include <util/bb_core.h>
#include <util/bb_util.h>

#include <cc_log.h>
#include <cc_option.h>
#include <cc_signal.h>

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <sys/socket.h>
#include <sysexits.h>

static struct setting setting = {
    SETTING(OPTION_INIT)
};

#define PRINT_DEFAULT(_name, _type, _default, _description) \
    log_stdout("  %-31s ( default: %s )", #_name,  _default);

static const unsigned int nopt = OPTION_CARDINALITY(struct setting);

struct stats Stats = {
    STATS(METRIC_INIT)
};

const unsigned int Nmetric = METRIC_CARDINALITY(struct stats);

static void
show_usage(void)
{
    log_stdout(
            "Usage:" CRLF
            "  broadbill_twemcache [option|config]" CRLF
            );
    log_stdout(
            "Description:" CRLF
            "  broadbill_twemcache is one of the unified cache backends. " CRLF
            "  It uses a slab based key/val storage scheme to cache key/val" CRLF
            "  pairs. It speaks the memcached protocol and supports all " CRLF
            "  ASCII memcached commands." CRLF
            );
    log_stdout(
            "Options:" CRLF
            "  -h, --help        show this message" CRLF
            "  -v, --version     show version number" CRLF
            );
    log_stdout(
            "Example:" CRLF
            "  ./broadbill_slimcache ../template/slimcache.config" CRLF
            );
    log_stdout("Setting & Default Values:");
    SETTING(PRINT_DEFAULT)
}

static void
setup(void)
{
    struct addrinfo *ai;
    /* int ret; */
    rstatus_t status;

    /* Setup log */
    if (log_setup((int)setting.log_level.val.vuint,
                 setting.log_name.val.vstr) < 0) {
        log_error("log setup failed");
        goto error;
    }

    time_setup();

    buf_setup((uint32_t)setting.buf_size.val.vuint);
    dbuf_setup((uint32_t)setting.dbuf_max_size.val.vuint,
               (uint32_t)setting.dbuf_shrink_factor.val.vuint);

    if (slab_setup((uint32_t)setting.slab_size.val.vuint,
                   setting.slab_use_cas.val.vbool,
                   setting.slab_prealloc.val.vbool,
                   (int)setting.slab_evict_opt.val.vuint,
                   setting.slab_use_freeq.val.vbool,
                   (size_t)setting.slab_chunk_size.val.vuint,
                   (size_t)setting.slab_maxbytes.val.vuint,
                   setting.slab_profile.val.vstr,
                   (uint8_t)setting.slab_profile_last_id.val.vuint)
        != CC_OK) {
        log_error("slab module setup failed");
        goto error;
    }

    if (item_setup((uint32_t)setting.slab_hash_power.val.vuint) != CC_OK) {
        log_error("item module setup failed");
        goto error;
    }

    buf_sock_pool_create((uint32_t)setting.buf_sock_poolsize.val.vuint);
    request_pool_create((uint32_t)setting.request_poolsize.val.vuint);

    /* set up core */
    status = getaddr(&ai, setting.server_host.val.vstr,
                     setting.server_port.val.vstr);

    if(status != CC_OK) {
        log_error("address invalid");
        goto error;
    }

    /* Set up core with connection ring array being either the tcp poolsize or
       the ring array default capacity if poolsize is unlimited */
    status = core_setup(ai, (setting.tcp_poolsize.val.vuint == 0 ?
                             setting.ring_array_cap.val.vuint :
                             setting.tcp_poolsize.val.vuint));
    freeaddrinfo(ai);

    if (status != CC_OK) {
        log_crit("could not start core event loop");
        goto error;
    }

    /* Not overriding signals for now, since we are still testing */

    /* override signals that we want to customize */
    /* ret = signal_segv_stacktrace(); */
    /* if (ret < 0) { */
    /*     goto error; */
    /* } */

    /* ret = signal_ttin_logrotate(); */
    /* if (ret < 0) { */
    /*     goto error; */
    /* } */

    /* ret = signal_pipe_ignore(); */
    /* if (ret < 0) { */
    /*     goto error; */
    /* } */

    /* daemonize */
    if (setting.daemonize.val.vbool) {
        daemonize();
    }

    /* create pid file, call it after daemonize to have the correct pid */
    if (!option_empty(&setting.pid_filename)) {
        create_pidfile(setting.pid_filename.val.vstr);
    }

    return;

error:
    if (!option_empty(&setting.pid_filename)) {
        remove_pidfile(setting.pid_filename.val.vstr);
    }

    core_teardown();

    request_pool_destroy();
    buf_sock_pool_destroy();
    conn_pool_destroy();

    item_teardown();
    slab_teardown();
    dbuf_teardown();
    buf_teardown();
    time_teardown();

    log_teardown();

    log_crit("setup failed");

    exit(EX_CONFIG);
}

int
main(int argc, char **argv)
{
    rstatus_t status = CC_OK;;
    FILE *fp = NULL;

    if (argc > 2) {
        show_usage();
        exit(EX_USAGE);
    }

    if (argc == 1) {
        log_stderr("launching server with default values.");
    } else {
        /* argc == 2 */
        if (strcmp(argv[1], "-h") == 0 || strcmp(argv[1], "--help") == 0) {
            show_usage();

            exit(EX_OK);
        }
        if (strcmp(argv[1], "-v") == 0 || strcmp(argv[1], "--version") == 0) {
            show_version();

            exit(EX_OK);
        }
        fp = fopen(argv[1], "r");
        if (fp == NULL) {
            log_stderr("cannot open config: incorrect path or doesn't exist");

            exit(EX_DATAERR);
        }
    }

    if (option_load_default((struct option *)&setting, nopt) != CC_OK) {
        log_stderr("failed to load default option values");
        exit(EX_CONFIG);
    }

    if (fp != NULL) {
        log_stderr("load config from %s", argv[1]);
        status = option_load_file(fp, (struct option *)&setting, nopt);
        fclose(fp);
    }
    if (status != CC_OK) {
        log_stderr("failed to load config");

        exit(EX_DATAERR);
    }

    option_printall((struct option *)&setting, nopt);

    setup();

    core_run();

    exit(EX_OK);
}
