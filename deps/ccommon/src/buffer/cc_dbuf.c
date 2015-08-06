#include <buffer/cc_dbuf.h>

#include <cc_bstring.h>
#include <cc_debug.h>
#include <cc_mm.h>

#include <stddef.h>

#define DBUF_MODULE_NAME "ccommon::buffer::dbuf"

static bool dbuf_init = false;

/* Maximum size of the buffer */
static uint8_t max_power = DBUF_DEFAULT_MAX;
static uint32_t max_size = BUF_INIT_SIZE << DBUF_DEFAULT_MAX;

void
dbuf_setup(uint8_t power)
{
    log_info("set up the %s module", DBUF_MODULE_NAME);

    /* TODO(yao): validate input */
    max_power = power;
    max_size = buf_init_size << power;

    if (dbuf_init) {
        log_warn("%s has already been setup, overwrite", DBUF_MODULE_NAME);
    }

    dbuf_init = true;

    log_info("buffer/dbuf: max size %zu", max_size);
}

void
dbuf_teardown(void)
{
    log_info("tear down the %s module", DBUF_MODULE_NAME);

    if (!dbuf_init) {
        log_warn("%s was not setup", DBUF_MODULE_NAME);
    }

    dbuf_init = false;
}

static rstatus_t
_dbuf_resize(struct buf **buf, uint32_t nsize)
{
    struct buf *nbuf;
    uint32_t size = buf_size(*buf);

    /* cc_realloc can return an address different than *buf, hence we should
     * update *buf if allocation is successful, but leave it if failed.
     */
    nbuf = cc_realloc(*buf, nsize);

    if (nbuf == NULL) {
        return CC_ENOMEM;
    }

    /* only end needs adjustment, other fields are copied by realloc */
    nbuf->end = (uint8_t *)nbuf + nsize;
    *buf = nbuf;
    DECR_N(buf_metrics, buf_memory, size);
    INCR_N(buf_metrics, buf_memory, nsize);

    return CC_OK;
}

rstatus_t
dbuf_double(struct buf **buf)
{
    ASSERT(buf_capacity(*buf) <= max_size);

    uint32_t nsize = buf_size(*buf) * 2;

    if (nsize > max_size) {
        return CC_ERROR;
    }

    return _dbuf_resize(buf, nsize);
}

rstatus_t
dbuf_fit(struct buf **buf, uint32_t cap)
{
    uint32_t nsize = buf_init_size;

    if (cap + BUF_HDR_SIZE > max_size) {
        return CC_ERROR;
    }

    /* cap is checked, given how max_size is initialized this is safe */
    while (nsize < cap + BUF_HDR_SIZE) {
        nsize *= 2;
    }

    return _dbuf_resize(buf, nsize);
}

rstatus_t
dbuf_shrink(struct buf **buf)
{
    return _dbuf_resize(buf, buf_init_size);
}