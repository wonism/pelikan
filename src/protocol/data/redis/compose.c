#include <protocol/data/redis/compose.h>

#include <protocol/data/redis/request.h>
#include <protocol/data/redis/response.h>
#include <time/time.h>

#include <cc_debug.h>
#include <cc_print.h>

#define COMPOSE_MODULE_NAME "protocol::redis::compose"

#define NOREPLY "$-1\r\n"
#define NOREPLY_LEN (sizeof(NOREPLY) - 1)
#define CC_INT64_MAXLEN (CC_UINT64_MAXLEN + 1)

static bool compose_init = false;
static compose_req_metrics_st *compose_req_metrics = NULL;
static compose_rsp_metrics_st *compose_rsp_metrics = NULL;

void
compose_setup(compose_req_metrics_st *req, compose_rsp_metrics_st *rsp)
{
    log_info("set up the %s module", COMPOSE_MODULE_NAME);

    compose_req_metrics = req;
    if (compose_req_metrics != NULL) {
        COMPOSE_REQ_METRIC_INIT(compose_req_metrics);
    }
    compose_rsp_metrics = rsp;
    if (compose_rsp_metrics != NULL) {
        COMPOSE_RSP_METRIC_INIT(compose_rsp_metrics);
    }

    if (compose_init) {
        log_warn("%s has already been setup, overwrite", COMPOSE_MODULE_NAME);
    }
    compose_init = true;
}

void
compose_teardown(void)
{
    log_info("tear down the %s module", COMPOSE_MODULE_NAME);

    if (!compose_init) {
        log_warn("%s has never been setup", COMPOSE_MODULE_NAME);
    }
    compose_req_metrics = NULL;
    compose_rsp_metrics = NULL;
    compose_init = false;
}

/*
 * common functions
 */

static inline compose_rstatus_t
_check_buf_size(struct buf **buf, uint32_t n)
{
    while (n > buf_wsize(*buf)) {
        if (dbuf_double(buf) != CC_OK) {
            log_debug("failed to write  %u bytes to buf %p: insufficient "
                    "buffer space", n, *buf);

            return COMPOSE_ENOMEM;
        }
    }

    return CC_OK;
}

static inline int
_write_number(struct buf **buf, int64_t val, char *prefix)
{
    size_t n, len;

    if (_check_buf_size(buf, CC_INT64_MAXLEN + CRLF_LEN) != CC_OK) {
        return COMPOSE_ENOMEM;
    }

    n = buf_write(*buf, prefix, strlen(prefix));
    len = cc_snprintf((*buf)->wpos, CC_INT64_MAXLEN + CRLF_LEN, "%"PRId64"\r\n", val);
    (*buf)->wpos += len;
    n += len;

    return n;
}

static inline int
_write_i64(struct buf **buf, int64_t val)
{
    return _write_number(buf, val, ":");
}

static inline int
_write_length(struct buf **buf, int64_t len)
{
    return _write_number(buf, len, "*");
}

static inline int
_write_bulk(struct buf **buf, const struct bstring *str)
{
    size_t n;

    if (_check_buf_size(buf, 1 + CC_UINT64_MAXLEN + CRLF_LEN + str->len + CRLF_LEN) != CC_OK) {
        return COMPOSE_ENOMEM;
    }

    n = cc_snprintf((*buf)->wpos, 1 + CC_UINT64_MAXLEN + CRLF_LEN, "$%"PRIu32"\r\n", str->len);
    (*buf)->wpos += n;
    n += buf_write(*buf, str->data, str->len);
    n += buf_write(*buf, CRLF, CRLF_LEN);

    return n;
}

static inline int
_write_simple(struct buf **buf, const struct bstring *str, char *prefix)
{
    size_t n;

    if (_check_buf_size(buf, 1 + str->len + CRLF_LEN) != CC_OK) {
        return COMPOSE_ENOMEM;
    }

    n = buf_write(*buf, prefix, strlen(prefix));
    n += buf_write(*buf, str->data, str->len);
    n += buf_write(*buf, CRLF, CRLF_LEN);
    return n;
}

static inline int
_write_string(struct buf **buf, const struct bstring *str)
{
    return _write_simple(buf, str, "+");
}

static inline int
_write_error(struct buf **buf, const struct bstring *str)
{
    return _write_simple(buf, str, "-");
}

/*
 * request specific functions
 */
static inline int
_noreply(struct buf **buf)
{
    return buf_write(*buf, NOREPLY, NOREPLY_LEN);
}

int
compose_req(struct buf **buf, struct request *req)
{
    request_type_t type = req->type;
    char s[CC_UINT64_MAXLEN];
    struct bstring v = { .len = 0, .data = s };
    struct bstring *str = &req_strings[type];
    struct bstring *key = req->keys->data;
    uint32_t i;
    int sz, n = 0;

    switch (type) {
    case REQ_FLUSH:
    case REQ_QUIT:
        if (_check_buf_size(buf, str->len) != COMPOSE_OK) {
            goto error;
        }
        n += _write_bulk(buf, str);
        break;

    case REQ_GET:
    case REQ_MGET:
    case REQ_DELETE:
        // the structure for these commands goes like this:
        // *3\r\n$4\r\nMGET\r\n$3\r\nkey\r\n$4\r\nkey2\r\n
        /* here we may overestimate the size of message header because we
         * estimate the int size based on max value
         */
        for (i = 0, sz = 0; i < array_nelem(req->keys); i++) {
            key = array_get(req->keys, i);
            sz += 1 + CC_UINT64_MAXLEN + key->len + CRLF_LEN;
        }
        if (_check_buf_size(buf,
                    1 + CC_UINT64_MAXLEN + CRLF_LEN +
                    1 + CC_UINT64_MAXLEN + CRLF_LEN +
                    str->len + CRLF_LEN +
                    sz
                    ) != COMPOSE_OK) {
            goto error;
        }
        n += _write_length(buf, 1 + array_nelem(req->keys));
        n += _write_bulk(buf, str);
        for (i = 0; i < array_nelem(req->keys); i++) {
            n += _write_bulk(buf, (struct bstring *)array_get(req->keys, i));
        }
        break;

    case REQ_INCR:
    case REQ_DECR:
        // the structure for this command goes like this:
        // *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
        /* here we may overestimate the size of message header because we
         * estimate the int size based on max value
         */
        if (_check_buf_size(buf,
                    1 + CC_UINT32_MAXLEN + CRLF_LEN +
                    1 + CC_UINT64_MAXLEN + CRLF_LEN +
                    str->len + CRLF_LEN +
                    1 + CC_UINT64_MAXLEN + CRLF_LEN +
                    key->len + CRLF_LEN +
                    1 + CC_UINT64_MAXLEN + CRLF_LEN +
                    1 + CC_UINT64_MAXLEN + CRLF_LEN
                    )
                != COMPOSE_OK) {
            goto error;
        }
        n += _write_length(buf, 2 + array_nelem(req->keys));
        n += _write_bulk(buf, str);
        n += _write_bulk(buf, key);
        v.len = snprintf(s, CC_UINT64_MAXLEN, "%"PRIu64, req->delta);
        n += _write_bulk(buf, &v);
        break;

    case REQ_SET:
        // the structure for this command goes like this:
        // *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
        /* here we may overestimate the size of message header because we
         * estimate the int size based on max value
         */
        if (_check_buf_size(buf,
                    1 + CC_UINT32_MAXLEN + CRLF_LEN +
                    1 + CC_UINT64_MAXLEN + CRLF_LEN +
                    str->len + CRLF_LEN +
                    1 + CC_UINT64_MAXLEN + CRLF_LEN +
                    key->len + CRLF_LEN +
                    1 + CC_UINT64_MAXLEN + CRLF_LEN +
                    req->vstr.len + CRLF_LEN
                    )
                != COMPOSE_OK) {
            goto error;
        }
        n += _write_length(buf, 2 + array_nelem(req->keys));
        n += _write_bulk(buf, str);
        n += _write_bulk(buf, key);
        n += _write_bulk(buf, &req->vstr);
        break;

    default:
        NOT_REACHED();
        break;
    }

    INCR(compose_req_metrics, request_compose);

    return n;

error:
    INCR(compose_req_metrics, request_compose_ex);

    return COMPOSE_ENOMEM;
}

/*
 * response specific functions
 */

int
compose_rsp(struct buf **buf, struct response *rsp)
{
    int n = 0;
    uint32_t vlen;
    response_type_t type = rsp->type;
    struct bstring *str = &rsp_strings[type];

    /**
     * if we check size for each field to write, we end up being more precise.
     * However, it makes the code really cumbersome to read/write. Instead, we
     * can try to estimate the size for each response upfront and over-estimate
     * length of decimal integers. The absolute margin should be under 40 bytes
     * (2x 32-bit flag+vlen, 1x 64-bit cas) when estimate based on max length.
     * This means in a few cases we will be expanding the buffer unnecessarily,
     * or return error when the message can be squeezed in, but that remains a
     * very small chance in the face of reasonably sized buffers.
     *
     * No delimiter is needed right after each command type (the strings are
     * stored with an extra white space), delimiters are required to be inserted
     * for every additional field.
     */

    log_verb("composing rsp into buf %p from rsp object %p", *buf, rsp);

    switch (type) {
    case RSP_OK:
    case RSP_END:
    case RSP_STORED:
    case RSP_EXISTS:
    case RSP_DELETED:
    case RSP_NOT_FOUND:
    case RSP_NOT_STORED:
        if (_check_buf_size(buf, str->len) != COMPOSE_OK) {
            goto error;
        }
        n += _write_bulk(buf, str);
        log_verb("response type %d, total length %d", rsp->type, n);
        break;

    case RSP_CLIENT_ERROR:
    case RSP_SERVER_ERROR:
        if (_check_buf_size(buf, str->len + rsp->vstr.len + CRLF_LEN) !=
                COMPOSE_OK) {
            goto error;
        }
        n += _write_bulk(buf, str);
        n += _write_bulk(buf, &rsp->vstr);
        log_verb("response type %d, total length %d", rsp->type, n);
        break;

    case RSP_NUMERIC:
        /* the **_MAXLEN constants include an extra byte for delimiter */
        if (_check_buf_size(buf, CC_UINT64_MAXLEN + CRLF_LEN) != COMPOSE_OK) {
            goto error;
        }
        n += _write_i64(buf, rsp->vint);
        log_verb("response type %d, total length %d", rsp->type, n);
        break;

    case RSP_VALUE:
        if (rsp->num) {
            vlen = digits(rsp->vint);
        } else {
            vlen = rsp->vstr.len;
        }

        if (_check_buf_size(buf, str->len + rsp->key.len + CC_UINT32_MAXLEN * 2
                    + vlen + CRLF_LEN * 2) != COMPOSE_OK) {
            goto error;
        }
        n += _write_bulk(buf, str);
        n += _write_bulk(buf, &rsp->key);
        n += _write_i64(buf, rsp->flag);
        n += _write_i64(buf, vlen);
        if (rsp->cas) {
            n += _write_i64(buf, rsp->vcas);
        }
        if (rsp->num) {
            n += _write_i64(buf, rsp->vint);
        } else {
            n += _write_bulk(buf, &rsp->vstr);
        }
        log_verb("response type %d, total length %d", rsp->type, n);
        break;

    default:
        NOT_REACHED();
        break;
    }

    INCR(compose_rsp_metrics, response_compose);

    return n;

error:
    INCR(compose_rsp_metrics, response_compose_ex);

    return CC_ENOMEM;
}
