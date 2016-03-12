#include <protocol/data/redis/parse.h>

#include <protocol/data/redis/request.h>
#include <protocol/data/redis/response.h>
#include <time/time.h>

#include <buffer/cc_buf.h>
#include <cc_array.h>
#include <cc_debug.h>
#include <cc_define.h>
#include <cc_print.h>
#include <cc_util.h>

#include <ctype.h>

#define PARSE_MODULE_NAME "protocol::redis::parse"

static bool parse_init = false;
static parse_req_metrics_st *parse_req_metrics = NULL;
static parse_rsp_metrics_st *parse_rsp_metrics = NULL;

void
parse_setup(parse_req_metrics_st *req, parse_rsp_metrics_st *rsp)
{
    log_info("set up the %s module", PARSE_MODULE_NAME);

    parse_req_metrics = req;
    if (parse_req_metrics != NULL) {
        PARSE_REQ_METRIC_INIT(parse_req_metrics);
    }
    parse_rsp_metrics = rsp;
    if (parse_rsp_metrics != NULL) {
        PARSE_RSP_METRIC_INIT(parse_rsp_metrics);
    }

    if (parse_init) {
        log_warn("%s has already been setup, overwrite", PARSE_MODULE_NAME);
    }
    parse_init = true;
}

void
parse_teardown(void)
{
    log_info("tear down the %s module", PARSE_MODULE_NAME);

    if (!parse_init) {
        log_warn("%s has never been setup", PARSE_MODULE_NAME);
    }
    parse_req_metrics = NULL;
    parse_rsp_metrics = NULL;
    parse_init = false;
}

/*
 * common functions
 */
/* CRLF is special and we need to "peek into the future" */
static inline parse_rstatus_t
_try_crlf(struct buf *buf, char *p)
{
    if (*p != CR) {
        return PARSE_EINVALID;
    }

    if (buf->wpos == p + 1) { /* the next byte hasn't been received */
        return PARSE_EUNFIN;
    }

    if (*(p + 1) == LF) {
        return PARSE_OK;
    } else {
        return PARSE_EINVALID;
    }
}

static inline parse_rstatus_t
_check_uint(uint64_t *num, struct buf *buf, uint64_t max)
{
    char *p = buf->rpos;
    *num = 0;

    while (isdigit(*p)) {
        if (buf_rsize(buf) == 0) {
            return PARSE_EUNFIN;
        }

        if (*num > max / 10) {
            /* TODO(yao): catch the few numbers that will still overflow */
            log_warn("ill formatted request: integer too big");

            return PARSE_EINVALID;
        }
        *num = *num * 10ULL;
        *num += (uint64_t)(*p - '0');

        p++;
    }

    if (p == buf->rpos) {
        log_warn("ill formatted request: no integer provided");

        return PARSE_EEMPTY;
    }

    if (_try_crlf(buf, p) != PARSE_OK) {
        log_warn("ill formatted request: non-digit char in integer field");

        return PARSE_EINVALID;
    }

    buf->rpos = (p + CRLF_LEN);

    return PARSE_OK;
}

static inline parse_rstatus_t
_parse_bulk(struct buf *buf, struct bstring *t)
{
    parse_rstatus_t status;
    uint64_t bulklen;

    if (*buf->rpos != '$') {
        return PARSE_EINVALID;
    }

    if (buf->wpos == buf->rpos + 1) { /* the next byte hasn't been received */
        return PARSE_EUNFIN;
    }

    buf->rpos++;

    status = _check_uint(&bulklen, buf, UINT64_MAX);
    if (status != PARSE_OK) {
        return status;
    }

    if (buf_rsize(buf) < bulklen + CRLF_LEN) {
        return PARSE_EUNFIN;
    }
    bstring_init(t);
    t->len = bulklen;
    t->data = buf->rpos;
    buf->rpos = t->data + t->len + CRLF_LEN;
    return PARSE_OK;
}

static inline parse_rstatus_t
_parse_bulk_numeric(struct buf *buf, uint64_t *num, uint64_t max)
{
    parse_rstatus_t status;
    struct bstring s;
    uint32_t i;
    *num = 0;

    bstring_init(&s);
    status = _parse_bulk(buf, &s);
    if (status == PARSE_OK) {
        if (s.len > CC_UINT64_MAXLEN) {
            return PARSE_EINVALID;
        }
        for (i = 0; i < s.len; i++) {
            if (!isdigit(s.data[i])) {
                return PARSE_EINVALID;
            }
            if (*num > max / 10) {
                /* TODO(yao): catch the few numbers that will still overflow */
                log_warn("ill formatted request: integer too big");
                return PARSE_EINVALID;
            }
            *num = *num * 10ULL;
            *num += (uint64_t)(s.data[i] - '0');
        }
    }
    return status;
}

/*
 * request specific functions
 */

static inline parse_rstatus_t
_check_req_type(struct request *req, struct buf *buf)
{
    parse_rstatus_t status;
    struct bstring t;

    bstring_init(&t);

    status = _parse_bulk(buf, &t);
    if (status != PARSE_OK)  {
        return status;
    }
    switch (t.len) {
    case 3:
        if (str3cmp(t.data, 'g', 'e', 't')) {
            req->type = REQ_GET;
            break;
        }

        if (str3cmp(t.data, 's', 'e', 't')) {
            req->type = REQ_SET;
            break;
        }

        break;

    case 4:
        if (str4cmp(t.data, 'm', 'g', 'e', 't')) {
            req->type = REQ_MGET;
            break;
        }

        if (str4cmp(t.data, 'q', 'u', 'i', 't')) {
            req->type = REQ_QUIT;
            break;
        }

        break;

    case 5:
        if (str5cmp(t.data, 'f', 'l', 'u', 's', 'h')) {
            req->type = REQ_FLUSH;
            break;
        }

        break;

    case 6:
        if (str6cmp(t.data, 'd', 'e', 'l', 'e', 't', 'e')) {
            req->type = REQ_DELETE;
            break;
        }

        if (str6cmp(t.data, 'i', 'n', 'c', 'r', 'b', 'y')) {
            req->type = REQ_INCR;
            break;
        }

        if (str6cmp(t.data, 'd', 'e', 'c', 'r', 'b', 'y')) {
            req->type = REQ_DECR;
            break;
        }

        break;
    }

    if (req->type == REQ_UNKNOWN) { /* no match */
        log_warn("ill formatted request: unknown command");

        return PARSE_EINVALID;
    } else {
        return PARSE_OK;
    }
}

static inline parse_rstatus_t
_push_key(struct request *req, struct bstring *t)
{
    struct bstring *k;

    if (array_nelem(req->keys) >= MAX_BATCH_SIZE) {
          log_warn("ill formatted request: too many keys in a batch");

          return PARSE_EOTHER;
      }

      /* push should never fail as keys are preallocated for MAX_BATCH_SIZE */
      k = array_push(req->keys);
      *k = *t;

      return PARSE_OK;
}


static inline parse_rstatus_t
_check_noreply(struct buf *buf, struct bstring *t, char *p)
{
    bool complete;

    if (*p == ' ' && t->len == 0) { /* pre-key spaces */
        return PARSE_EUNFIN;
    }

    complete = _try_crlf(buf, p);
    if (complete) {
        buf->rpos = p + CRLF_LEN;

        if (t->len == 0) {
            return PARSE_EEMPTY;
        }

        if (t->len == 7 && str7cmp(t->data, 'n', 'o', 'r', 'e', 'p', 'l', 'y')) {
            return PARSE_OK;
        }

        return PARSE_EINVALID;
    }

    return PARSE_EUNFIN;
}

static parse_rstatus_t
_subrequest_delete(struct request *req, struct buf *buf)
{
    parse_rstatus_t status;
    struct bstring t;

    /* parsing order:
     *   KEY
     *   NOREPLY, optional
     */

    bstring_init(&t);
    /* KEY */
    status = _parse_bulk(buf, &t);
    if (status == PARSE_OK) {
        status = _push_key(req, &t);
    }
    return status;
}

static parse_rstatus_t
_subrequest_arithmetic(struct request *req, struct buf *buf)
{
    parse_rstatus_t status;
    uint64_t delta;
    struct bstring t;

    /* parsing order:
     *   KEY
     *   DELTA,
     *   NOREPLY, optional
     */

    bstring_init(&t);
    /* KEY */
    status = _parse_bulk(buf, &t);
    if (status == PARSE_OK) {
        status = _push_key(req, &t);
    }
    if (status != PARSE_OK) {
        return status;
    }

    delta = 0;
    status = _parse_bulk_numeric(buf, &delta, UINT64_MAX);
    if (status == PARSE_OK) {
        req->delta = delta;
    }
    return status;
}


static parse_rstatus_t
_subrequest_retrieve(struct request *req, struct buf *buf)
{
    parse_rstatus_t status;
    struct bstring t;

    while (true) {
        bstring_init(&t);
        status = _parse_bulk(buf, &t);
        if (status == PARSE_OK) {
            status = _push_key(req, &t);
        } else if (status == PARSE_EEMPTY) {
            if (array_nelem(req->keys) == 0) {
                log_warn("ill formatted request: missing field(s) in retrieve "
                        "command");

                return PARSE_EOTHER;
            } else {
                return PARSE_OK;
            }
        }
        if (status != PARSE_OK) {
            return status;
        }
    }
}

/* parse the first line("header") according to redis ASCII protocol */
static parse_rstatus_t
_parse_req_hdr(struct request *req, struct buf *buf)
{
    parse_rstatus_t status;
    char *old_rpos = buf->rpos;

    ASSERT(req != NULL);
    ASSERT(buf != NULL);
    ASSERT(req->rstate == REQ_PARSING);
    ASSERT(req->pstate == REQ_HDR);

    log_verb("parsing hdr at %p into req %p", buf->rpos, req);

    /* get the verb first */
    status = _check_req_type(req, buf);
    if (status != PARSE_OK) {
        return status;
    }

    /* rest of the request header */
    switch (req->type) {
    case REQ_GET:
    case REQ_MGET:
        status = _subrequest_retrieve(req, buf);
        break;

    case REQ_DELETE:
        status = _subrequest_delete(req, buf);
        break;

    case REQ_INCR:
    case REQ_DECR:
        status = _subrequest_arithmetic(req, buf);
        break;

    /* flush_all can take a delay e.g. 'flush_all 10\r\n', not implemented */
    case REQ_FLUSH:
    case REQ_QUIT:
        break;

    default:
        NOT_REACHED();
        return PARSE_EOTHER;
    }

    if (status != PARSE_OK) {
        buf->rpos = old_rpos; /* reset rpos */

        return status;
    }

    return status;
}

parse_rstatus_t
parse_req(struct request *req, struct buf *buf)
{
    parse_rstatus_t status = PARSE_EUNFIN;
    struct bstring s;

    ASSERT(req->rstate == REQ_PARSING);

    log_verb("parsing buf %p into req %p", buf, req);

    if (req->pstate == REQ_HDR) {
        status = _parse_req_hdr(req, buf);
        if (status != PARSE_OK) {
            goto done;
        }
        if (req->val) {
            req->pstate = REQ_VAL;
        }
    }

    if (req->pstate == REQ_VAL) {
        bstring_init(&s);
        status = _parse_bulk(buf, &s);
        req->vstr = s;
        req->vlen = s.len;
        if (status != PARSE_OK) {
            goto done;
        }
    }

    req->rstate = REQ_PARSED;
    INCR(parse_req_metrics, request_parse);

done:
    if (status != PARSE_OK && status != PARSE_EUNFIN) {
        log_debug("parse req returned error state %d", status);
        req->cerror = 1;
        INCR(parse_req_metrics, request_parse_ex);
    }

    return status;
}


/*
 * response specific functions
 */

static inline parse_rstatus_t
_check_rsp_type(struct response *rsp, struct buf *buf)
{
    return PARSE_EOTHER;
}

static parse_rstatus_t
_subresponse_stat(struct response *rsp, struct buf *buf)
{
    return PARSE_EOTHER;
}

static parse_rstatus_t
_subresponse_value(struct response *rsp, struct buf *buf)
{
    return PARSE_EOTHER;
}

static parse_rstatus_t
_subresponse_error(struct response *rsp, struct buf *buf)
{
    return PARSE_EOTHER;
}


static parse_rstatus_t
_parse_rsp_hdr(struct response *rsp, struct buf *buf)
{
    parse_rstatus_t status;
    char *old_rpos = buf->rpos;

    ASSERT(rsp != NULL);
    ASSERT(buf != NULL);
    ASSERT(rsp->rstate == RSP_PARSING);
    ASSERT(rsp->pstate == RSP_HDR);

    log_verb("parsing hdr at %p into rsp %p", buf->rpos, rsp);

    /* get the type first */
    status = _check_rsp_type(rsp, buf);
    if (status != PARSE_OK) {
        return status;
    }

    /* rest of the response (first line) */
    switch (rsp->type) {
    case RSP_STAT:
        status = _subresponse_stat(rsp, buf);
        break;

    case RSP_VALUE:
        rsp->val = 1;
        status = _subresponse_value(rsp, buf);
        break;

    case RSP_CLIENT_ERROR:
    case RSP_SERVER_ERROR:
        status = _subresponse_error(rsp, buf);
        break;

    case RSP_OK:
    case RSP_END:
    case RSP_EXISTS:
    case RSP_STORED:
    case RSP_DELETED:
    case RSP_NOT_FOUND:
    case RSP_NOT_STORED:
    case RSP_NUMERIC:
        break;

    default:
        NOT_REACHED();
        return PARSE_EOTHER;
    }

    if (status != PARSE_OK) {
        buf->rpos = old_rpos; /* reset rpos */

        return status;
    }

    return status;
}

parse_rstatus_t
parse_rsp(struct response *rsp, struct buf *buf)
{
    parse_rstatus_t status = PARSE_EUNFIN;
    struct bstring s;

    ASSERT(rsp->rstate == RSP_PARSING);

    log_verb("parsing buf %p into rsp %p", buf, rsp);

    if (rsp->pstate == RSP_HDR) {
        status = _parse_rsp_hdr(rsp, buf);
        if (status != PARSE_OK) {
            goto done;
        }
        if (rsp->val) {
            rsp->pstate = RSP_VAL;
        }
    }

    if (rsp->pstate == RSP_VAL) {
        bstring_init(&s);
        status = _parse_bulk(buf, &s);
        rsp->vstr = s;
        rsp->vlen = s.len;
        if (status != PARSE_OK) {
            goto done;
        }
    }

    rsp->rstate = RSP_PARSED;
    INCR(parse_rsp_metrics, response_parse);

done:
    if (status != PARSE_OK && status != PARSE_EUNFIN) {
        rsp->error = 1;
        INCR(parse_rsp_metrics, response_parse_ex);
    }

    return status;
}
