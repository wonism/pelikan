#include <protocol/redis_include.h>

#include <buffer/cc_buf.h>
#include <cc_array.h>
#include <cc_bstring.h>
#include <cc_define.h>

#include <check.h>

#include <stdio.h>
#include <stdlib.h>

/* define for each suite, local scope due to macro visibility rule */
#define SUITE_NAME "redis"
#define DEBUG_LOG  SUITE_NAME ".log"

struct request *req;
struct response *rsp;
struct buf *buf;

/*
 * utilities
 */
static void
test_setup(void)
{
    req = request_create();
    rsp = response_create();
    buf = buf_create();
}

static void
test_reset(void)
{
    request_reset(req);
    response_reset(rsp);
    buf_reset(buf);
}

static void
test_teardown(void)
{
    buf_destroy(&buf);
    response_destroy(&rsp);
    request_destroy(&req);
    buf_teardown();
}

/**************
 * test cases *
 **************/

/*
 * basic requests
 */
START_TEST(test_quit)
{
#define SERIALIZED "$4\r\nquit\r\n"

    int ret;
    int len = sizeof(SERIALIZED) - 1;

    test_reset();

    /* compose */
    req->type = REQ_QUIT;
    ret = compose_req(&buf, req);
    ck_assert_msg(ret == len, "expected: %d, returned: %d", len, ret);
    ck_assert_int_eq(cc_bcmp(buf->rpos, SERIALIZED, ret), 0);

    /* parse */
    request_reset(req);
    ret = parse_req(req, buf);
    ck_assert_int_eq(ret, PARSE_OK);
    ck_assert(req->rstate == REQ_PARSED);
    ck_assert(req->type == REQ_QUIT);
    ck_assert(buf->rpos == buf->wpos);
#undef SERIALIZED
}
END_TEST

START_TEST(test_delete)
{
#define SERIALIZED "*2\r\n$6\r\ndelete\r\n$3\r\nfoo\r\n"
#define KEY "foo"

    int ret;
    int len = sizeof(SERIALIZED) - 1;
    struct bstring key = str2bstr(KEY);
    struct bstring *pos;

    test_reset();

    /* compose */
    req->type = REQ_DELETE;
    pos = array_push(req->keys);
    *pos = key;
    ret = compose_req(&buf, req);
    ck_assert_msg(ret == len, "expected: %d, returned: %d", len, ret);
    ck_assert_int_eq(cc_bcmp(buf->rpos, SERIALIZED, ret), 0);

    /* parse */
    request_reset(req);
    ret = parse_req(req, buf);
    ck_assert_int_eq(ret, PARSE_OK);
    ck_assert(req->rstate == REQ_PARSED);
    ck_assert(req->type == REQ_DELETE);
    ck_assert_int_eq(array_nelem(req->keys), 1);
    ck_assert_int_eq(bstring_compare(&key, array_first(req->keys)), 0);
    ck_assert(buf->rpos == buf->wpos);
#undef KEY
#undef SERIALIZED
}
END_TEST

START_TEST(test_get)
{
#define SERIALIZED "*2\r\n$3\r\nget\r\n$3\r\nfoo\r\n"
#define KEY "foo"

    int ret;
    int len = sizeof(SERIALIZED) - 1;
    struct bstring key = str2bstr(KEY);
    struct bstring *pos;

    test_reset();

    /* compose */
    req->type = REQ_GET;
    pos = array_push(req->keys);
    *pos = key;
    ret = compose_req(&buf, req);
    ck_assert_msg(ret == len, "expected: %d, returned: %d", len, ret);
    ck_assert_int_eq(cc_bcmp(buf->rpos, SERIALIZED, ret), 0);

    /* parse */
    request_reset(req);
    ret = parse_req(req, buf);
    ck_assert_int_eq(ret, PARSE_OK);
    ck_assert(req->rstate == REQ_PARSED);
    ck_assert(req->type == REQ_GET);
    ck_assert_int_eq(array_nelem(req->keys), 1);
    ck_assert_int_eq(bstring_compare(&key, array_first(req->keys)), 0);
    ck_assert(buf->rpos == buf->wpos);
#undef KEY
#undef SERIALIZED
}
END_TEST

START_TEST(test_mget)
{
#define SERIALIZED "*2\r\n$4\r\nmget\r\n$3\r\nfoo\r\n"
#define KEY "foo"

    int ret;
    int len = sizeof(SERIALIZED) - 1;
    struct bstring key = str2bstr(KEY);
    struct bstring *pos;

    test_reset();

    /* compose */
    req->type = REQ_MGET;
    pos = array_push(req->keys);
    *pos = key;
    ret = compose_req(&buf, req);
    ck_assert_msg(ret == len, "expected: %d, returned: %d", len, ret);
    ck_assert_int_eq(cc_bcmp(buf->rpos, SERIALIZED, ret), 0);

    /* parse */
    request_reset(req);
    ret = parse_req(req, buf);
    ck_assert_int_eq(ret, PARSE_OK);
    ck_assert(req->rstate == REQ_PARSED);
    ck_assert(req->type == REQ_MGET);
    ck_assert_int_eq(array_nelem(req->keys), 1);
    ck_assert_int_eq(bstring_compare(&key, array_first(req->keys)), 0);
    ck_assert(buf->rpos == buf->wpos);
#undef KEY
#undef SERIALIZED
}
END_TEST

START_TEST(test_set)
{
#define SERIALIZED "*3\r\n$3\r\nset\r\n$3\r\nfoo\r\n$3\r\nXYZ\r\n"
#define KEY "foo"
#define VAL "XYZ"

    int ret;
    int len = sizeof(SERIALIZED) - 1;
    struct bstring key = str2bstr(KEY);
    struct bstring val = str2bstr(VAL);
    struct bstring *pos;

    test_reset();

    /* compose */
    req->type = REQ_SET;
    pos = array_push(req->keys);
    *pos = key;
    req->vstr = val;
    ret = compose_req(&buf, req);
    ck_assert_msg(ret == len, "expected: %d, returned: %d", len, ret);
    ck_assert_int_eq(cc_bcmp(buf->rpos, SERIALIZED, ret), 0);

    /* parse */
    request_reset(req);
    ret = parse_req(req, buf);
    ck_assert_int_eq(ret, PARSE_OK);
    ck_assert(req->rstate == REQ_PARSED);
    ck_assert(req->type == REQ_SET);
    ck_assert_int_eq(array_nelem(req->keys), 1);
    ck_assert_int_eq(bstring_compare(&key, array_first(req->keys)), 0);
    ck_assert_int_eq(bstring_compare(&val, &req->vstr), 0);
    ck_assert(buf->rpos == buf->wpos);
#undef VAL
#undef KEY
#undef SERIALIZED
}
END_TEST

START_TEST(test_incr)
{
#define SERIALIZED "*3\r\n$6\r\nincrby\r\n$3\r\nfoo\r\n$3\r\n909\r\n"
#define KEY "foo"
#define DELTA 909

    int ret;
    int len = sizeof(SERIALIZED) - 1;
    struct bstring key = str2bstr(KEY);
    struct bstring *pos;

    test_reset();

    /* compose */
    req->type = REQ_INCR;
    pos = array_push(req->keys);
    *pos = key;
    req->delta = DELTA;
    ret = compose_req(&buf, req);
    ck_assert_msg(ret == len, "expected: %d, returned: %d", len, ret);
    ck_assert_int_eq(cc_bcmp(buf->rpos, SERIALIZED, ret), 0);

    /* parse */
    request_reset(req);
    ret = parse_req(req, buf);
    ck_assert_int_eq(ret, PARSE_OK);
    ck_assert(req->rstate == REQ_PARSED);
    ck_assert(req->type == REQ_INCR);
    ck_assert_int_eq(array_nelem(req->keys), 1);
    ck_assert_int_eq(bstring_compare(&key, array_first(req->keys)), 0);
    ck_assert_int_eq(req->delta, DELTA);
    ck_assert(buf->rpos == buf->wpos);
#undef KEY
#undef SERIALIZED
}
END_TEST

START_TEST(test_decr)
{
#define SERIALIZED "*3\r\n$6\r\ndecrby\r\n$3\r\nfoo\r\n$3\r\n909\r\n"
#define KEY "foo"
#define DELTA 909

    int ret;
    int len = sizeof(SERIALIZED) - 1;
    struct bstring key = str2bstr(KEY);
    struct bstring *pos;

    test_reset();

    /* compose */
    req->type = REQ_DECR;
    pos = array_push(req->keys);
    *pos = key;
    req->delta = DELTA;
    req->noreply = 1;
    ret = compose_req(&buf, req);
    ck_assert_msg(ret == len, "expected: %d, returned: %d", len, ret);
    ck_assert_int_eq(cc_bcmp(buf->rpos, SERIALIZED, ret), 0);

    /* parse */
    request_reset(req);
    ret = parse_req(req, buf);
    ck_assert_int_eq(ret, PARSE_OK);
    ck_assert(req->rstate == REQ_PARSED);
    ck_assert(req->type == REQ_DECR);
    ck_assert_int_eq(array_nelem(req->keys), 1);
    ck_assert_int_eq(bstring_compare(&key, array_first(req->keys)), 0);
    ck_assert_int_eq(req->delta, DELTA);
    ck_assert_int_eq(req->noreply, 1);
    ck_assert(buf->rpos == buf->wpos);
#undef KEY
#undef SERIALIZED
}
END_TEST

/*
 * test suite
 */
static Suite *
redis_suite(void)
{
    Suite *s = suite_create(SUITE_NAME);

    /* basic requests */
    TCase *tc_basic_req = tcase_create("basic request");
    suite_add_tcase(s, tc_basic_req);

    tcase_add_test(tc_basic_req, test_quit);
    tcase_add_test(tc_basic_req, test_get);
    tcase_add_test(tc_basic_req, test_mget);
    tcase_add_test(tc_basic_req, test_delete);
    tcase_add_test(tc_basic_req, test_incr);
    tcase_add_test(tc_basic_req, test_decr);
    tcase_add_test(tc_basic_req, test_set);

    return s;
}

int
main(void)
{
    int nfail;

    /* setup */
    test_setup();

    Suite *suite = redis_suite();
    SRunner *srunner = srunner_create(suite);
    srunner_set_log(srunner, DEBUG_LOG);
    srunner_run_all(srunner, CK_ENV); /* set CK_VEBOSITY in ENV to customize */
    nfail = srunner_ntests_failed(srunner);
    srunner_free(srunner);

    /* teardown */
    test_teardown();

    return (nfail == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}
