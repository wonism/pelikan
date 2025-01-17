#include <storage/slab/slab.h>

#include <cc_debug.h>

#include <stdlib.h>
#include <stdio.h>

static rel_time_t flush_at = 0;


static inline bool
_item_expired(struct item *it)
{
    return ((it->expire_at > 0 && it->expire_at < time_now())
            || (it->create_at <= flush_at));
}

static inline void
_copy_key(struct item *it, const struct bstring *key)
{
    cc_memcpy(item_key(it), key->data, key->len);
    it->klen = key->len;
}

static inline void
_copy_key_item(struct item *nit, struct item *oit)
{
    cc_memcpy(item_key(nit), item_key(oit), oit->klen);
    nit->klen = oit->klen;
}

static inline void
_copy_val(struct item *it, const struct bstring *val)
{
    cc_memcpy(item_data(it), val->data, val->len);
    it->vlen = val->len;
}

void
item_hdr_init(struct item *it, uint32_t offset, uint8_t id)
{
    ASSERT(offset >= SLAB_HDR_SIZE && offset < slab_size);

#if CC_ASSERT_PANIC == 1 || CC_ASSERT_LOG == 1
    it->magic = ITEM_MAGIC;
#endif
    it->offset = offset;
    it->id = id;
    it->is_linked = it->in_freeq = it->is_raligned = 0;
}

static inline void
_item_reset(struct item *it)
{
    it->is_linked = 0;
    it->in_freeq = 0;
    it->is_raligned = 0;
    it->vlen = 0;
    it->dataflag = 0;
    it->klen = 0;
    it->expire_at = 0;
    it->create_at = 0;
}

/*
 * Allocate an item. We allocate an item by consuming the next free item
 * from slab of the item's slab class.
 *
 * On success we return the pointer to the allocated item.
 */
static item_rstatus_t
_item_alloc(struct item **it_p, uint8_t klen, uint32_t vlen)
{
    uint8_t id = slab_id(item_ntotal(klen, vlen));
    struct item *it;

    log_verb("allocate item with klen %u vlen %u", klen, vlen);

    if (id == SLABCLASS_INVALID_ID) {
        return ITEM_EOVERSIZED;
    }

    it = slab_get_item(id);
    *it_p = it;
    if (it != NULL) {
        _item_reset(it);
        INCR(slab_metrics, item_req);

        log_verb("alloc it %p of id %"PRIu8" at offset %"PRIu32, it, it->id,
                it->offset);

        return ITEM_OK;
    } else {
        INCR(slab_metrics, item_req_ex);
        log_warn("server error on allocating item in slab %"PRIu8, id);

        return ITEM_ENOMEM;
    }
}

/*
 * Link an item into the hash table
 */
static void
_item_link(struct item *it)
{
    ASSERT(it->magic == ITEM_MAGIC);
    ASSERT(!(it->is_linked));
    ASSERT(!(it->in_freeq));

    log_verb("link it %p of id %"PRIu8" at offset %"PRIu32, it, it->id,
            it->offset);

    it->is_linked = 1;

    hashtable_put(it, hash_table);

    INCR(slab_metrics, item_curr);
    INCR(slab_metrics, item_insert);
    INCR_N(slab_metrics, item_keyval_byte, it->klen + it->vlen);
    INCR_N(slab_metrics, item_val_byte, it->vlen);
}

/*
 * Unlinks an item from the hash table.
 */
static void
_item_unlink(struct item *it)
{
    ASSERT(it->magic == ITEM_MAGIC);

    log_verb("unlink it %p of id %"PRIu8" at offset %"PRIu32, it, it->id,
            it->offset);

    if (it->is_linked) {
        it->is_linked = 0;
        hashtable_delete(item_key(it), it->klen, hash_table);
    }
    slab_put_item(it, it->id);

    DECR(slab_metrics, item_curr);
    INCR(slab_metrics, item_remove);
    DECR_N(slab_metrics, item_keyval_byte, it->klen + it->vlen);
    DECR_N(slab_metrics, item_val_byte, it->vlen);
}

/**
 * Return an item if it hasn't been marked as expired, lazily expiring
 * item as-and-when needed
 */
struct item *
item_get(const struct bstring *key)
{
    struct item *it;

    it = hashtable_get(key->data, key->len, hash_table);
    if (it == NULL) {
        log_verb("get it '%.*s' not found", key->len, key->data);
        return NULL;
    }

    log_verb("get it key %.*s val %.*s", key->len, key->data, it->vlen, item_data(it));

    if (_item_expired(it)) {
        _item_unlink(it);
        log_verb("get it '%.*s' expired and nuked", key->len, key->data);
        return NULL;
    }

    log_verb("get it %p of id %"PRIu8, it, it->id);

    return it;
}

item_rstatus_t
item_insert(const struct bstring *key, const struct bstring *val, uint32_t dataflag, rel_time_t expire_at)
{
    item_rstatus_t status;
    struct item *it = NULL;

    if ((status = _item_alloc(&it, key->len, val->len)) != ITEM_OK) {
        return status;
    }

    it->expire_at = expire_at;
    it->create_at = time_now();
    it->dataflag = dataflag;
    _copy_key(it, key);
    _copy_val(it, val);
    item_set_cas(it);
    _item_link(it);

    log_verb("insert it %p of id %"PRIu8" it->klen: %d dataflag %u", it, it->id, it->klen, it->dataflag);

    return ITEM_OK;
}

item_rstatus_t
item_annex(struct item *oit, const struct bstring *val, bool append)
{
    item_rstatus_t status = ITEM_OK;
    struct item *nit = NULL;
    uint8_t id;
    uint32_t ntotal = oit->vlen + val->len;

    id = item_slabid(oit->klen, ntotal);
    if (id == SLABCLASS_INVALID_ID) {
        log_info("client error: annex operation results in oversized item with"
                   "key size %"PRIu8" old value size %"PRIu32" and new value "
                   "size %"PRIu32, oit->klen, oit->vlen, ntotal);

        return ITEM_EOVERSIZED;
    }

    if (append) {
        /* if it is large enough to hold the extra data and left-aligned,
         * which is the default behavior, we copy the delta to the end of
         * the existing data. Otherwise, allocate a new item and store the
         * payload left-aligned.
         */
        if (id == oit->id && !(oit->is_raligned)) {
            cc_memcpy(item_data(oit) + oit->vlen, val->data, val->len);
            oit->vlen = ntotal;
            INCR_N(slab_metrics, item_keyval_byte, val->len);
            INCR_N(slab_metrics, item_val_byte, val->len);
            item_set_cas(oit);
        } else {
            status = _item_alloc(&nit, oit->klen, ntotal);
            if (status != ITEM_OK) {
                log_debug("annex failed due to failure to allocate new item");
                return status;
            }
            _copy_key_item(nit, oit);
            nit->expire_at = oit->expire_at;
            nit->create_at = time_now();
            nit->dataflag = oit->dataflag;
            item_set_cas(nit);
            /* value is left-aligned */
            cc_memcpy(item_data(nit), item_data(oit), oit->vlen);
            cc_memcpy(item_data(nit) + oit->vlen, val->data, val->len);
            nit->vlen = ntotal;
            _item_unlink(oit);
            _item_link(nit);
        }
    } else {
        /* if oit is large enough to hold the extra data and is already
         * right-aligned, we copy the delta to the front of the existing
         * data. Otherwise, allocate a new item and store the payload
         * right-aligned, assuming more prepends will happen in the future.
         */
        if (id == oit->id && oit->is_raligned) {
            cc_memcpy(item_data(oit) - val->len, val->data, val->len);
            oit->vlen = ntotal;
            INCR_N(slab_metrics, item_keyval_byte, val->len);
            INCR_N(slab_metrics, item_val_byte, val->len);
            item_set_cas(oit);
        } else {
            status = _item_alloc(&nit, oit->klen, ntotal);
            if (status != ITEM_OK) {
                log_debug("annex failed due to failure to allocate new item");
                return status;
            }
            _copy_key_item(nit, oit);
            nit->expire_at = oit->expire_at;
            nit->create_at = time_now();
            nit->dataflag = oit->dataflag;
            item_set_cas(nit);
            /* value is right-aligned */
            nit->is_raligned = 1;
            cc_memcpy(item_data(nit) - ntotal, val->data, val->len);
            cc_memcpy(item_data(nit) - oit->vlen, item_data(oit), oit->vlen);
            nit->vlen = ntotal;
            _item_unlink(oit);
            _item_link(nit);
        }
    }

    log_verb("annex to it %p of id %"PRIu8", new it at %p", oit, oit->id,
            nit ? oit : nit);

    return status;
}

item_rstatus_t
item_update(struct item *it, const struct bstring *val)
{
    ASSERT(item_slabid(it->klen, val->len) == it->id);

    it->vlen = val->len;
    cc_memcpy(item_data(it), val->data, val->len);
    item_set_cas(it);

    log_verb("update it %p of id %"PRIu8, it, it->id);

    return ITEM_OK;
}

bool
item_delete(const struct bstring *key)
{
    struct item *it;

    it = item_get(key);
    if (it != NULL) {
        _item_unlink(it);
        return true;
    } else {
        return false;
    }
}

void
item_flush(void)
{
    time_update();
    flush_at = time_now();
    log_info("all keys flushed at %"PRIu32, flush_at);
}
