/* -*- Mode: C; tab-width: 4 -*- */
/*
  +----------------------------------------------------------------------+
  | PHP Version 5                                                        |
  +----------------------------------------------------------------------+
  | Copyright (c) 1997-2009 The PHP Group                                |
  +----------------------------------------------------------------------+
  | This source file is subject to version 3.01 of the PHP license,      |
  | that is bundled with this package in the file LICENSE, and is        |
  | available through the world-wide-web at the following url:           |
  | http://www.php.net/license/3_01.txt                                  |
  | If you did not receive a copy of the PHP license and are unable to   |
  | obtain it through the world-wide-web, please send a note to          |
  | license@php.net so we can mail you a copy immediately.               |
  +----------------------------------------------------------------------+
  | Original author: Alfonso Jimenez <yo@alfonsojimenez.com>             |
  | Maintainer: Nicolas Favre-Felix <n.favre-felix@owlient.eu>           |
  | Maintainer: Nasreddine Bouafif <n.bouafif@owlient.eu>                |
  | Maintainer: Michael Grunder <michael.grunder@gmail.com>              |
  +----------------------------------------------------------------------+
*/

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "common.h"
#include "ext/standard/info.h"
#include "php_ini.h"
#include "php_redis.h"
#include "redis_array.h"
#include <zend_exceptions.h>

#ifdef PHP_SESSION
#include "ext/session/php_session.h"
#endif

#include <ext/standard/php_smart_string.h>
#include <ext/standard/php_var.h>
#include <ext/standard/php_math.h>

#include "library.h"

#define R_SUB_CALLBACK_CLASS_TYPE 1
#define R_SUB_CALLBACK_FT_TYPE 2
#define R_SUB_CLOSURE_TYPE 3

int le_redis_sock;
extern int le_redis_array;

#ifdef PHP_SESSION
extern ps_module ps_mod_redis;
#endif

extern zend_class_entry *redis_array_ce;
zend_class_entry *redis_ce;
zend_class_entry *redis_exception_ce;
zend_class_entry *spl_ce_RuntimeException = NULL;

//extern zend_function_entry redis_array_functions[];

PHP_INI_BEGIN()
	/* redis arrays */
	PHP_INI_ENTRY("redis.arrays.names", "", PHP_INI_ALL, NULL)
	PHP_INI_ENTRY("redis.arrays.hosts", "", PHP_INI_ALL, NULL)
	PHP_INI_ENTRY("redis.arrays.previous", "", PHP_INI_ALL, NULL)
	PHP_INI_ENTRY("redis.arrays.functions", "", PHP_INI_ALL, NULL)
	PHP_INI_ENTRY("redis.arrays.index", "", PHP_INI_ALL, NULL)
	PHP_INI_ENTRY("redis.arrays.autorehash", "", PHP_INI_ALL, NULL)
PHP_INI_END()

/**
 * Argument info for the SCAN proper
 */
ZEND_BEGIN_ARG_INFO_EX(arginfo_scan, 0, 0, 1)
    ZEND_ARG_INFO(1, i_iterator)
    ZEND_ARG_INFO(0, str_pattern)
    ZEND_ARG_INFO(0, i_count)
ZEND_END_ARG_INFO();

/**
 * Argument info for key scanning
 */
ZEND_BEGIN_ARG_INFO_EX(arginfo_kscan, 0, 0, 2)
    ZEND_ARG_INFO(0, str_key)
    ZEND_ARG_INFO(1, i_iterator)
    ZEND_ARG_INFO(0, str_pattern)
    ZEND_ARG_INFO(0, i_count)
ZEND_END_ARG_INFO();

#ifdef ZTS
ZEND_DECLARE_MODULE_GLOBALS(redis)
#endif

static zend_function_entry redis_functions[] = {
     PHP_ME(Redis, __construct, NULL, ZEND_ACC_CTOR | ZEND_ACC_PUBLIC)
     PHP_ME(Redis, __destruct, NULL, ZEND_ACC_DTOR | ZEND_ACC_PUBLIC)
     PHP_ME(Redis, connect, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, pconnect, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, close, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, ping, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, echo, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, get, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, set, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, setex, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, psetex, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, setnx, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, getSet, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, randomKey, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, renameKey, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, renameNx, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, getMultiple, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, exists, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, delete, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, incr, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, incrBy, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, incrByFloat, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, decr, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, decrBy, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, type, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, append, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, getRange, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, setRange, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, getBit, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, setBit, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, strlen, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, getKeys, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, sort, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, sortAsc, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, sortAscAlpha, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, sortDesc, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, sortDescAlpha, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, lPush, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, rPush, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, lPushx, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, rPushx, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, lPop, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, rPop, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, blPop, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, brPop, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, lSize, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, lRemove, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, listTrim, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, lGet, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, lGetRange, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, lSet, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, lInsert, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, sAdd, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, sSize, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, sRemove, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, sMove, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, sPop, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, sRandMember, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, sContains, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, sMembers, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, sInter, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, sInterStore, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, sUnion, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, sUnionStore, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, sDiff, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, sDiffStore, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, setTimeout, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, save, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, bgSave, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, lastSave, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, flushDB, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, flushAll, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, dbSize, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, auth, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, ttl, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, pttl, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, persist, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, info, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, resetStat, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, select, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, move, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, bgrewriteaof, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, slaveof, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, object, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, bitop, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, bitcount, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, bitpos, NULL, ZEND_ACC_PUBLIC)

     /* 1.1 */
     PHP_ME(Redis, mset, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, msetnx, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, rpoplpush, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, brpoplpush, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, zAdd, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, zDelete, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, zRange, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, zReverseRange, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, zRangeByScore, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, zRevRangeByScore, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, zRangeByLex, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, zCount, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, zDeleteRangeByScore, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, zDeleteRangeByRank, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, zCard, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, zScore, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, zRank, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, zRevRank, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, zInter, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, zUnion, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, zIncrBy, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, expireAt, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, pexpire, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, pexpireAt, NULL, ZEND_ACC_PUBLIC)

     /* 1.2 */
     PHP_ME(Redis, hGet, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, hSet, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, hSetNx, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, hDel, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, hLen, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, hKeys, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, hVals, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, hGetAll, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, hExists, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, hIncrBy, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, hIncrByFloat, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, hMset, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, hMget, NULL, ZEND_ACC_PUBLIC)

     PHP_ME(Redis, multi, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, discard, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, exec, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, pipeline, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, watch, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, unwatch, NULL, ZEND_ACC_PUBLIC)

	 PHP_ME(Redis, publish, NULL, ZEND_ACC_PUBLIC)
	 PHP_ME(Redis, subscribe, NULL, ZEND_ACC_PUBLIC)
	 PHP_ME(Redis, psubscribe, NULL, ZEND_ACC_PUBLIC)
	 PHP_ME(Redis, unsubscribe, NULL, ZEND_ACC_PUBLIC)
	 PHP_ME(Redis, punsubscribe, NULL, ZEND_ACC_PUBLIC)

	 PHP_ME(Redis, time, NULL, ZEND_ACC_PUBLIC)

     PHP_ME(Redis, eval, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, evalsha, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, script, NULL, ZEND_ACC_PUBLIC)

     PHP_ME(Redis, debug, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, dump, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, restore, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, migrate, NULL, ZEND_ACC_PUBLIC)

     PHP_ME(Redis, getLastError, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, clearLastError, NULL, ZEND_ACC_PUBLIC)

     PHP_ME(Redis, _prefix, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, _serialize, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, _unserialize, NULL, ZEND_ACC_PUBLIC)

     PHP_ME(Redis, client, NULL, ZEND_ACC_PUBLIC)

     /* SCAN and friends */
     PHP_ME(Redis, scan, arginfo_scan, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, hscan, arginfo_kscan, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, zscan, arginfo_kscan, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, sscan, arginfo_kscan, ZEND_ACC_PUBLIC)

     /* HyperLogLog commands */
     PHP_ME(Redis, pfadd, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, pfcount, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, pfmerge, NULL, ZEND_ACC_PUBLIC)

     /* options */
     PHP_ME(Redis, getOption, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, setOption, NULL, ZEND_ACC_PUBLIC)

     /* config */
     PHP_ME(Redis, config, NULL, ZEND_ACC_PUBLIC)

     /* slowlog */
     PHP_ME(Redis, slowlog, NULL, ZEND_ACC_PUBLIC)

     /* Send a raw command and read raw results */
     PHP_ME(Redis, rawCommand, NULL, ZEND_ACC_PUBLIC)

     /* introspection */
     PHP_ME(Redis, getHost, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, getPort, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, getDBNum, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, getTimeout, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, getReadTimeout, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, getPersistentID, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, getAuth, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, isConnected, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, getMode, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, wait, NULL, ZEND_ACC_PUBLIC)
     PHP_ME(Redis, pubsub, NULL, ZEND_ACC_PUBLIC)

     /* aliases */
     PHP_MALIAS(Redis, open, connect, NULL, ZEND_ACC_PUBLIC)
     PHP_MALIAS(Redis, popen, pconnect, NULL, ZEND_ACC_PUBLIC)
     PHP_MALIAS(Redis, lLen, lSize, NULL, ZEND_ACC_PUBLIC)
     PHP_MALIAS(Redis, sGetMembers, sMembers, NULL, ZEND_ACC_PUBLIC)
     PHP_MALIAS(Redis, mget, getMultiple, NULL, ZEND_ACC_PUBLIC)
     PHP_MALIAS(Redis, expire, setTimeout, NULL, ZEND_ACC_PUBLIC)
     PHP_MALIAS(Redis, zunionstore, zUnion, NULL, ZEND_ACC_PUBLIC)
     PHP_MALIAS(Redis, zinterstore, zInter, NULL, ZEND_ACC_PUBLIC)

     PHP_MALIAS(Redis, zRemove, zDelete, NULL, ZEND_ACC_PUBLIC)
     PHP_MALIAS(Redis, zRem, zDelete, NULL, ZEND_ACC_PUBLIC)
     PHP_MALIAS(Redis, zRemoveRangeByScore, zDeleteRangeByScore, NULL, ZEND_ACC_PUBLIC)
     PHP_MALIAS(Redis, zRemRangeByScore, zDeleteRangeByScore, NULL, ZEND_ACC_PUBLIC)
     PHP_MALIAS(Redis, zRemRangeByRank, zDeleteRangeByRank, NULL, ZEND_ACC_PUBLIC)
     PHP_MALIAS(Redis, zSize, zCard, NULL, ZEND_ACC_PUBLIC)
     PHP_MALIAS(Redis, substr, getRange, NULL, ZEND_ACC_PUBLIC)
     PHP_MALIAS(Redis, rename, renameKey, NULL, ZEND_ACC_PUBLIC)
     PHP_MALIAS(Redis, del, delete, NULL, ZEND_ACC_PUBLIC)
     PHP_MALIAS(Redis, keys, getKeys, NULL, ZEND_ACC_PUBLIC)
     PHP_MALIAS(Redis, lrem, lRemove, NULL, ZEND_ACC_PUBLIC)
     PHP_MALIAS(Redis, ltrim, listTrim, NULL, ZEND_ACC_PUBLIC)
     PHP_MALIAS(Redis, lindex, lGet, NULL, ZEND_ACC_PUBLIC)
     PHP_MALIAS(Redis, lrange, lGetRange, NULL, ZEND_ACC_PUBLIC)
     PHP_MALIAS(Redis, scard, sSize, NULL, ZEND_ACC_PUBLIC)
     PHP_MALIAS(Redis, srem, sRemove, NULL, ZEND_ACC_PUBLIC)
     PHP_MALIAS(Redis, sismember, sContains, NULL, ZEND_ACC_PUBLIC)
     PHP_MALIAS(Redis, zrevrange, zReverseRange, NULL, ZEND_ACC_PUBLIC)

     PHP_MALIAS(Redis, sendEcho, echo, NULL, ZEND_ACC_PUBLIC)

     PHP_MALIAS(Redis, evaluate, eval, NULL, ZEND_ACC_PUBLIC)
     PHP_MALIAS(Redis, evaluateSha, evalsha, NULL, ZEND_ACC_PUBLIC)
     {NULL, NULL, NULL}
};

zend_module_entry redis_module_entry = {
#if ZEND_MODULE_API_NO >= 20010901
     STANDARD_MODULE_HEADER,
#endif
     "redis",
     NULL,
     PHP_MINIT(redis),
     PHP_MSHUTDOWN(redis),
     PHP_RINIT(redis),
     PHP_RSHUTDOWN(redis),
     PHP_MINFO(redis),
#if ZEND_MODULE_API_NO >= 20010901
     PHP_REDIS_VERSION,
#endif
     STANDARD_MODULE_PROPERTIES
};

#ifdef COMPILE_DL_REDIS
ZEND_GET_MODULE(redis)
#endif

PHP_REDIS_API zend_class_entry *redis_get_exception_base(int root)
{
#if HAVE_SPL
    if (!root) {
         if (!spl_ce_RuntimeException) {
            zend_class_entry *pce;

            if ((pce = zend_hash_str_find_ptr(CG(class_table), "runtimeexception", sizeof("RuntimeException") - 1))) {
	            spl_ce_RuntimeException = pce;
                return pce;
            }
        } else {
            return spl_ce_RuntimeException;
        }
	}
#endif

    return zend_exception_get_default();
}

/**
 * Send a static DISCARD in case we're in MULTI mode.
 */
static int send_discard_static(RedisSock *redis_sock) {

	int result = FAILURE;
	char *cmd, *response;
   	int response_len, cmd_len;

   	/* format our discard command */
   	cmd_len = redis_cmd_format_static(&cmd, "DISCARD", "");

   	/* send our DISCARD command */
   	if (redis_sock_write(redis_sock, cmd, cmd_len) >= 0 &&
   	   (response = redis_sock_read(redis_sock, &response_len)) != NULL) {

   		/* success if we get OK */
   		result = (response_len == 3 && strncmp(response,"+OK", 3) == 0) ? SUCCESS : FAILURE;

   		/* free our response */
   		efree(response);
   	}

   	/* free our command */
   	efree(cmd);

   	/* return success/failure */
   	return result;
}

/**
 * redis_destructor_redis_sock
 */
static void redis_destructor_redis_sock(zend_resource * rsrc)
{
    RedisSock *redis_sock = (RedisSock *) rsrc->ptr;
    redis_sock_disconnect(redis_sock);
    redis_free_socket(redis_sock);
}

/**
 * redis_sock_get_resource
 */
PHP_REDIS_API zend_resource *redis_sock_get_resource(zval *id, int type, int no_throw)
{
    zval *socket;

    if (Z_TYPE_P(id) == IS_OBJECT) {
        if (socket = zend_hash_str_find(Z_OBJPROP_P(id), "socket", sizeof("socket") - 1)) {
            if (Z_TYPE_P(socket) == IS_RESOURCE &&
                    Z_RES_P(socket)->type == type &&
                    Z_RES_P(socket)->ptr)
            {
                return Z_RES_P(socket);
            }
        }
    }

    if(!no_throw) {
        zend_throw_exception(redis_exception_ce, "Redis server went away", 0);
    }

    return NULL;
}

/**
 * redis_sock_get
 */
PHP_REDIS_API int redis_sock_get(zval *id, RedisSock **redis_sock, int no_throw)
{
    zend_resource *socket;

    if (!(socket = redis_sock_get_resource(id, le_redis_sock, no_throw))) {
        return -1;
    }

    *redis_sock = socket->ptr;

    if ((*redis_sock)->lazy_connect)
    {
        (*redis_sock)->lazy_connect = 0;
        if (redis_sock_server_open(*redis_sock, 1) < 0) {
            return -1;
        }
    }

    return socket->handle;
}

/*
 * redis_sock_get_direct
 * Returns our attached RedisSock pointer if we're connected
 */
PHP_REDIS_API RedisSock *redis_sock_get_connected(INTERNAL_FUNCTION_PARAMETERS) {
    zval *object;
    RedisSock *redis_sock;

    /* If we can't grab our object, or get a socket, or we're not connected, return NULL */
    if((zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "O", &object, redis_ce) == FAILURE) ||
       (redis_sock_get(object, &redis_sock, 1) < 0) || redis_sock->status != REDIS_SOCK_STATUS_CONNECTED)
    {
        return NULL;
    }

    /* Return our socket */
    return redis_sock;
}


/**
 * PHP_MINIT_FUNCTION
 */
PHP_MINIT_FUNCTION(redis)
{

    zend_class_entry redis_class_entry;
    zend_class_entry redis_array_class_entry;
    zend_class_entry redis_exception_class_entry;

	REGISTER_INI_ENTRIES();

	/* Redis class */
	INIT_CLASS_ENTRY(redis_class_entry, "Redis", redis_functions);
    redis_ce = zend_register_internal_class(&redis_class_entry);

	/* RedisArray class
	INIT_CLASS_ENTRY(redis_array_class_entry, "RedisArray", redis_array_functions);
    redis_array_ce = zend_register_internal_class(&redis_array_class_entry);

    le_redis_array = zend_register_list_destructors_ex(
        redis_destructor_redis_array,
        NULL,
        "Redis Array", module_number
    );

	/* RedisException class */

    INIT_CLASS_ENTRY(redis_exception_class_entry, "RedisException", NULL);
    redis_exception_ce = zend_register_internal_class_ex(
        &redis_exception_class_entry,
        redis_get_exception_base(0)
    );

    le_redis_sock = zend_register_list_destructors_ex(
        redis_destructor_redis_sock,
        NULL,
        redis_sock_name, module_number
    );

	add_constant_long(redis_ce, "REDIS_NOT_FOUND", REDIS_NOT_FOUND);
	add_constant_long(redis_ce, "REDIS_STRING", REDIS_STRING);
	add_constant_long(redis_ce, "REDIS_SET", REDIS_SET);
	add_constant_long(redis_ce, "REDIS_LIST", REDIS_LIST);
	add_constant_long(redis_ce, "REDIS_ZSET", REDIS_ZSET);
	add_constant_long(redis_ce, "REDIS_HASH", REDIS_HASH);

	add_constant_long(redis_ce, "ATOMIC", ATOMIC);
	add_constant_long(redis_ce, "MULTI", MULTI);
	add_constant_long(redis_ce, "PIPELINE", PIPELINE);

    /* options */
    add_constant_long(redis_ce, "OPT_SERIALIZER", REDIS_OPT_SERIALIZER);
    add_constant_long(redis_ce, "OPT_PREFIX", REDIS_OPT_PREFIX);
    add_constant_long(redis_ce, "OPT_READ_TIMEOUT", REDIS_OPT_READ_TIMEOUT);

    /* serializer */
    add_constant_long(redis_ce, "SERIALIZER_NONE", REDIS_SERIALIZER_NONE);
    add_constant_long(redis_ce, "SERIALIZER_PHP", REDIS_SERIALIZER_PHP);

    /* scan options */
    add_constant_long(redis_ce, "OPT_SCAN", REDIS_OPT_SCAN);
    add_constant_long(redis_ce, "SCAN_RETRY", REDIS_SCAN_RETRY);
    add_constant_long(redis_ce, "SCAN_NORETRY", REDIS_SCAN_NORETRY);
#ifdef HAVE_REDIS_IGBINARY
    add_constant_long(redis_ce, "SERIALIZER_IGBINARY", REDIS_SERIALIZER_IGBINARY);
#endif

	zend_declare_class_constant_stringl(redis_ce, "AFTER", 5, "after", 5);
	zend_declare_class_constant_stringl(redis_ce, "BEFORE", 6, "before", 6);

#ifdef PHP_SESSION
    /* declare session handler */
    php_session_register_module(&ps_mod_redis);
#endif

    return SUCCESS;
}

/**
 * PHP_MSHUTDOWN_FUNCTION
 */
PHP_MSHUTDOWN_FUNCTION(redis)
{
    return SUCCESS;
}

/**
 * PHP_RINIT_FUNCTION
 */
PHP_RINIT_FUNCTION(redis)
{
    return SUCCESS;
}

/**
 * PHP_RSHUTDOWN_FUNCTION
 */
PHP_RSHUTDOWN_FUNCTION(redis)
{
    return SUCCESS;
}

/**
 * PHP_MINFO_FUNCTION
 */
PHP_MINFO_FUNCTION(redis)
{
    php_info_print_table_start();
    php_info_print_table_header(2, "Redis Support", "enabled");
    php_info_print_table_row(2, "Redis Version", PHP_REDIS_VERSION);
    php_info_print_table_end();
}

/* {{{ proto Redis Redis::__construct()
    Public constructor */
PHP_METHOD(Redis, __construct)
{
    if (zend_parse_parameters(ZEND_NUM_ARGS(), "") == FAILURE) {
        RETURN_FALSE;
    }
}
/* }}} */

/* {{{ proto Redis Redis::__destruct()
    Public Destructor
 */
PHP_METHOD(Redis,__destruct) {
	RedisSock *redis_sock;

	if(zend_parse_parameters(ZEND_NUM_ARGS(), "") == FAILURE) {
		RETURN_FALSE;
	}

	/* Grab our socket */
	if (redis_sock_get(getThis(), &redis_sock, 1) < 0) {
		RETURN_FALSE;
	}

	/* If we think we're in MULTI mode, send a discard */
	if(redis_sock->mode == MULTI) {
		/* Discard any multi commands, and free any callbacks that have been queued */
		send_discard_static(redis_sock);
		free_reply_callbacks(getThis(), redis_sock);
	}
}

/* {{{ proto boolean Redis::connect(string host, int port [, double timeout [, long retry_interval]])
 */
PHP_METHOD(Redis, connect)
{
	if (redis_connect(INTERNAL_FUNCTION_PARAM_PASSTHRU, 0) == FAILURE) {
		RETURN_FALSE;
	} else {
		RETURN_TRUE;
	}
}
/* }}} */

/* {{{ proto boolean Redis::pconnect(string host, int port [, double timeout])
 */
PHP_METHOD(Redis, pconnect)
{
	if (redis_connect(INTERNAL_FUNCTION_PARAM_PASSTHRU, 1) == FAILURE) {
		RETURN_FALSE;
	} else {
		/* reset multi/exec state if there is one. */
		RedisSock *redis_sock;
		if (redis_sock_get(getThis(), &redis_sock, 0) < 0) {
			RETURN_FALSE;
		}

		RETURN_TRUE;
	}
}
/* }}} */

PHP_REDIS_API int redis_connect(INTERNAL_FUNCTION_PARAMETERS, int persistent) {
	zval *object;
	zval *socket;
    zend_resource *oldsocket;
	int host_len, id;
	char *host = NULL;
	long port = -1;
	long retry_interval = 0;

	char *persistent_id = NULL;
	int persistent_id_len = -1;

	double timeout = 0.0;
	RedisSock *redis_sock  = NULL;

#ifdef ZTS
	/* not sure how in threaded mode this works so disabled persistents at first */
    persistent = 0;
#endif

	if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "Os|ldsl",
				&object, redis_ce, &host, &host_len, &port,
				&timeout, &persistent_id, &persistent_id_len,
				&retry_interval) == FAILURE) {
		return FAILURE;
	}

	if (timeout < 0L || timeout > INT_MAX) {
		zend_throw_exception(redis_exception_ce, "Invalid timeout", 0);
		return FAILURE;
	}

	if (retry_interval < 0L || retry_interval > INT_MAX) {
		zend_throw_exception(redis_exception_ce, "Invalid retry interval", 0);
		return FAILURE;
	}

	if(port == -1 && host_len && host[0] != '/') { /* not unix socket, set to default value */
		port = 6379;
	}

	/* if there is a redis sock already we have to remove it from the list */
    if (oldsocket = redis_sock_get_resource(object, le_redis_sock, 1)) {
        /* the refcount should be decreased and the detructor called */
        zend_list_close(oldsocket);
    }

	redis_sock = redis_sock_create(host, host_len, port, timeout, persistent, persistent_id, retry_interval, 0);

	if (redis_sock_server_open(redis_sock, 1) < 0) {
		redis_free_socket(redis_sock);
		return FAILURE;
	}

	socket = zend_list_insert(redis_sock, le_redis_sock);
	add_property_resource(object, "socket", Z_RES_P(socket));

	return SUCCESS;
}

/* {{{ proto boolean Redis::bitop(string op, string key, ...)
 */
PHP_METHOD(Redis, bitop)
{
    char *cmd;
    int cmd_len;

	zval *z_args;
	zend_string **keys;
	int argc = ZEND_NUM_ARGS(), i;
    RedisSock *redis_sock = NULL;
    smart_string buf = {0};

	/* get redis socket */
    if (redis_sock_get(getThis(), &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

	/* fetch args */
    z_args = emalloc(argc * sizeof(zval));
    if(zend_get_parameters_array_ex(argc, z_args) == FAILURE
			|| argc < 3 /* 3 args min. */
			|| Z_TYPE(z_args[0]) != IS_STRING /* operation must be a string. */
			) {
        efree(z_args);
		RETURN_FALSE;
    }

	keys = emalloc(argc * sizeof(zend_string *));

    keys[0] = zend_string_copy(Z_STR(z_args[0]));

	/* prefix keys */
	for(i = 1; i < argc; ++i) {
		convert_to_string(&z_args[i]);

		keys[i] = redis_key_prefix(redis_sock, Z_STR(z_args[i]));
	}

	/* start building the command */
	smart_string_appendc(&buf, '*');
	smart_string_append_long(&buf, argc + 1); /* +1 for BITOP command */
	smart_string_appendl(&buf, _NL, sizeof(_NL) - 1);

	/* add command name */
	smart_string_appendc(&buf, '$');
	smart_string_append_long(&buf, 5);
	smart_string_appendl(&buf, _NL, sizeof(_NL) - 1);
	smart_string_appendl(&buf, "BITOP", 5);
	smart_string_appendl(&buf, _NL, sizeof(_NL) - 1);

	/* add keys */
	for(i = 0; i < argc; ++i) {
		smart_string_appendc(&buf, '$');
		smart_string_append_long(&buf, keys[i]->len);
		smart_string_appendl(&buf, _NL, sizeof(_NL) - 1);
		smart_string_appendl(&buf, keys[i]->val, keys[i]->len);
		smart_string_appendl(&buf, _NL, sizeof(_NL) - 1);
	}
	/* end string */
	smart_string_0(&buf);
	cmd = buf.c;
	cmd_len = buf.len;

	/* cleanup */
	for (i = 0; i < argc; ++i) {
		zend_string_release(keys[i]);
	}

	efree(keys);
	efree(z_args);

	/* send */
	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
	IF_ATOMIC() {
		redis_long_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
	}
	REDIS_PROCESS_RESPONSE(redis_long_response);
}
/* }}} */

/* {{{ proto boolean Redis::bitcount(string key, [int start], [int end])
 */
PHP_METHOD(Redis, bitcount)
{
    zval *object;
    RedisSock *redis_sock;
    zend_string *key;
    char *cmd;
    int cmd_len;
    long start = 0, end = -1;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OS|ll",
                                     &object, redis_ce,
                                     &key, &start, &end) == FAILURE) {
        RETURN_FALSE;
    }

    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

	/* BITCOUNT key start end */
	key = redis_key_prefix(redis_sock, key);
    cmd_len = redis_cmd_format_static(&cmd, "BITCOUNT", "sdd", key->val, key->len, (int)start, (int)end);
	zend_string_release(key);

	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
	IF_ATOMIC() {
		redis_long_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
	}
	REDIS_PROCESS_RESPONSE(redis_long_response);

}
/* }}} */

/* {{{ proto integer Redis::bitpos(string key, int bit, [int start], [int end]) */
PHP_METHOD(Redis, bitpos)
{
    zval *object;
    RedisSock *redis_sock;
    zend_string *key;
    char *cmd;
    int cmd_len, argc;
    long bit, start, end;

    argc = ZEND_NUM_ARGS();

    if(zend_parse_method_parameters(argc, getThis(), "OSl|ll",
                                    &object, redis_ce, &key, &bit,
                                    &start, &end)==FAILURE)
    {
        RETURN_FALSE;
    }

    if(redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

    /* We can prevalidate the first argument */
    if(bit != 0 && bit != 1) {
        RETURN_FALSE;
    }

    /* Prefix our key */
    key = redis_key_prefix(redis_sock, key);

    /* Various command semantics */
    if(argc == 2) {
        cmd_len = redis_cmd_format_static(&cmd, "BITPOS", "sd", key->val, key->len,
                                          bit);
    } else if(argc == 3) {
        cmd_len = redis_cmd_format_static(&cmd, "BITPOS", "sdd", key->val, key->len,
                                          bit, start);
    } else {
        cmd_len = redis_cmd_format_static(&cmd, "BITPOS", "sddd", key->val, key->len,
                                          bit, start, end);
    }

    /* Free our key if it was prefixed */
    zend_string_release(key);

    REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
    IF_ATOMIC() {
        redis_long_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
    }
    REDIS_PROCESS_RESPONSE(redis_long_response);
}
/* }}} */

/* {{{ proto boolean Redis::close()
 */
PHP_METHOD(Redis, close)
{
    zval *object;
    RedisSock *redis_sock = NULL;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "O",
        &object, redis_ce) == FAILURE) {
        RETURN_FALSE;
    }

    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

    if (redis_sock_disconnect(redis_sock)) {
        RETURN_TRUE;
    }

    RETURN_FALSE;
}
/* }}} */

/* {{{ proto boolean Redis::set(string key, mixed value, long timeout | array options) */
PHP_METHOD(Redis, set) {
    zval *object;
    RedisSock *redis_sock;
    zend_string *key, *val;
    char *cmd, *exp_type = NULL, *set_type = NULL;
    int cmd_len;
    long expire = -1;
    zval *z_value, *z_opts = NULL;

    /* Make sure the arguments are correct */
    if(zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OSz|z",
                                    &object, redis_ce, &key, &z_value,
                                    &z_opts) == FAILURE)
    {
        RETURN_FALSE;
    }

    /* Ensure we can grab our redis socket */
    if(redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

    /* Our optional argument can either be a long (to support legacy SETEX */
    /* redirection), or an array with Redis >= 2.6.12 set options */
    if(z_opts && Z_TYPE_P(z_opts) != IS_LONG && Z_TYPE_P(z_opts) != IS_ARRAY
       && Z_TYPE_P(z_opts) != IS_NULL)
    {
        RETURN_FALSE;
    }

    /* Serialization, key prefixing */
    val = redis_serialize(redis_sock, z_value);
    key = redis_key_prefix(redis_sock, key);

    if(z_opts && Z_TYPE_P(z_opts) == IS_ARRAY) {
        HashTable *kt = Z_ARRVAL_P(z_opts);
        unsigned int ht_key_len;
        unsigned long idx;
        ulong nk;
        zend_string *k;
        zval *v;

        /* Iterate our option array */
        ZEND_HASH_FOREACH_KEY_VAL(kt, nk, k, v) {
            /* Grab key and value */
            if (key && IS_EX_PX_ARG(k->val) && Z_TYPE_P(v) == IS_LONG && Z_LVAL_P(v) > 0) {
                exp_type = k->val;
                expire = Z_LVAL_P(v);
            } else if(Z_TYPE_P(v) == IS_STRING && IS_NX_XX_ARG(Z_STRVAL_P(v))) {
                set_type = Z_STRVAL_P(v);
            }
        } ZEND_HASH_FOREACH_END();
    } else if(z_opts && Z_TYPE_P(z_opts) == IS_LONG) {
        expire = Z_LVAL_P(z_opts);
    }

    /* Now let's construct the command we want */
    if(exp_type && set_type) {
        /* SET <key> <value> NX|XX PX|EX <timeout> */
        cmd_len = redis_cmd_format_static(&cmd, "SET", "ssssl", key->val, key->len,
                                          val->val, val->len, set_type, 2, exp_type,
                                          2, expire);
    } else if(exp_type) {
        /* SET <key> <value> PX|EX <timeout> */
        cmd_len = redis_cmd_format_static(&cmd, "SET", "sssl", key->val, key->len,
                                          val->val, val->len, exp_type, 2, expire);
    } else if(set_type) {
        /* SET <key> <value> NX|XX */
        cmd_len = redis_cmd_format_static(&cmd, "SET", "sss", key->val, key->len,
                                          val->val, val->len, set_type, 2);
    } else if(expire > 0) {
        /* Backward compatible SETEX redirection */
        cmd_len = redis_cmd_format_static(&cmd, "SETEX", "sls", key->val, key->len,
                                          expire, val->val, val->len);
    } else {
        /* SET <key> <value> */
        cmd_len = redis_cmd_format_static(&cmd, "SET", "ss", key->val, key->len,
                                          val->val, val->len);
    }

    /* Free our key or value if we prefixed/serialized */
	zend_string_release(key);
    zend_string_release(val);

    /* Kick off the command */
    REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
    IF_ATOMIC() {
        redis_boolean_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
    }
    REDIS_PROCESS_RESPONSE(redis_boolean_response);
}

PHP_REDIS_API void redis_generic_setex(INTERNAL_FUNCTION_PARAMETERS, char *keyword) {

    zval *object;
    RedisSock *redis_sock;
    zend_string *key, *val;
    char *cmd;
    int cmd_len;
    long expire;
    zval *z_value;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OSlz",
                                     &object, redis_ce, &key,
                                     &expire, &z_value) == FAILURE) {
        RETURN_FALSE;
    }

    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

    val = redis_serialize(redis_sock, z_value);
	key = redis_key_prefix(redis_sock, key);
    cmd_len = redis_cmd_format_static(&cmd, keyword, "sls", key->val, key->len, expire, val->val, val->len);
    zend_string_release(val);
    zend_string_release(key);

	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
	IF_ATOMIC() {
		redis_boolean_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
	}
	REDIS_PROCESS_RESPONSE(redis_boolean_response);
}

/* {{{ proto boolean Redis::setex(string key, long expire, string value)
 */
PHP_METHOD(Redis, setex)
{
	redis_generic_setex(INTERNAL_FUNCTION_PARAM_PASSTHRU, "SETEX");
}

/* {{{ proto boolean Redis::psetex(string key, long expire, string value)
 */
PHP_METHOD(Redis, psetex)
{
	redis_generic_setex(INTERNAL_FUNCTION_PARAM_PASSTHRU, "PSETEX");
}

/* {{{ proto boolean Redis::setnx(string key, string value)
 */
PHP_METHOD(Redis, setnx)
{

    zval *object;
    RedisSock *redis_sock;
    zend_string *key, *val;
    char *cmd;
    int cmd_len;
    zval *z_value;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OSz",
                                     &object, redis_ce, &key,
                                     &z_value) == FAILURE) {
        RETURN_FALSE;
    }

    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

    val = redis_serialize(redis_sock, z_value);
	key = redis_key_prefix(redis_sock, key);
    cmd_len = redis_cmd_format_static(&cmd, "SETNX", "ss", key->val, key->len, val->val, val->len);
    zend_string_release(val);
    zend_string_release(key);

    REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);

    IF_ATOMIC() {
	  redis_1_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
    }

    REDIS_PROCESS_RESPONSE(redis_1_response);

}
/* }}} */
/* {{{ proto string Redis::getSet(string key, string value)
 */
PHP_METHOD(Redis, getSet)
{

    zval *object;
    RedisSock *redis_sock;
    zend_string *key, *val;
    char *cmd;
    int cmd_len;
    zval *z_value;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OSz",
                                     &object, redis_ce, &key,
                                     &z_value) == FAILURE) {
        RETURN_FALSE;
    }

    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

    val = redis_serialize(redis_sock, z_value);
	key = redis_key_prefix(redis_sock, key);
    cmd_len = redis_cmd_format_static(&cmd, "GETSET", "ss", key->val, key->len, val->val, val->len);
    zend_string_release(val);
    zend_string_release(key);

	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
	IF_ATOMIC() {
		redis_string_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
	}
	REDIS_PROCESS_RESPONSE(redis_string_response);

}
/* }}} */

/* {{{ proto string Redis::randomKey()
 */
PHP_METHOD(Redis, randomKey)
{

    zval *object;
    RedisSock *redis_sock;
    char *cmd;
    int cmd_len;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "O",
                                     &object, redis_ce) == FAILURE) {
        RETURN_FALSE;
    }

    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

    cmd_len = redis_cmd_format(&cmd, "*1" _NL "$9" _NL "RANDOMKEY" _NL);
	/* TODO: remove prefix from key */

	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
	IF_ATOMIC() {
	  redis_ping_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
	}
	REDIS_PROCESS_RESPONSE(redis_ping_response);
}
/* }}} */

/* {{{ proto string Redis::echo(string key)
 */
PHP_METHOD(Redis, echo)
{
    zval *object;
    RedisSock *redis_sock;
    zend_string *key;
    char *cmd;
    int cmd_len;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OS",
                                     &object, redis_ce, &key) == FAILURE) {
        RETURN_FALSE;
    }

    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

	key = redis_key_prefix(redis_sock, key);
    cmd_len = redis_cmd_format_static(&cmd, "ECHO", "s", key->val, key->len);
	zend_string_release(key);

	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
    IF_ATOMIC() {
	  redis_string_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
    }
    REDIS_PROCESS_RESPONSE(redis_string_response);

}
/* }}} */

/* {{{ proto string Redis::renameKey(string key_src, string key_dst)
 */
PHP_METHOD(Redis, renameKey)
{

    zval *object;
    RedisSock *redis_sock;
    char *cmd;
	zend_string *src, *dst;
    int cmd_len;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OSS",
                                     &object, redis_ce,
                                     &src, &dst) == FAILURE) {
        RETURN_FALSE;
    }

    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

	src = redis_key_prefix(redis_sock, src);
	dst = redis_key_prefix(redis_sock, dst);
    cmd_len = redis_cmd_format_static(&cmd, "RENAME", "ss", src->val, src->len, dst->val, dst->len);
	zend_string_release(src);
	zend_string_release(dst);

	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
	IF_ATOMIC() {
		redis_boolean_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
	}
	REDIS_PROCESS_RESPONSE(redis_boolean_response);

}
/* }}} */

/* {{{ proto string Redis::renameNx(string key_src, string key_dst)
 */
PHP_METHOD(Redis, renameNx)
{

    zval *object;
    RedisSock *redis_sock;
    char *cmd;
	zend_string *src, *dst;
    int cmd_len;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OSS",
                                     &object, redis_ce,
                                     &src, &dst) == FAILURE) {
        RETURN_FALSE;
    }

    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

	src = redis_key_prefix(redis_sock, src);
	dst = redis_key_prefix(redis_sock, dst);
    cmd_len = redis_cmd_format_static(&cmd, "RENAMENX", "ss", src->val, src->len, dst->val, dst->len);
	zend_string_release(src);
	zend_string_release(dst);

	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
	IF_ATOMIC() {
		redis_1_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
	}
	REDIS_PROCESS_RESPONSE(redis_1_response);

}
/* }}} */

/* {{{ proto string Redis::get(string key)
 */
PHP_METHOD(Redis, get)
{
    zval *object;
    RedisSock *redis_sock;
    zend_string *key;
    char *cmd;
    int cmd_len;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OS",
                                     &object, redis_ce, &key) == FAILURE) {
        RETURN_FALSE;
    }

    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

	key = redis_key_prefix(redis_sock, key);
    cmd_len = redis_cmd_format_static(&cmd, "GET", "s", key->val, key->len);
    zend_string_release(key);

	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
    IF_ATOMIC() {
	  redis_string_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
    }
    REDIS_PROCESS_RESPONSE(redis_string_response);

}
/* }}} */


/* {{{ proto string Redis::ping()
 */
PHP_METHOD(Redis, ping)
{
    zval *object;
    RedisSock *redis_sock;
    char *cmd;
    int cmd_len;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "O",
                                     &object, redis_ce) == FAILURE) {
        RETURN_FALSE;
    }

    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

    cmd_len = redis_cmd_format_static(&cmd, "PING", "");

	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
    IF_ATOMIC() {
	  redis_ping_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
    }
    REDIS_PROCESS_RESPONSE(redis_ping_response);
}
/* }}} */

PHP_REDIS_API void redis_atomic_increment(INTERNAL_FUNCTION_PARAMETERS, char *keyword, int count) {

    zval *object;
    RedisSock *redis_sock;
    zend_string *key;
    char *cmd;
    int cmd_len;
    long val = 1;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OS|l",
                                     &object, redis_ce,
                                     &key, &val) == FAILURE) {
        RETURN_FALSE;
    }

    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }
	key = redis_key_prefix(redis_sock, key);
    if (val == 1) {
        cmd_len = redis_cmd_format_static(&cmd, keyword, "s", key->val, key->len);
    } else {
        cmd_len = redis_cmd_format_static(&cmd, keyword, "sl", key->val, key->len, val);
    }
	zend_string_release(key);

	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
    IF_ATOMIC() {
		redis_long_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
    }
    REDIS_PROCESS_RESPONSE(redis_long_response);
}

/* {{{ proto boolean Redis::incr(string key [,int value])
 */
PHP_METHOD(Redis, incr){

    zval *object;
    zend_string *key;
    long val = 1;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OS|l",
                                     &object, redis_ce,
                                     &key, &val) == FAILURE) {
        RETURN_FALSE;
    }

    if(val == 1) {
        redis_atomic_increment(INTERNAL_FUNCTION_PARAM_PASSTHRU, "INCR", 1);
    } else {
        redis_atomic_increment(INTERNAL_FUNCTION_PARAM_PASSTHRU, "INCRBY", val);
    }
}
/* }}} */

/* {{{ proto boolean Redis::incrBy(string key ,int value)
 */
PHP_METHOD(Redis, incrBy){

    zval *object;
    zend_string *key;
    long val = 1;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OSl",
                                     &object, redis_ce,
                                     &key, &val) == FAILURE) {
        RETURN_FALSE;
    }

    if(val == 1) {
        redis_atomic_increment(INTERNAL_FUNCTION_PARAM_PASSTHRU, "INCR", 1);
    } else {
        redis_atomic_increment(INTERNAL_FUNCTION_PARAM_PASSTHRU, "INCRBY", val);
    }
}
/* }}} */

/* {{{ proto float Redis::incrByFloat(string key, float value)
 */
PHP_METHOD(Redis, incrByFloat) {
	zval *object;
	RedisSock *redis_sock;
    zend_string *key;
	char *cmd;
	int cmd_len;
	double val;

	if(zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OSd",
									&object, redis_ce, &key, &val) == FAILURE) {
		RETURN_FALSE;
	}

	if(redis_sock_get(object, &redis_sock, 0) < 0) {
		RETURN_FALSE;
	}

	/* Prefix key, format command, free old key if necissary */
	key = redis_key_prefix(redis_sock, key);
	cmd_len = redis_cmd_format_static(&cmd, "INCRBYFLOAT", "sf", key->val, key->len, val);
	zend_string_release(key);

	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
	IF_ATOMIC() {
		redis_bulk_double_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
	}
	REDIS_PROCESS_RESPONSE(redis_bulk_double_response);
}

/* {{{ proto boolean Redis::decr(string key [,int value])
 */
PHP_METHOD(Redis, decr)
{
    zval *object;
    zend_string *key;
    long val = 1;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OS|l",
                                     &object, redis_ce,
                                     &key, &val) == FAILURE) {
        RETURN_FALSE;
    }

    if(val == 1) {
        redis_atomic_increment(INTERNAL_FUNCTION_PARAM_PASSTHRU, "DECR", 1);
    } else {
        redis_atomic_increment(INTERNAL_FUNCTION_PARAM_PASSTHRU, "DECRBY", val);
    }
}
/* }}} */

/* {{{ proto boolean Redis::decrBy(string key ,int value)
 */
PHP_METHOD(Redis, decrBy){

    zval *object;
    zend_string *key;
    long val = 1;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OSl",
                                     &object, redis_ce,
                                     &key, &val) == FAILURE) {
        RETURN_FALSE;
    }

    if(val == 1) {
        redis_atomic_increment(INTERNAL_FUNCTION_PARAM_PASSTHRU, "DECR", 1);
    } else {
        redis_atomic_increment(INTERNAL_FUNCTION_PARAM_PASSTHRU, "DECRBY", val);
    }
}
/* }}} */

/* {{{ proto array Redis::getMultiple(array keys)
 */
PHP_METHOD(Redis, getMultiple)
{
    zval *object, *z_args, *z_ele;
    HashTable *hash;
    HashPosition ptr;
    RedisSock *redis_sock;
    smart_string cmd = {0};
    int arg_count;
	zend_string *key;

    /* Make sure we have proper arguments */
    if(zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "Oa",
                                    &object, redis_ce, &z_args) == FAILURE) {
        RETURN_FALSE;
    }

    /* We'll need the socket */
    if(redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

    /* Grab our array */
    hash = Z_ARRVAL_P(z_args);

    /* We don't need to do anything if there aren't any keys */
    if((arg_count = zend_hash_num_elements(hash)) == 0) {
        RETURN_FALSE;
    }

    /* Build our command header */
    redis_cmd_init_sstr(&cmd, arg_count, "MGET", 4);

    /* Iterate through and grab our keys */
    ZEND_HASH_FOREACH_STR_KEY_VAL(hash, key, z_ele) {
        /* Apply key prefix if necessary */
        key = redis_key_prefix(redis_sock, key);

        /* Append this key to our command */
        redis_cmd_append_sstr(&cmd, key->val, key->len);

        /* Free our key if it was prefixed */
        zend_string_release(key);
    } ZEND_HASH_FOREACH_END();

    /* Kick off our command */
    REDIS_PROCESS_REQUEST(redis_sock, cmd.c, cmd.len);
    IF_ATOMIC() {
        if(redis_sock_read_multibulk_reply(INTERNAL_FUNCTION_PARAM_PASSTHRU,
                                           redis_sock, NULL, NULL) < 0) {
            RETURN_FALSE;
        }
    }
    REDIS_PROCESS_RESPONSE(redis_sock_read_multibulk_reply);
}

/* {{{ proto boolean Redis::exists(string key)
 */
PHP_METHOD(Redis, exists)
{
    zval *object;
    RedisSock *redis_sock;
	zend_string *key;
    char *cmd;
    int cmd_len;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OS",
                                     &object, redis_ce, &key) == FAILURE) {
        RETURN_FALSE;
    }
    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

	key = redis_key_prefix(redis_sock, key);
    cmd_len = redis_cmd_format_static(&cmd, "EXISTS", "s", key->val, key->len);
	zend_string_release(key);

    REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
    IF_ATOMIC() {
	  redis_1_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
    }
    REDIS_PROCESS_RESPONSE(redis_1_response);

}
/* }}} */

/* {{{ proto boolean Redis::delete(string key)
 */
PHP_METHOD(Redis, delete)
{
    RedisSock *redis_sock;

    if(FAILURE == generic_multiple_args_cmd(INTERNAL_FUNCTION_PARAM_PASSTHRU,
                    "DEL", sizeof("DEL") - 1,
					1, &redis_sock, 0, 1, 1))
		return;

    IF_ATOMIC() {
	  redis_long_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
    }
	REDIS_PROCESS_RESPONSE(redis_long_response);

}
/* }}} */

PHP_REDIS_API void redis_set_watch(RedisSock *redis_sock)
{
    redis_sock->watching = 1;
}

PHP_REDIS_API void redis_watch_response(INTERNAL_FUNCTION_PARAMETERS, RedisSock *redis_sock, zval *z_tab, void *ctx)
{
    redis_boolean_response_impl(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, z_tab, ctx, redis_set_watch);
}

/* {{{ proto boolean Redis::watch(string key1, string key2...)
 */
PHP_METHOD(Redis, watch)
{
    RedisSock *redis_sock;

    generic_multiple_args_cmd(INTERNAL_FUNCTION_PARAM_PASSTHRU,
                    "WATCH", sizeof("WATCH") - 1,
					1, &redis_sock, 0, 1, 1);
    redis_sock->watching = 1;
    IF_ATOMIC() {
        redis_watch_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
    }
    REDIS_PROCESS_RESPONSE(redis_watch_response);

}
/* }}} */

PHP_REDIS_API void redis_clear_watch(RedisSock *redis_sock)
{
    redis_sock->watching = 0;
}

PHP_REDIS_API void redis_unwatch_response(INTERNAL_FUNCTION_PARAMETERS, RedisSock *redis_sock, zval *z_tab, void *ctx)
{
    redis_boolean_response_impl(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, z_tab, ctx, redis_clear_watch);
}

/* {{{ proto boolean Redis::unwatch()
 */
PHP_METHOD(Redis, unwatch)
{
    char cmd[] = "*1" _NL "$7" _NL "UNWATCH" _NL;
    generic_empty_cmd_impl(INTERNAL_FUNCTION_PARAM_PASSTHRU, estrdup(cmd), sizeof(cmd)-1, redis_unwatch_response);

}
/* }}} */

/* {{{ proto array Redis::getKeys(string pattern)
 */
PHP_METHOD(Redis, getKeys)
{
    zval *object;
    RedisSock *redis_sock;
    zend_string *pattern;
	char *cmd;
    int cmd_len;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OS",
                                     &object, redis_ce,
                                     &pattern) == FAILURE) {
        RETURN_NULL();
    }

    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

	pattern = redis_key_prefix(redis_sock, pattern);
    cmd_len = redis_cmd_format_static(&cmd, "KEYS", "s", pattern->val, pattern->len);
	zend_string_release(pattern);

	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
    IF_ATOMIC() {
	    if (redis_mbulk_reply_raw(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL) < 0) {
    	    RETURN_FALSE;
	    }
    }
    REDIS_PROCESS_RESPONSE(redis_mbulk_reply_raw);
}
/* }}} */

/* {{{ proto int Redis::type(string key)
 */
PHP_METHOD(Redis, type)
{
    zval *object;
    RedisSock *redis_sock;
    zend_string *key;
    char *cmd;
    int cmd_len;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OS",
                                     &object, redis_ce, &key) == FAILURE) {
        RETURN_NULL();
    }

    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

	key = redis_key_prefix(redis_sock, key);
    cmd_len = redis_cmd_format_static(&cmd, "TYPE", "s", key->val, key->len);
	zend_string_release(key);

	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
	IF_ATOMIC() {
	  redis_type_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
	}
	REDIS_PROCESS_RESPONSE(redis_type_response);
}
/* }}} */

PHP_METHOD(Redis, append)
{
	zval *object;
	RedisSock *redis_sock;
	zend_string *key, *val;
	char *cmd;
	int cmd_len;

	if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OSS",
                                     &object, redis_ce,
                                     &key, &val) == FAILURE) {
		RETURN_NULL();
	}

	if (redis_sock_get(object, &redis_sock, 0) < 0) {
		RETURN_FALSE;
	}

	key = redis_key_prefix(redis_sock, key);
	cmd_len = redis_cmd_format_static(&cmd, "APPEND", "ss", key->val, key->len, val->val, val->len);
	zend_string_release(key);

	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
	IF_ATOMIC() {
		redis_long_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
	}
	REDIS_PROCESS_RESPONSE(redis_long_response);
}

PHP_METHOD(Redis, getRange)
{
	zval *object;
	RedisSock *redis_sock;
    zend_string *key;
	char *cmd;
	int cmd_len;
	long start, end;

	if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OSll",
                                     &object, redis_ce, &key,
                                     &start, &end) == FAILURE) {
		RETURN_FALSE;
	}

	if (redis_sock_get(object, &redis_sock, 0) < 0) {
		RETURN_FALSE;
	}

	key = redis_key_prefix(redis_sock, key);
	cmd_len = redis_cmd_format_static(&cmd, "GETRANGE", "sdd", key->val, key->len, (int)start, (int)end);
	zend_string_release(key);
	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
	IF_ATOMIC() {
		redis_string_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
	}
	REDIS_PROCESS_RESPONSE(redis_string_response);
}

PHP_METHOD(Redis, setRange)
{
	zval *object;
	RedisSock *redis_sock;
    zend_string *key, *val;
	char *cmd;
	int cmd_len;
	long offset;

	if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OSlS",
                                     &object, redis_ce, &key,
                                     &offset, &val) == FAILURE) {
		RETURN_FALSE;
	}

	if (redis_sock_get(object, &redis_sock, 0) < 0) {
		RETURN_FALSE;
	}

	key = redis_key_prefix(redis_sock, key);
	cmd_len = redis_cmd_format_static(&cmd, "SETRANGE", "sds", key->val, key->len, (int)offset, val->val, val->len);
	zend_string_release(key);
	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
	IF_ATOMIC() {
		redis_long_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
	}
	REDIS_PROCESS_RESPONSE(redis_long_response);
}

PHP_METHOD(Redis, getBit)
{
	zval *object;
	RedisSock *redis_sock;
	zend_string *key; char *cmd;
	int cmd_len;
	long offset;

	if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OSl",
                                     &object, redis_ce, &key,
                                     &offset) == FAILURE) {
		RETURN_FALSE;
	}

	if (redis_sock_get(object, &redis_sock, 0) < 0) {
		RETURN_FALSE;
	}

	/* GETBIT and SETBIT only work for 0 - 2^32-1 */
	if(offset < BITOP_MIN_OFFSET || offset > BITOP_MAX_OFFSET) {
	    RETURN_FALSE;
	}

	key = redis_key_prefix(redis_sock, key);
	cmd_len = redis_cmd_format_static(&cmd, "GETBIT", "sd", key->val, key->len, (int)offset);
	zend_string_release(key);
	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
	IF_ATOMIC() {
		redis_long_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
	}
	REDIS_PROCESS_RESPONSE(redis_long_response);
}

PHP_METHOD(Redis, setBit)
{
	zval *object;
	RedisSock *redis_sock;
	zend_string *key; char *cmd;
	int cmd_len;
	long offset;
	zend_bool val;

	if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OSlb",
                                     &object, redis_ce, &key,
                                     &offset, &val) == FAILURE) {
		RETURN_FALSE;
	}

	if (redis_sock_get(object, &redis_sock, 0) < 0) {
		RETURN_FALSE;
	}

	/* GETBIT and SETBIT only work for 0 - 2^32-1 */
	if(offset < BITOP_MIN_OFFSET || offset > BITOP_MAX_OFFSET) {
	    RETURN_FALSE;
	}

	key = redis_key_prefix(redis_sock, key);
	cmd_len = redis_cmd_format_static(&cmd, "SETBIT", "sdd", key->val, key->len, (int)offset, (int)val);
	zend_string_release(key);
	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
	IF_ATOMIC() {
		redis_long_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
	}
	REDIS_PROCESS_RESPONSE(redis_long_response);
}


PHP_METHOD(Redis, strlen)
{
	zval *object;
	RedisSock *redis_sock;
	char *cmd;
	int cmd_len;
	zend_string *key;

	if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OS",
                                     &object, redis_ce,
                                     &key) == FAILURE) {
		RETURN_NULL();
	}

	if (redis_sock_get(object, &redis_sock, 0) < 0) {
		RETURN_FALSE;
	}

	key = redis_key_prefix(redis_sock, key);
	cmd_len = redis_cmd_format_static(&cmd, "STRLEN", "s", key->val, key->len);
	zend_string_release(key);

	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
	IF_ATOMIC() {
		redis_long_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
	}
	REDIS_PROCESS_RESPONSE(redis_long_response);
}

PHP_REDIS_API void
generic_push_function(INTERNAL_FUNCTION_PARAMETERS, char *keyword, int keyword_len) {
    zval *object;
    RedisSock *redis_sock;
    zend_string *key, *val;
    char *cmd;
    int cmd_len;
    zval *z_value;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OSz",
                                     &object, redis_ce,
                                     &key, &z_value) == FAILURE) {
        RETURN_NULL();
    }

    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

    val = redis_serialize(redis_sock, z_value);
	key = redis_key_prefix(redis_sock, key);
    cmd_len = redis_cmd_format_static(&cmd, keyword, "ss", key->val, key->len, val->val, val->len);
    zend_string_release(val);
    zend_string_release(key);

	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
	IF_ATOMIC() {
		redis_long_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
	}
	REDIS_PROCESS_RESPONSE(redis_long_response);
}

/* {{{ proto boolean Redis::lPush(string key , string value)
 */
PHP_METHOD(Redis, lPush)
{
    RedisSock *redis_sock;

    if(FAILURE == generic_multiple_args_cmd(INTERNAL_FUNCTION_PARAM_PASSTHRU,
                    "LPUSH", sizeof("LPUSH") - 1,
                    2, &redis_sock, 0, 0, 1))
		return;

    IF_ATOMIC() {
        redis_long_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
    }
    REDIS_PROCESS_RESPONSE(redis_long_response);
}
/* }}} */

/* {{{ proto boolean Redis::rPush(string key , string value)
 */
PHP_METHOD(Redis, rPush)
{
    RedisSock *redis_sock;

    if(FAILURE == generic_multiple_args_cmd(INTERNAL_FUNCTION_PARAM_PASSTHRU,
                    "RPUSH", sizeof("RPUSH") - 1,
                    2, &redis_sock, 0, 0, 1))
		return;

    IF_ATOMIC() {
        redis_long_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
    }
    REDIS_PROCESS_RESPONSE(redis_long_response);
}
/* }}} */

PHP_METHOD(Redis, lInsert)
{

	zval *object;
	RedisSock *redis_sock;
    zend_string *key, *val, *position, *pivot;
	char *cmd;
	int cmd_len;
    zval *z_value, *z_pivot;


	if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OSSzz",
					&object, redis_ce,
					&key,
					&position,
					&z_pivot,
					&z_value) == FAILURE) {
		RETURN_NULL();
	}

	if (redis_sock_get(object, &redis_sock, 0) < 0) {
		RETURN_FALSE;
	}

	if(zend_string_equals_literal_ci(position, "after") ||
            zend_string_equals_literal_ci(position, "before"))
    {
		key = redis_key_prefix(redis_sock, key);
        val = redis_serialize(redis_sock, z_value);
        pivot = redis_serialize(redis_sock, z_pivot);
        cmd_len = redis_cmd_format_static(&cmd, "LINSERT", "ssss",
                key->val, key->len,
                position->val, position->len,
                pivot->val, pivot->len,
                val->val, val->len);
        zend_string_release(val);
		zend_string_release(key);
        zend_string_release(pivot);

		REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
		IF_ATOMIC() {
			redis_long_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
		}
		REDIS_PROCESS_RESPONSE(redis_long_response);
	} else {
		php_error_docref(NULL, E_ERROR, "Error on position");
	}

}

PHP_METHOD(Redis, lPushx)
{
	generic_push_function(INTERNAL_FUNCTION_PARAM_PASSTHRU, "LPUSHX", sizeof("LPUSHX")-1);
}

PHP_METHOD(Redis, rPushx)
{
	generic_push_function(INTERNAL_FUNCTION_PARAM_PASSTHRU, "RPUSHX", sizeof("RPUSHX")-1);
}

PHP_REDIS_API void
generic_pop_function(INTERNAL_FUNCTION_PARAMETERS, char *keyword, int keyword_len) {

    zval *object;
    RedisSock *redis_sock;
    zend_string *key;
    char *cmd;
    int cmd_len;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OS",
                                     &object, redis_ce,
                                     &key) == FAILURE) {
        RETURN_FALSE;
    }

    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

	key = redis_key_prefix(redis_sock, key);
    cmd_len = redis_cmd_format_static(&cmd, keyword, "s", key->val, key->len);
	zend_string_release(key);

	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
	IF_ATOMIC() {
		redis_string_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
	}
	REDIS_PROCESS_RESPONSE(redis_string_response);
}

/* {{{ proto string Redis::lPOP(string key)
 */
PHP_METHOD(Redis, lPop)
{
        generic_pop_function(INTERNAL_FUNCTION_PARAM_PASSTHRU, "LPOP", sizeof("LPOP")-1);
}
/* }}} */

/* {{{ proto string Redis::rPOP(string key)
 */
PHP_METHOD(Redis, rPop)
{
        generic_pop_function(INTERNAL_FUNCTION_PARAM_PASSTHRU, "RPOP", sizeof("RPOP")-1);
}
/* }}} */

/* {{{ proto string Redis::blPop(string key1, string key2, ..., int timeout)
 */
PHP_METHOD(Redis, blPop)
{

    RedisSock *redis_sock;

    if(FAILURE == generic_multiple_args_cmd(INTERNAL_FUNCTION_PARAM_PASSTHRU,
                    "BLPOP", sizeof("BLPOP") - 1,
					2, &redis_sock, 1, 1, 1))
		return;

    IF_ATOMIC() {
    	if (redis_sock_read_multibulk_reply(INTERNAL_FUNCTION_PARAM_PASSTHRU,
											redis_sock, NULL, NULL) < 0) {
        	RETURN_FALSE;
	    }
    }
    REDIS_PROCESS_RESPONSE(redis_sock_read_multibulk_reply);
}
/* }}} */

/* {{{ proto string Redis::brPop(string key1, string key2, ..., int timeout)
 */
PHP_METHOD(Redis, brPop)
{
    RedisSock *redis_sock;

    if(FAILURE == generic_multiple_args_cmd(INTERNAL_FUNCTION_PARAM_PASSTHRU,
                    "BRPOP", sizeof("BRPOP") - 1,
					2, &redis_sock, 1, 1, 1))
		return;

    IF_ATOMIC() {
    	if (redis_sock_read_multibulk_reply(INTERNAL_FUNCTION_PARAM_PASSTHRU,
											redis_sock, NULL, NULL) < 0) {
        	RETURN_FALSE;
	    }
    }
    REDIS_PROCESS_RESPONSE(redis_sock_read_multibulk_reply);
}
/* }}} */


/* {{{ proto int Redis::lSize(string key)
 */
PHP_METHOD(Redis, lSize)
{
    zval *object;
    RedisSock *redis_sock;
    zend_string *key;
    char *cmd;
    int cmd_len;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OS",
                                     &object, redis_ce,
                                     &key) == FAILURE) {
        RETURN_FALSE;
    }

    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

	key = redis_key_prefix(redis_sock, key);
    cmd_len = redis_cmd_format_static(&cmd, "LLEN", "s", key->val, key->len);
	zend_string_release(key);

	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
	IF_ATOMIC() {
		redis_long_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
	}
	REDIS_PROCESS_RESPONSE(redis_long_response);

}
/* }}} */

/* {{{ proto boolean Redis::lRemove(string list, string value, int count = 0)
 */
PHP_METHOD(Redis, lRemove)
{
    zval *object;
    RedisSock *redis_sock;
    char *cmd;
    int cmd_len;
    zend_string *key, *val;
    long count = 0;
    zval *z_value;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OSz|l",
                                     &object, redis_ce,
                                     &key, &z_value, &count) == FAILURE) {
        RETURN_NULL();
    }

    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }


    /* LREM key count value */
    val = redis_serialize(redis_sock, z_value);
	key = redis_key_prefix(redis_sock, key);
    cmd_len = redis_cmd_format_static(&cmd, "LREM", "sds", key->val, key->len, count, val->val, val->len);
    zend_string_release(val);
    zend_string_release(key);

	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
	IF_ATOMIC() {
		redis_long_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
	}
	REDIS_PROCESS_RESPONSE(redis_long_response);
}
/* }}} */

/* {{{ proto boolean Redis::listTrim(string key , int start , int end)
 */
PHP_METHOD(Redis, listTrim)
{
    zval *object;
    RedisSock *redis_sock;
    zend_string *key;
    char *cmd;
    int cmd_len;
    long start, end;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OSll",
                                     &object, redis_ce, &key,
                                     &start, &end) == FAILURE) {
        RETURN_FALSE;
    }

    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

	key = redis_key_prefix(redis_sock, key);
    cmd_len = redis_cmd_format_static(&cmd, "LTRIM", "sdd", key->val, key->len, (int)start, (int)end);
	zend_string_release(key);

	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
	IF_ATOMIC() {
		redis_boolean_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
	}
	REDIS_PROCESS_RESPONSE(redis_boolean_response);

}
/* }}} */

/* {{{ proto string Redis::lGet(string key , int index)
 */
PHP_METHOD(Redis, lGet)
{
    zval *object;
    RedisSock *redis_sock;
    zend_string *key; char *cmd;
    int cmd_len;
    long index;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OSl",
                                     &object, redis_ce,
                                     &key, &index) == FAILURE) {
        RETURN_NULL();
    }

    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

    /* LINDEX key pos */
	key = redis_key_prefix(redis_sock, key);
    cmd_len = redis_cmd_format_static(&cmd, "LINDEX", "sd", key->val, key->len, (int)index);
	zend_string_release(key);

	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
	IF_ATOMIC() {
		redis_string_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
	}
	REDIS_PROCESS_RESPONSE(redis_string_response);
}
/* }}} */

/* {{{ proto array Redis::lGetRange(string key, int start , int end)
 */
PHP_METHOD(Redis, lGetRange)
{
    zval *object;
    RedisSock *redis_sock;
    zend_string *key; char *cmd;
    int cmd_len;
    long start, end;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OSll",
                                     &object, redis_ce,
                                     &key, &start, &end) == FAILURE) {
        RETURN_FALSE;
    }

    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

    /* LRANGE key start end */
	key = redis_key_prefix(redis_sock, key);
    cmd_len = redis_cmd_format_static(&cmd, "LRANGE", "sdd", key->val, key->len, (int)start, (int)end);
	zend_string_release(key);

	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
	IF_ATOMIC() {
		redis_sock_read_multibulk_reply(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
	}
	REDIS_PROCESS_RESPONSE(redis_sock_read_multibulk_reply);

}
/* }}} */

/* {{{ proto boolean Redis::sAdd(string key , mixed value)
 */
PHP_METHOD(Redis, sAdd)
{
    RedisSock *redis_sock;

    if(FAILURE == generic_multiple_args_cmd(INTERNAL_FUNCTION_PARAM_PASSTHRU,
                    "SADD", sizeof("SADD") - 1,
					2, &redis_sock, 0, 0, 1))
		return;

	IF_ATOMIC() {
	  redis_long_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
	}
	REDIS_PROCESS_RESPONSE(redis_long_response);
}
/* }}} */

/* {{{ proto int Redis::sSize(string key)
 */
PHP_METHOD(Redis, sSize)
{
    zval *object;
    RedisSock *redis_sock;
    zend_string *key; char *cmd;
    int cmd_len;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OS",
                                     &object, redis_ce,
                                     &key) == FAILURE) {
        RETURN_FALSE;
    }

    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

	key = redis_key_prefix(redis_sock, key);
    cmd_len = redis_cmd_format_static(&cmd, "SCARD", "s", key->val, key->len);
	zend_string_release(key);

	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
	IF_ATOMIC() {
		redis_long_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
	}
	REDIS_PROCESS_RESPONSE(redis_long_response);
}
/* }}} */

/* {{{ proto boolean Redis::sRemove(string set, string value)
 */
PHP_METHOD(Redis, sRemove)
{
    RedisSock *redis_sock;

    if(FAILURE == generic_multiple_args_cmd(INTERNAL_FUNCTION_PARAM_PASSTHRU,
                    "SREM", sizeof("SREM") - 1,
					2, &redis_sock, 0, 0, 1))
		return;

	IF_ATOMIC() {
	  redis_long_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
	}
	REDIS_PROCESS_RESPONSE(redis_long_response);
}
/* }}} */
/* {{{ proto boolean Redis::sMove(string set_src, string set_dst, mixed value)
 */
PHP_METHOD(Redis, sMove)
{
    zval *object;
    RedisSock *redis_sock;
    zend_string *src, *dst, *val;
    char *cmd;
    int cmd_len;
    zval *z_value;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OSSz",
                                     &object, redis_ce,
                                     &src, &dst,
                                     &z_value) == FAILURE) {
        RETURN_FALSE;
    }

    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

    val = redis_serialize(redis_sock, z_value);
	src = redis_key_prefix(redis_sock, src);
	dst = redis_key_prefix(redis_sock, dst);
    cmd_len = redis_cmd_format_static(&cmd, "SMOVE", "sss", src->val, src->len, dst->val, dst->len, val->val, val->len);
    zend_string_release(val);
	zend_string_release(src);
	zend_string_release(dst);

	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
	IF_ATOMIC() {
	  redis_1_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
	}
	REDIS_PROCESS_RESPONSE(redis_1_response);
}
/* }}} */

/* }}} */
/* {{{ proto string Redis::sPop(string key)
 */
PHP_METHOD(Redis, sPop)
{
    generic_pop_function(INTERNAL_FUNCTION_PARAM_PASSTHRU, "SPOP", 4);
}
/* }}} */

/* }}} */
/* {{{ proto string Redis::sRandMember(string key [int count])
 */
PHP_METHOD(Redis, sRandMember)
{
    zval *object;
    RedisSock *redis_sock;
    zend_string *key; char *cmd;
    int cmd_len = 0;
    long count;

    /* Parse our params */
    if(zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OS|l",
                                    &object, redis_ce, &key, &count) == FAILURE) {
        RETURN_FALSE;
    }

    /* Get our redis socket */
    if(redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

    /* Prefix our key if necissary */
    key = redis_key_prefix(redis_sock, key);

    /* If we have two arguments, we're running with an optional COUNT, which will return */
    /* a multibulk reply.  Without the argument we'll return a string response */
    if(ZEND_NUM_ARGS() == 2) {
        /* Construct our command with count */
        cmd_len = redis_cmd_format_static(&cmd, "SRANDMEMBER", "sl", key->val, key->len, count);
    } else {
        /* Construct our command */
        cmd_len = redis_cmd_format_static(&cmd, "SRANDMEMBER", "s", key->val, key->len);
    }

    /* Free our key if we prefixed it */
    zend_string_release(key);

    /* Process our command */
    REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);

    /* Either bulk or multi-bulk depending on argument count */
    if(ZEND_NUM_ARGS() == 2) {
        IF_ATOMIC() {
            if(redis_sock_read_multibulk_reply(INTERNAL_FUNCTION_PARAM_PASSTHRU,
                                               redis_sock, NULL, NULL) < 0)
            {
                RETURN_FALSE;
            }
        }
        REDIS_PROCESS_RESPONSE(redis_sock_read_multibulk_reply);
    } else {
        IF_ATOMIC() {
            redis_string_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock,
                                  NULL, NULL);
        }
        REDIS_PROCESS_RESPONSE(redis_string_response);
    }
}
/* }}} */

/* {{{ proto boolean Redis::sContains(string set, string value)
 */
PHP_METHOD(Redis, sContains)
{
	zval *object;
    RedisSock *redis_sock;
    zend_string *key, *val;
    char *cmd;
    int cmd_len;
    zval *z_value;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OSz",
                                     &object, redis_ce,
                                     &key, &z_value) == FAILURE) {
        return;
    }

    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

    val = redis_serialize(redis_sock, z_value);
	key = redis_key_prefix(redis_sock, key);
    cmd_len = redis_cmd_format_static(&cmd, "SISMEMBER", "ss", key->val, key->len, val->val, val->len);
    zend_string_release(val);
    zend_string_release(key);

	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
	IF_ATOMIC() {
		redis_1_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
	}
	REDIS_PROCESS_RESPONSE(redis_1_response);
}
/* }}} */

/* {{{ proto array Redis::sMembers(string set)
 */
PHP_METHOD(Redis, sMembers)
{
    zval *object;
    RedisSock *redis_sock;
    zend_string *key; char *cmd;
    int cmd_len;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OS",
                                     &object, redis_ce,
                                     &key) == FAILURE) {
        RETURN_FALSE;
    }

    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

	key = redis_key_prefix(redis_sock, key);
    cmd_len = redis_cmd_format_static(&cmd, "SMEMBERS", "s", key->val, key->len);
	zend_string_release(key);

	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
	IF_ATOMIC() {
	    if (redis_sock_read_multibulk_reply(INTERNAL_FUNCTION_PARAM_PASSTHRU,
    	                                    redis_sock, NULL, NULL) < 0) {
        	RETURN_FALSE;
	    }
	}
    REDIS_PROCESS_RESPONSE(redis_sock_read_multibulk_reply);
}
/* }}} */

PHP_REDIS_API int generic_multiple_args_cmd(INTERNAL_FUNCTION_PARAMETERS, char *keyword, int keyword_len,
									 int min_argc, RedisSock **out_sock, int has_timeout, int all_keys, int can_serialize)
{
    zval *z_args, *z_array;
    zend_string **keys;
    char *cmd;
    int cmd_len;
    int i, j, argc = ZEND_NUM_ARGS(), real_argc = 0;
    int single_array = 0;
	int timeout = 0;
	int pos;
	int array_size;

    RedisSock *redis_sock;

    if(argc < min_argc) {
		zend_wrong_param_count();
		ZVAL_BOOL(return_value, 0);
		return FAILURE;
    }

	/* get redis socket */
    if (redis_sock_get(getThis(), out_sock, 0) < 0) {
		ZVAL_BOOL(return_value, 0);
		return FAILURE;
    }
    redis_sock = *out_sock;

    z_args = emalloc(argc * sizeof(zval));
    if(zend_get_parameters_array_ex(argc, z_args) == FAILURE) {
        efree(z_args);
		ZVAL_BOOL(return_value, 0);
		return FAILURE;
    }

    /* case of a single array */
	if(has_timeout == 0) {
	    if(argc == 1 && Z_TYPE(z_args[0]) == IS_ARRAY) {
    	    single_array = 1;
        	z_array = &z_args[0];
    	    z_args = NULL;

        	/* new count */
	        argc = zend_hash_num_elements(Z_ARRVAL_P(z_array));
    	}
	} else if(has_timeout == 1) {
		if(argc == 2 && Z_TYPE(z_args[0]) == IS_ARRAY && Z_TYPE(z_args[1]) == IS_LONG) {
    	    single_array = 1;
        	z_array = &z_args[0];
			timeout = Z_LVAL(z_args[1]);
    	    z_args = NULL;
        	/* new count */
	        argc = zend_hash_num_elements(Z_ARRVAL_P(z_array));
    	}
	}

	/* prepare an array for the keys, one for their lengths, one to mark the keys to free. */
	array_size = argc;
	if(has_timeout)
		array_size++;

	keys = emalloc(array_size * sizeof(zend_string *));

    cmd_len = 1 + integer_length(keyword_len) + 2 +keyword_len + 2; /* start computing the command length */

    if(single_array) { /* loop over the array */
        HashTable *keytable = Z_ARRVAL_P(z_array);
		ulong j = 0;
        zend_string *key;
		zval *z_value_p;

		ZEND_HASH_FOREACH_STR_KEY_VAL(keytable, key, z_value_p) {
			if(!all_keys && j != 0) { /* not just operating on keys */

				if(can_serialize) {
					keys[j] = redis_serialize(redis_sock, z_value_p);
				} else {
					keys[j] = zval_get_string(z_value_p);
				}

			} else {

				/* only accept strings */
				if(Z_TYPE_P(z_value_p) != IS_STRING) {
					convert_to_string(z_value_p);
				}

                /* get current value w/ optional prefix */
				keys[j] = redis_key_prefix(redis_sock, Z_STR_P(z_value_p));
			}

            cmd_len += 1 + integer_length(keys[j]->len) + 2 + keys[j]->len + 2; /* $ + size + NL + string + NL */
            j++;
            real_argc++;
        } ZEND_HASH_FOREACH_END();

		if(has_timeout) {
			char *tmp;
			int tmp_len = spprintf(&tmp, 0, "%d", timeout);

			keys[j] = zend_string_init(tmp, tmp_len, 0);
			cmd_len += 1 + integer_length(keys[j]->len) + 2 + keys[j]->len + 2; /* $ + size + NL + string + NL  */
			j++;
			real_argc++;
		}
    } else {
		if(has_timeout && Z_TYPE(z_args[argc - 1]) != IS_LONG) {
			php_error_docref(NULL, E_ERROR, "Syntax error on timeout");
		}

        for(i = 0, j = 0; i < argc; ++i) { /* store each key */
			if(!all_keys && j != 0) { /* not just operating on keys */

				if(can_serialize) {
					keys[j] = redis_serialize(redis_sock, &z_args[i]);
				} else {
					keys[j] = zval_get_string(&z_args[i]);
				}

			} else {

           	    /* If we have a timeout it should be the last argument, which we do not want to prefix */
				if(!has_timeout || i < argc-1) {
                    zend_string *tmp = zval_get_string(&z_args[i]);
					keys[j] = redis_key_prefix(redis_sock, tmp); /* add optional prefix  TSRMLS_CC*/
                    zend_string_release(tmp);
				} else {
					keys[j] = zval_get_string(&z_args[i]);
				}
			}

            cmd_len += 1 + integer_length(keys[j]->len) + 2 + keys[j]->len + 2; /* $ + size + NL + string + NL */
            j++;
   	        real_argc++;
		}
    }

    cmd_len += 1 + integer_length(real_argc+1) + 2; /* *count NL  */
    cmd = emalloc(cmd_len+1);

    sprintf(cmd, "*%d" _NL "$%d" _NL "%s" _NL, 1+real_argc, keyword_len, keyword);

    pos = 1 +integer_length(real_argc + 1) + 2
          + 1 + integer_length(keyword_len) + 2
          + keyword_len + 2;

    /* copy each key to its destination */
    for(i = 0; i < real_argc; ++i) {
        sprintf(cmd + pos, "$%d" _NL, keys[i]->len);     /* size */
        pos += 1 + integer_length(keys[i]->len) + 2;
        memcpy(cmd + pos, keys[i]->val, keys[i]->len);
        pos += keys[i]->len;
        memcpy(cmd + pos, _NL, 2);
        pos += 2;
    }

	/* cleanup prefixed keys. */
	for(i = 0; i < real_argc + (has_timeout?-1:0); ++i) {
		zend_string_release(keys[i]);
	}
	if(single_array && has_timeout) { /* cleanup string created to contain timeout value */
		zend_string_release(keys[real_argc-1]);
	}

    efree(z_args);
    efree(keys);

    if(z_args) efree(z_args);

	/* call REDIS_PROCESS_REQUEST and skip void returns */
	IF_MULTI_OR_ATOMIC() {
		if(redis_sock_write(redis_sock, cmd, cmd_len) < 0) {
			efree(cmd);
			return FAILURE;
		}
		efree(cmd);
	}
	IF_PIPELINE() {
		PIPELINE_ENQUEUE_COMMAND(cmd, cmd_len);
		efree(cmd);
	}

    return SUCCESS;
}

/* {{{ proto array Redis::sInter(string key0, ... string keyN)
 */
PHP_METHOD(Redis, sInter) {

    RedisSock *redis_sock;

    if(FAILURE == generic_multiple_args_cmd(INTERNAL_FUNCTION_PARAM_PASSTHRU,
                    "SINTER", sizeof("SINTER") - 1,
					0, &redis_sock, 0, 1, 1))
		return;

    IF_ATOMIC() {
    	if (redis_sock_read_multibulk_reply(INTERNAL_FUNCTION_PARAM_PASSTHRU,
											redis_sock, NULL, NULL) < 0) {
        	RETURN_FALSE;
	    }
    }
    REDIS_PROCESS_RESPONSE(redis_sock_read_multibulk_reply);
}
/* }}} */

/* {{{ proto array Redis::sInterStore(string destination, string key0, ... string keyN)
 */
PHP_METHOD(Redis, sInterStore) {

    RedisSock *redis_sock;

    if(FAILURE == generic_multiple_args_cmd(INTERNAL_FUNCTION_PARAM_PASSTHRU,
                    "SINTERSTORE", sizeof("SINTERSTORE") - 1,
					1, &redis_sock, 0, 1, 1))
		return;

	IF_ATOMIC() {
		redis_long_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
	}
    REDIS_PROCESS_RESPONSE(redis_long_response);


}
/* }}} */

/* {{{ proto array Redis::sUnion(string key0, ... string keyN)
 */
PHP_METHOD(Redis, sUnion) {

    RedisSock *redis_sock;

    if(FAILURE == generic_multiple_args_cmd(INTERNAL_FUNCTION_PARAM_PASSTHRU,
                    "SUNION", sizeof("SUNION") - 1,
							  0, &redis_sock, 0, 1, 1))
		return;

	IF_ATOMIC() {
    	if (redis_sock_read_multibulk_reply(INTERNAL_FUNCTION_PARAM_PASSTHRU,
        	                                redis_sock, NULL, NULL) < 0) {
	        RETURN_FALSE;
    	}
	}
	REDIS_PROCESS_RESPONSE(redis_sock_read_multibulk_reply);
}
/* }}} */
/* {{{ proto array Redis::sUnionStore(string destination, string key0, ... string keyN)
 */
PHP_METHOD(Redis, sUnionStore) {

    RedisSock *redis_sock;

    if(FAILURE == generic_multiple_args_cmd(INTERNAL_FUNCTION_PARAM_PASSTHRU,
                    "SUNIONSTORE", sizeof("SUNIONSTORE") - 1,
					1, &redis_sock, 0, 1, 1))
		return;

	IF_ATOMIC() {
		redis_long_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
	}
	REDIS_PROCESS_RESPONSE(redis_long_response);
}

/* }}} */

/* {{{ proto array Redis::sDiff(string key0, ... string keyN)
 */
PHP_METHOD(Redis, sDiff) {

    RedisSock *redis_sock;

    if(FAILURE == generic_multiple_args_cmd(INTERNAL_FUNCTION_PARAM_PASSTHRU,
                    "SDIFF", sizeof("SDIFF") - 1,
					0, &redis_sock, 0, 1, 1))
		return;

	IF_ATOMIC() {
	    /* read multibulk reply */
    	if (redis_sock_read_multibulk_reply(INTERNAL_FUNCTION_PARAM_PASSTHRU,
											redis_sock, NULL, NULL) < 0) {
        	RETURN_FALSE;
	    }
	}
	REDIS_PROCESS_RESPONSE(redis_sock_read_multibulk_reply);
}
/* }}} */

/* {{{ proto array Redis::sDiffStore(string destination, string key0, ... string keyN)
 */
PHP_METHOD(Redis, sDiffStore) {

    RedisSock *redis_sock;

    if(FAILURE == generic_multiple_args_cmd(INTERNAL_FUNCTION_PARAM_PASSTHRU,
                    "SDIFFSTORE", sizeof("SDIFFSTORE") - 1,
					1, &redis_sock, 0, 1, 1))
		return;

	IF_ATOMIC() {
	  redis_long_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
	}
	REDIS_PROCESS_RESPONSE(redis_long_response);
}
/* }}} */

PHP_METHOD(Redis, sort) {

    zval *object = getThis(), *z_array = NULL, *z_cur;
    char *cmd, *old_cmd = NULL;
    zend_string *key;
    int cmd_len, elements = 2;
    int using_store = 0;
    RedisSock *redis_sock;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OS|a",
                                     &object, redis_ce,
                                     &key, &z_array) == FAILURE) {
        RETURN_FALSE;
    }

    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

    key = redis_key_prefix(redis_sock, key);
    cmd_len = redis_cmd_format(&cmd, "$4" _NL "SORT" _NL "$%d" _NL "%s" _NL, key->val, key->len);
    zend_string_release(key);

    if(z_array) {
        if (REDIS_SORT_KEY_FIND(z_array, "by", "BY", z_cur) && Z_TYPE_P(z_cur) == IS_STRING) {
            old_cmd = cmd;
            cmd_len = redis_cmd_format(&cmd, "%s"
                                             "$2" _NL
                                             "BY" _NL
                                             "$%d" _NL
                                             "%s" _NL
                                             , cmd, cmd_len
                                             , Z_STRLEN_P(z_cur), Z_STRVAL_P(z_cur), Z_STRLEN_P(z_cur));
            elements += 2;
            efree(old_cmd);
        }

        if (REDIS_SORT_KEY_FIND(z_array, "sort", "SORT", z_cur) && Z_TYPE_P(z_cur) == IS_STRING) {
            old_cmd = cmd;
            cmd_len = redis_cmd_format(&cmd, "%s"
                                             "$%d" _NL
                                             "%s" _NL
                                             , cmd, cmd_len
                                             , Z_STRLEN_P(z_cur), Z_STRVAL_P(z_cur), Z_STRLEN_P(z_cur));
            elements += 1;
            efree(old_cmd);
        }

        if (REDIS_SORT_KEY_FIND(z_array, "store", "STORE", z_cur) && Z_TYPE_P(z_cur) == IS_STRING) {
            using_store = 1;
            old_cmd = cmd;
            cmd_len = redis_cmd_format(&cmd, "%s"
                                             "$5" _NL
                                             "STORE" _NL
                                             "$%d" _NL
                                             "%s" _NL
                                             , cmd, cmd_len
                                             , Z_STRLEN_P(z_cur), Z_STRVAL_P(z_cur), Z_STRLEN_P(z_cur));
            elements += 2;
            efree(old_cmd);
        }

        if (REDIS_SORT_KEY_FIND(z_array, "get", "GET", z_cur)) {
            if (Z_TYPE_P(z_cur) == IS_STRING) {
                old_cmd = cmd;
                cmd_len = redis_cmd_format(&cmd, "%s"
                                                 "$3" _NL
                                                 "GET" _NL
                                                 "$%d" _NL
                                                 "%s" _NL
                                                 , cmd, cmd_len
                                                 , Z_STRLEN_P(z_cur), Z_STRVAL_P(z_cur), Z_STRLEN_P(z_cur));
                elements += 2;
                efree(old_cmd);
            } else if(Z_TYPE_P(z_cur) == IS_ARRAY) {
                /* loop over the strings in that array and add them as patterns */
                zend_string *key;
                ulong num_key;
                zval *zv;

                ZEND_HASH_FOREACH_KEY_VAL(Z_ARRVAL_P(z_cur), num_key, key, zv) {
                    if(Z_TYPE_P(zv) == IS_STRING) {
                        old_cmd = cmd;
                        cmd_len = redis_cmd_format(&cmd, "%s"
                                                         "$3" _NL
                                                         "GET" _NL
                                                         "$%d" _NL
                                                         "%s" _NL
                                                         , cmd, cmd_len
                                                         , Z_STRLEN_P(zv), Z_STRVAL_P(zv), Z_STRLEN_P(zv));
                        elements += 2;
                        efree(old_cmd);
                    }
                } ZEND_HASH_FOREACH_END();
            }
        }

        if (REDIS_SORT_KEY_FIND(z_array, "alpha", "ALPHA", z_cur) && Z_TYPE_P(z_cur) == IS_TRUE) {
            old_cmd = cmd;
            cmd_len = redis_cmd_format(&cmd, "%s"
                                             "$5" _NL
                                             "ALPHA" _NL
                                             , cmd, cmd_len);
            elements += 1;
            efree(old_cmd);
        }

        if (REDIS_SORT_KEY_FIND(z_array, "limit", "LIMIT", z_cur) && Z_TYPE_P(z_cur) == IS_ARRAY) {
            if(zend_hash_num_elements(Z_ARRVAL_P(z_cur)) == 2) {
                zval *z_offset_p, *z_count_p;
                /* get the two values from the table, check that they are indeed of LONG type */
                if ((z_offset_p = zend_hash_index_find(Z_ARRVAL_P(z_cur), 0)) &&
                  (z_count_p = zend_hash_index_find(Z_ARRVAL_P(z_cur), 1))) {

                    long limit_low, limit_high;
                    if((Z_TYPE_P(z_offset_p) == IS_LONG || Z_TYPE_P(z_offset_p) == IS_STRING) &&
                        (Z_TYPE_P(z_count_p) == IS_LONG || Z_TYPE_P(z_count_p) == IS_STRING)) {


                        if(Z_TYPE_P(z_offset_p) == IS_LONG) {
                            limit_low = Z_LVAL_P(z_offset_p);
                        } else {
                            limit_low = atol(Z_STRVAL_P(z_offset_p));
                        }
                        if(Z_TYPE_P(z_count_p) == IS_LONG) {
                            limit_high = Z_LVAL_P(z_count_p);
                        } else {
                            limit_high = atol(Z_STRVAL_P(z_count_p));
                        }

                        old_cmd = cmd;
                        cmd_len = redis_cmd_format(&cmd, "%s"
                                                         "$5" _NL
                                                         "LIMIT" _NL
                                                         "$%d" _NL
                                                         "%d" _NL
                                                         "$%d" _NL
                                                         "%d" _NL
                                                         , cmd, cmd_len
                                                         , integer_length(limit_low), limit_low
                                                         , integer_length(limit_high), limit_high);
                        elements += 3;
                        efree(old_cmd);
                    }
                }
            }
        }
    }

    /* complete with prefix */

    old_cmd = cmd;
    cmd_len = redis_cmd_format(&cmd, "*%d" _NL "%s", elements, cmd, cmd_len);
    efree(old_cmd);

    /* run command */
    REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
    if(using_store) {
        IF_ATOMIC() {
            redis_long_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
        }
        REDIS_PROCESS_RESPONSE(redis_long_response);
    } else {
        IF_ATOMIC() {
            if (redis_sock_read_multibulk_reply(INTERNAL_FUNCTION_PARAM_PASSTHRU,
                                                redis_sock, NULL, NULL) < 0) {
                RETURN_FALSE;
            }
        }
        REDIS_PROCESS_RESPONSE(redis_sock_read_multibulk_reply);
    }
}

PHP_REDIS_API void generic_sort_cmd(INTERNAL_FUNCTION_PARAMETERS, char *sort, int use_alpha) {

    zval *object;
    RedisSock *redis_sock;
    zend_string *key; char *pattern = NULL, *get = NULL, *store = NULL, *cmd;
    int pattern_len = -1, get_len = -1, store_len = -1, cmd_len;
    long sort_start = -1, sort_count = -1;

    int cmd_elements;

    char *cmd_lines[30];
    int cmd_sizes[30];

	int sort_len;
    int i, pos;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OS|sslls",
                                     &object, redis_ce,
                                     &key, &pattern, &pattern_len,
                                     &get, &get_len, &sort_start, &sort_count, &store, &store_len) == FAILURE) {
        RETURN_FALSE;
    }

    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }
    if(key->len == 0) {
        RETURN_FALSE;
    }

    /* first line, sort. */
    cmd_lines[1] = estrdup("$4");
    cmd_sizes[1] = 2;
    cmd_lines[2] = estrdup("SORT");
    cmd_sizes[2] = 4;

    /* Prefix our key if we need to */
    key = redis_key_prefix(redis_sock, key);

    /* second line, key */
    cmd_sizes[3] = redis_cmd_format(&cmd_lines[3], "$%d", key->len);
    cmd_lines[4] = emalloc(key->len + 1);
    memcpy(cmd_lines[4], key->val, key->len);
    cmd_lines[4][key->len] = 0;
    cmd_sizes[4] = key->len;

    /* If we prefixed our key, free it */
    zend_string_release(key);

    cmd_elements = 5;
    if(pattern && pattern_len) {
            /* BY */
            cmd_lines[cmd_elements] = estrdup("$2");
            cmd_sizes[cmd_elements] = 2;
            cmd_elements++;
            cmd_lines[cmd_elements] = estrdup("BY");
            cmd_sizes[cmd_elements] = 2;
            cmd_elements++;

            /* pattern */
            cmd_sizes[cmd_elements] = redis_cmd_format(&cmd_lines[cmd_elements], "$%d", pattern_len);
            cmd_elements++;
            cmd_lines[cmd_elements] = emalloc(pattern_len + 1);
            memcpy(cmd_lines[cmd_elements], pattern, pattern_len);
            cmd_lines[cmd_elements][pattern_len] = 0;
            cmd_sizes[cmd_elements] = pattern_len;
            cmd_elements++;
    }
    if(sort_start >= 0 && sort_count >= 0) {
            /* LIMIT */
            cmd_lines[cmd_elements] = estrdup("$5");
            cmd_sizes[cmd_elements] = 2;
            cmd_elements++;
            cmd_lines[cmd_elements] = estrdup("LIMIT");
            cmd_sizes[cmd_elements] = 5;
            cmd_elements++;

            /* start */
            cmd_sizes[cmd_elements] = redis_cmd_format(&cmd_lines[cmd_elements], "$%d", integer_length(sort_start));
            cmd_elements++;
            cmd_sizes[cmd_elements] = spprintf(&cmd_lines[cmd_elements], 0, "%d", (int)sort_start);
            cmd_elements++;

            /* count */
            cmd_sizes[cmd_elements] = redis_cmd_format(&cmd_lines[cmd_elements], "$%d", integer_length(sort_count));
            cmd_elements++;
            cmd_sizes[cmd_elements] = spprintf(&cmd_lines[cmd_elements], 0, "%d", (int)sort_count);
            cmd_elements++;
    }
    if(get && get_len) {
            /* GET */
            cmd_lines[cmd_elements] = estrdup("$3");
            cmd_sizes[cmd_elements] = 2;
            cmd_elements++;
            cmd_lines[cmd_elements] = estrdup("GET");
            cmd_sizes[cmd_elements] = 3;
            cmd_elements++;

            /* pattern */
            cmd_sizes[cmd_elements] = redis_cmd_format(&cmd_lines[cmd_elements], "$%d", get_len);
            cmd_elements++;
            cmd_lines[cmd_elements] = emalloc(get_len + 1);
            memcpy(cmd_lines[cmd_elements], get, get_len);
            cmd_lines[cmd_elements][get_len] = 0;
            cmd_sizes[cmd_elements] = get_len;
            cmd_elements++;
    }

    /* add ASC or DESC */
    sort_len = strlen(sort);
    cmd_sizes[cmd_elements] = redis_cmd_format(&cmd_lines[cmd_elements], "$%d", sort_len);
    cmd_elements++;
    cmd_lines[cmd_elements] = emalloc(sort_len + 1);
    memcpy(cmd_lines[cmd_elements], sort, sort_len);
    cmd_lines[cmd_elements][sort_len] = 0;
    cmd_sizes[cmd_elements] = sort_len;
    cmd_elements++;

    if(use_alpha) {
            /* ALPHA */
            cmd_lines[cmd_elements] = estrdup("$5");
            cmd_sizes[cmd_elements] = 2;
            cmd_elements++;
            cmd_lines[cmd_elements] = estrdup("ALPHA");
            cmd_sizes[cmd_elements] = 5;
            cmd_elements++;
    }
    if(store && store_len) {
            /* STORE */
            cmd_lines[cmd_elements] = estrdup("$5");
            cmd_sizes[cmd_elements] = 2;
            cmd_elements++;
            cmd_lines[cmd_elements] = estrdup("STORE");
            cmd_sizes[cmd_elements] = 5;
            cmd_elements++;

            /* store key */
            cmd_sizes[cmd_elements] = redis_cmd_format(&cmd_lines[cmd_elements], "$%d", store_len);
            cmd_elements++;
            cmd_lines[cmd_elements] = emalloc(store_len + 1);
            memcpy(cmd_lines[cmd_elements], store, store_len);
            cmd_lines[cmd_elements][store_len] = 0;
            cmd_sizes[cmd_elements] = store_len;
            cmd_elements++;
    }

    /* first line has the star */
    cmd_sizes[0] = spprintf(&cmd_lines[0], 0, "*%d", (cmd_elements-1)/2);

    /* compute the command size */
    cmd_len = 0;
    for(i = 0; i < cmd_elements; ++i) {
            cmd_len += cmd_sizes[i] + sizeof(_NL) - 1; /* each line followeb by _NL */
    }

    /* copy all lines into the final command. */
    cmd = emalloc(1 + cmd_len);
    pos = 0;
    for(i = 0; i < cmd_elements; ++i) {
        memcpy(cmd + pos, cmd_lines[i], cmd_sizes[i]);
        pos += cmd_sizes[i];
        memcpy(cmd + pos, _NL, sizeof(_NL) - 1);
        pos += sizeof(_NL) - 1;

        /* free every line */
        efree(cmd_lines[i]);
    }

	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
	IF_ATOMIC() {
    	if (redis_sock_read_multibulk_reply(INTERNAL_FUNCTION_PARAM_PASSTHRU,
											redis_sock, NULL, NULL) < 0) {
        	RETURN_FALSE;
	    }
	}
	REDIS_PROCESS_RESPONSE(redis_sock_read_multibulk_reply);

}

/* {{{ proto array Redis::sortAsc(string key, string pattern, string get, int start, int end, bool getList])
 */
PHP_METHOD(Redis, sortAsc)
{
    generic_sort_cmd(INTERNAL_FUNCTION_PARAM_PASSTHRU, "ASC", 0);
}
/* }}} */

/* {{{ proto array Redis::sortAscAlpha(string key, string pattern, string get, int start, int end, bool getList])
 */
PHP_METHOD(Redis, sortAscAlpha)
{
    generic_sort_cmd(INTERNAL_FUNCTION_PARAM_PASSTHRU, "ASC", 1);
}
/* }}} */

/* {{{ proto array Redis::sortDesc(string key, string pattern, string get, int start, int end, bool getList])
 */
PHP_METHOD(Redis, sortDesc)
{
    generic_sort_cmd(INTERNAL_FUNCTION_PARAM_PASSTHRU, "DESC", 0);
}
/* }}} */

/* {{{ proto array Redis::sortDescAlpha(string key, string pattern, string get, int start, int end, bool getList])
 */
PHP_METHOD(Redis, sortDescAlpha)
{
    generic_sort_cmd(INTERNAL_FUNCTION_PARAM_PASSTHRU, "DESC", 1);
}
/* }}} */

PHP_REDIS_API void generic_expire_cmd(INTERNAL_FUNCTION_PARAMETERS, char *keyword, int keyword_len) {
    zval *object;
    RedisSock *redis_sock;
    zend_string *key; char *cmd, *t;
    int cmd_len, t_len;
	int i;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OSs",
                                     &object, redis_ce, &key,
                                     &t, &t_len) == FAILURE) {
        RETURN_FALSE;
    }

    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

	/* check that we have a number */
	for(i = 0; i < t_len; ++i)
		if(t[i] < '0' || t[i] > '9')
			RETURN_FALSE;

	key = redis_key_prefix(redis_sock, key);
    cmd_len = redis_cmd_format_static(&cmd, keyword, "ss", key->val, key->len, t, t_len);
	zend_string_release(key);

	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
	IF_ATOMIC() {
		redis_1_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
	}
	REDIS_PROCESS_RESPONSE(redis_1_response);
}

/* {{{ proto array Redis::setTimeout(string key, int timeout)
 */
PHP_METHOD(Redis, setTimeout) {
    generic_expire_cmd(INTERNAL_FUNCTION_PARAM_PASSTHRU, "EXPIRE", sizeof("EXPIRE")-1);
}

PHP_METHOD(Redis, pexpire) {
    generic_expire_cmd(INTERNAL_FUNCTION_PARAM_PASSTHRU, "PEXPIRE", sizeof("PEXPIRE")-1);
}
/* }}} */

/* {{{ proto array Redis::expireAt(string key, int timestamp)
 */
PHP_METHOD(Redis, expireAt) {
    generic_expire_cmd(INTERNAL_FUNCTION_PARAM_PASSTHRU, "EXPIREAT", sizeof("EXPIREAT")-1);
}
/* }}} */

/* {{{ proto array Redis::pexpireAt(string key, int timestamp)
 */
PHP_METHOD(Redis, pexpireAt) {
    generic_expire_cmd(INTERNAL_FUNCTION_PARAM_PASSTHRU, "PEXPIREAT", sizeof("PEXPIREAT")-1);
}
/* }}} */


/* {{{ proto array Redis::lSet(string key, int index, string value)
 */
PHP_METHOD(Redis, lSet) {

    zval *object;
    RedisSock *redis_sock;

    char *cmd;
    int cmd_len;
    long index;
    zend_string *key, *val;
    zval *z_value;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OSlz",
                                     &object, redis_ce, &key, &index, &z_value) == FAILURE) {
        RETURN_FALSE;
    }

    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

    val = redis_serialize(redis_sock, z_value);
	key = redis_key_prefix(redis_sock, key);
    cmd_len = redis_cmd_format_static(&cmd, "LSET", "sds", key->val, key->len, index, val->val, val->len);
    zend_string_release(val);
    zend_string_release(key);

	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
	IF_ATOMIC() {
		redis_boolean_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
	}
	REDIS_PROCESS_RESPONSE(redis_boolean_response);
}
/* }}} */

PHP_REDIS_API void generic_empty_cmd_impl(INTERNAL_FUNCTION_PARAMETERS, char *cmd, int cmd_len, ResultCallback result_callback) {
    zval *object;
    RedisSock *redis_sock;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "O",
                                     &object, redis_ce) == FAILURE) {
        RETURN_FALSE;
    }

    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
    IF_ATOMIC() {
        result_callback(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
    }
    REDIS_PROCESS_RESPONSE(result_callback);
}

PHP_REDIS_API void generic_empty_cmd(INTERNAL_FUNCTION_PARAMETERS, char *cmd, int cmd_len, ...) {
    generic_empty_cmd_impl(INTERNAL_FUNCTION_PARAM_PASSTHRU, cmd, cmd_len, redis_boolean_response);
}

/* {{{ proto string Redis::save()
 */
PHP_METHOD(Redis, save)
{
    char *cmd;
    int cmd_len = redis_cmd_format_static(&cmd, "SAVE", "");
    generic_empty_cmd(INTERNAL_FUNCTION_PARAM_PASSTHRU, cmd, cmd_len);

}
/* }}} */

/* {{{ proto string Redis::bgSave()
 */
PHP_METHOD(Redis, bgSave)
{
    char *cmd;
    int cmd_len = redis_cmd_format_static(&cmd, "BGSAVE", "");
    generic_empty_cmd(INTERNAL_FUNCTION_PARAM_PASSTHRU, cmd, cmd_len);

}
/* }}} */

PHP_REDIS_API void generic_empty_long_cmd(INTERNAL_FUNCTION_PARAMETERS, char *cmd, int cmd_len, ...) {

    zval *object;
    RedisSock *redis_sock;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "O",
                                     &object, redis_ce) == FAILURE) {
        RETURN_FALSE;
    }

    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
    IF_ATOMIC() {
	  redis_long_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
    }
    REDIS_PROCESS_RESPONSE(redis_long_response);
}

/* {{{ proto integer Redis::lastSave()
 */
PHP_METHOD(Redis, lastSave)
{
    char *cmd;
    int cmd_len = redis_cmd_format_static(&cmd, "LASTSAVE", "");
    generic_empty_long_cmd(INTERNAL_FUNCTION_PARAM_PASSTHRU, cmd, cmd_len);
}
/* }}} */


/* {{{ proto bool Redis::flushDB()
 */
PHP_METHOD(Redis, flushDB)
{
    char *cmd;
    int cmd_len = redis_cmd_format_static(&cmd, "FLUSHDB", "");
    generic_empty_cmd(INTERNAL_FUNCTION_PARAM_PASSTHRU, cmd, cmd_len);
}
/* }}} */

/* {{{ proto bool Redis::flushAll()
 */
PHP_METHOD(Redis, flushAll)
{
    char *cmd;
    int cmd_len = redis_cmd_format_static(&cmd, "FLUSHALL", "");
    generic_empty_cmd(INTERNAL_FUNCTION_PARAM_PASSTHRU, cmd, cmd_len);
}
/* }}} */

/* {{{ proto int Redis::dbSize()
 */
PHP_METHOD(Redis, dbSize)
{
    char *cmd;
    int cmd_len = redis_cmd_format_static(&cmd, "DBSIZE", "");
    generic_empty_long_cmd(INTERNAL_FUNCTION_PARAM_PASSTHRU, cmd, cmd_len);
}
/* }}} */

/* {{{ proto bool Redis::auth(string passwd)
 */
PHP_METHOD(Redis, auth) {

    zval *object;
    RedisSock *redis_sock;

    char *cmd, *password;
    int cmd_len, password_len;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "Os",
                                     &object, redis_ce, &password, &password_len) == FAILURE) {
        RETURN_FALSE;
    }

    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

    cmd_len = redis_cmd_format_static(&cmd, "AUTH", "s", password, password_len);

    /* Free previously stored auth if we have one, and store this password */
    if(redis_sock->auth) efree(redis_sock->auth);
    redis_sock->auth = estrndup(password, password_len);

	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
	IF_ATOMIC() {
		redis_boolean_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
	}
	REDIS_PROCESS_RESPONSE(redis_boolean_response);
}
/* }}} */

/* {{{ proto long Redis::persist(string key)
 */
PHP_METHOD(Redis, persist) {

    zval *object;
    RedisSock *redis_sock;

    zend_string *key;
    char *cmd;
    int cmd_len;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OS",
                                     &object, redis_ce, &key) == FAILURE) {
        RETURN_FALSE;
    }

    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

	key = redis_key_prefix(redis_sock, key);
    cmd_len = redis_cmd_format_static(&cmd, "PERSIST", "s", key->val, key->len);
	zend_string_release(key);

	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
	IF_ATOMIC() {
	  redis_1_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
	}
	REDIS_PROCESS_RESPONSE(redis_1_response);
}
/* }}} */

PHP_REDIS_API void generic_ttl(INTERNAL_FUNCTION_PARAMETERS, char *keyword) {
    zval *object;
    RedisSock *redis_sock;

    zend_string *key;
    char *cmd;
    int cmd_len;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OS",
                                     &object, redis_ce, &key) == FAILURE) {
        RETURN_FALSE;
    }

    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

	key = redis_key_prefix(redis_sock, key);
    cmd_len = redis_cmd_format_static(&cmd, keyword, "s", key->val, key->len);
	zend_string_release(key);

	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
	IF_ATOMIC() {
	  redis_long_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
	}
	REDIS_PROCESS_RESPONSE(redis_long_response);
}

/* {{{ proto long Redis::ttl(string key)
 */
PHP_METHOD(Redis, ttl) {
	generic_ttl(INTERNAL_FUNCTION_PARAM_PASSTHRU, "TTL");
}
/* }}} */

/* {{{ proto long Redis::pttl(string key)
 */
PHP_METHOD(Redis, pttl) {
	generic_ttl(INTERNAL_FUNCTION_PARAM_PASSTHRU, "PTTL");
}
/* }}} */

/* {{{ proto array Redis::info()
 */
PHP_METHOD(Redis, info) {

    zval *object;
    RedisSock *redis_sock;
    char *cmd, *opt = NULL;
    int cmd_len, opt_len;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "O|s",
                                     &object, redis_ce, &opt, &opt_len) == FAILURE) {
        RETURN_FALSE;
    }

    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

    /* Build a standalone INFO command or one with an option */
    if(opt != NULL) {
    	cmd_len = redis_cmd_format_static(&cmd, "INFO", "s", opt, opt_len);
    } else {
    	cmd_len = redis_cmd_format_static(&cmd, "INFO", "");
    }

	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
	IF_ATOMIC() {
	  redis_info_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
	}
	REDIS_PROCESS_RESPONSE(redis_info_response);

}
/* }}} */

/* {{{ proto string Redis::resetStat()
 */
PHP_METHOD(Redis, resetStat)
{
	char *cmd;
	int cmd_len = redis_cmd_format_static(&cmd, "CONFIG", "s", "RESETSTAT", 9);
	generic_empty_cmd(INTERNAL_FUNCTION_PARAM_PASSTHRU, cmd, cmd_len);
}
/* }}} */

/* {{{ proto bool Redis::select(long dbNumber)
 */
PHP_METHOD(Redis, select) {

    zval *object;
    RedisSock *redis_sock;

    char *cmd;
    int cmd_len;
    long dbNumber;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "Ol",
                                     &object, redis_ce, &dbNumber) == FAILURE) {
        RETURN_FALSE;
    }

    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

    redis_sock->dbNumber = dbNumber;

    cmd_len = redis_cmd_format_static(&cmd, "SELECT", "d", dbNumber);

	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
	IF_ATOMIC() {
		redis_boolean_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
	}
	REDIS_PROCESS_RESPONSE(redis_boolean_response);
}
/* }}} */

/* {{{ proto bool Redis::move(string key, long dbindex)
 */
PHP_METHOD(Redis, move) {

    zval *object;
    RedisSock *redis_sock;

    zend_string *key;
    char *cmd;
    int cmd_len;
    long dbNumber;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OSl",
                                     &object, redis_ce, &key, &dbNumber) == FAILURE) {
        RETURN_FALSE;
    }

    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

	key = redis_key_prefix(redis_sock, key);
    cmd_len = redis_cmd_format_static(&cmd, "MOVE", "sd", key->val, key->len, dbNumber);
	zend_string_release(key);

	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
	IF_ATOMIC() {
	  redis_1_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
	}
	REDIS_PROCESS_RESPONSE(redis_1_response);

}
/* }}} */

PHP_REDIS_API void
generic_mset(INTERNAL_FUNCTION_PARAMETERS, char *kw, void (*fun)(INTERNAL_FUNCTION_PARAMETERS, RedisSock *, zval *, void *)) {

    zval *object;
    RedisSock *redis_sock;

    char *cmd = NULL, *p = NULL;
    int cmd_len = 0, argc = 0, kw_len = strlen(kw);
	int step = 0;	/* 0: compute size; 1: copy strings. */
    zval *z_array;

	HashTable *keytable;
	zend_string *key, *val;
	zval *z_value_p;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "Oa",
                                     &object, redis_ce, &z_array) == FAILURE) {
        RETURN_FALSE;
    }

    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

    if(zend_hash_num_elements(Z_ARRVAL_P(z_array)) == 0) {
        RETURN_FALSE;
    }

	for(step = 0; step < 2; ++step) {
		if(step == 1) {
			cmd_len += 1 + integer_length(1 + 2 * argc) + 2;	/* star + arg count + NL */
			cmd_len += 1 + integer_length(kw_len) + 2;			/* dollar + strlen(kw) + NL */
			cmd_len += kw_len + 2;								/* kw + NL */

			p = cmd = emalloc(cmd_len + 1);	/* alloc */
			p += sprintf(cmd, "*%d" _NL "$%d" _NL "%s" _NL, 1 + 2 * argc, kw_len, kw); /* copy header */
		}

		keytable = Z_ARRVAL_P(z_array);
		ZEND_HASH_FOREACH_STR_KEY_VAL(keytable, key, z_value_p) {
			if(step == 0)
				argc++; /* found a valid arg */

			val = redis_serialize(redis_sock, z_value_p);
			key = redis_key_prefix(redis_sock, key);

			if(step == 0) { /* counting */
				cmd_len += 1 + integer_length(key->len) + 2
						+ key->len + 2
						+ 1 + integer_length(val->len) + 2
						+ val->len + 2;
			} else {
				p += sprintf(p, "$%d" _NL, key->len);	/* key len */
				memcpy(p, key->val, key->len); p += key->len;	/* key */
				memcpy(p, _NL, 2); p += 2;

				p += sprintf(p, "$%d" _NL, val->len);	/* val len */
				memcpy(p, val->val, val->len); p += val->len;	/* val */
				memcpy(p, _NL, 2); p += 2;
			}

			zend_string_release(val);
			zend_string_release(key);
		} ZEND_HASH_FOREACH_END();
	}

	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);

	IF_ATOMIC() {
		fun(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
	}
	REDIS_PROCESS_RESPONSE(fun);
}

/* {{{ proto bool Redis::mset(array (key => value, ...))
 */
PHP_METHOD(Redis, mset) {
	generic_mset(INTERNAL_FUNCTION_PARAM_PASSTHRU, "MSET", redis_boolean_response);
}
/* }}} */


/* {{{ proto bool Redis::msetnx(array (key => value, ...))
 */
PHP_METHOD(Redis, msetnx) {
	generic_mset(INTERNAL_FUNCTION_PARAM_PASSTHRU, "MSETNX", redis_1_response);
}
/* }}} */

PHP_REDIS_API void common_rpoplpush(INTERNAL_FUNCTION_PARAMETERS, RedisSock *redis_sock,
		zend_string *srckey, zend_string *dstkey, int timeout) {

	char *cmd;
	int cmd_len;

	srckey = redis_key_prefix(redis_sock, srckey);
	dstkey = redis_key_prefix(redis_sock, dstkey);
	if(timeout < 0) {
		cmd_len = redis_cmd_format_static(&cmd, "RPOPLPUSH", "ss", srckey->val, srckey->len, dstkey->val, dstkey->len);
	} else {
		cmd_len = redis_cmd_format_static(&cmd, "BRPOPLPUSH", "ssd", srckey->val, srckey->len, dstkey->val, dstkey->len, timeout);
	}
	zend_string_release(srckey);
	zend_string_release(dstkey);

	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
	IF_ATOMIC() {
		redis_string_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
	}
	REDIS_PROCESS_RESPONSE(redis_string_response);

}

/* {{{ proto string Redis::rpoplpush(string srckey, string dstkey)
 */
PHP_METHOD(Redis, rpoplpush)
{
    zval *object;
    RedisSock *redis_sock;
    zend_string *srckey, *dstkey;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OSS",
                                     &object, redis_ce, &srckey, &dstkey) == FAILURE) {
        RETURN_FALSE;
    }

    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

	common_rpoplpush(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, srckey, dstkey, -1);
}
/* }}} */

/* {{{ proto string Redis::brpoplpush(string srckey, string dstkey)
 */
PHP_METHOD(Redis, brpoplpush)
{
    zval *object;
    RedisSock *redis_sock;
    zend_string *srckey, *dstkey;
	long timeout = 0;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OSSl",
                                     &object, redis_ce, &srckey, &dstkey,
                                     &timeout) == FAILURE) {
        RETURN_FALSE;
    }

    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

	common_rpoplpush(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, srckey, dstkey, timeout);
}
/* }}} */

/* {{{ proto long Redis::zAdd(string key, int score, string value)
 */
PHP_METHOD(Redis, zAdd) {

    RedisSock *redis_sock;

	zend_string *key, *val, *dbl_str;
    char *cmd;
    int cmd_len;
    double score;
    smart_string buf = {0};

	zval *z_args;
	int argc = ZEND_NUM_ARGS(), i;

	/* get redis socket */
    if (redis_sock_get(getThis(), &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

    z_args = emalloc(argc * sizeof(zval));
    if(zend_get_parameters_array_ex(argc, z_args) == FAILURE) {
        efree(z_args);
		RETURN_FALSE;
    }

	/* need key, score, value, [score, value...] */
	if(argc > 1) {
		convert_to_string(&z_args[0]); /* required string */
	}
	if(argc < 3 || Z_TYPE(z_args[0]) != IS_STRING || (argc-1) % 2 != 0) {
		efree(z_args);
		RETURN_FALSE;
	}

	/* possibly serialize key */
	key = redis_key_prefix(redis_sock, Z_STR(z_args[0]));

	/* start building the command */
	smart_string_appendc(&buf, '*');
	smart_string_append_long(&buf, argc + 1); /* +1 for ZADD command */
	smart_string_appendl(&buf, _NL, sizeof(_NL) - 1);

	/* add command name */
	smart_string_appendc(&buf, '$');
	smart_string_append_long(&buf, 4);
	smart_string_appendl(&buf, _NL, sizeof(_NL) - 1);
	smart_string_appendl(&buf, "ZADD", 4);
	smart_string_appendl(&buf, _NL, sizeof(_NL) - 1);

	/* add key */
	smart_string_appendc(&buf, '$');
	smart_string_append_long(&buf, key->len);
	smart_string_appendl(&buf, _NL, sizeof(_NL) - 1);
	smart_string_appendl(&buf, key->val, key->len);
	smart_string_appendl(&buf, _NL, sizeof(_NL) - 1);

	for(i = 1; i < argc; i +=2) {
		/* add score */
		score = zval_get_double(&z_args[i]);
		REDIS_DOUBLE_TO_STRING(dbl_str, score)
		smart_string_appendc(&buf, '$');
		smart_string_append_long(&buf, dbl_str->len);
		smart_string_appendl(&buf, _NL, sizeof(_NL) - 1);
		smart_string_appendl(&buf, dbl_str->val, dbl_str->len);
		smart_string_appendl(&buf, _NL, sizeof(_NL) - 1);
		zend_string_release(dbl_str);

        /* possibly serialize value. */
		val = redis_serialize(redis_sock, &z_args[i+1]);

		/* add value */
		smart_string_appendc(&buf, '$');
		smart_string_append_long(&buf, val->len);
		smart_string_appendl(&buf, _NL, sizeof(_NL) - 1);
		smart_string_appendl(&buf, val->val, val->len);
		smart_string_appendl(&buf, _NL, sizeof(_NL) - 1);

		zend_string_release(val);
	}

	/* end string */
	smart_string_0(&buf);
	cmd = buf.c;
	cmd_len = buf.len;
    zend_string_release(key);

	/* cleanup */
	efree(z_args);

	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
	IF_ATOMIC() {
		redis_long_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
	}
	REDIS_PROCESS_RESPONSE(redis_long_response);
}
/* }}} */
/* {{{ proto array Redis::zRange(string key, int start , int end, bool withscores = FALSE)
 */
PHP_METHOD(Redis, zRange)
{
    zval *object;
    RedisSock *redis_sock;
    zend_string *key;
    char *cmd;
    int cmd_len;
    long start, end;
    long withscores = 0;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OSll|b",
                                     &object, redis_ce,
                                     &key, &start, &end, &withscores) == FAILURE) {
        RETURN_FALSE;
    }

    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

	key = redis_key_prefix(redis_sock, key);
    if(withscores) {
        cmd_len = redis_cmd_format_static(&cmd, "ZRANGE", "sdds", key->val, key->len, start, end, "WITHSCORES", 10);
    } else {
        cmd_len = redis_cmd_format_static(&cmd, "ZRANGE", "sdd", key->val, key->len, start, end);
    }
	zend_string_release(key);

	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
    if(withscores) {
            IF_ATOMIC() {
                redis_mbulk_reply_zipped_keys_dbl(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
            }
            REDIS_PROCESS_RESPONSE(redis_mbulk_reply_zipped_keys_dbl);
    } else {
            IF_ATOMIC() {
                if (redis_sock_read_multibulk_reply(INTERNAL_FUNCTION_PARAM_PASSTHRU,
                                                    redis_sock, NULL, NULL) < 0) {
                    RETURN_FALSE;
                }
            }
            REDIS_PROCESS_RESPONSE(redis_sock_read_multibulk_reply);
    }
}
/* }}} */
/* {{{ proto long Redis::zDelete(string key, string member)
 */
PHP_METHOD(Redis, zDelete)
{
    RedisSock *redis_sock;

    if(FAILURE == generic_multiple_args_cmd(INTERNAL_FUNCTION_PARAM_PASSTHRU,
                    "ZREM", sizeof("ZREM") - 1,
					2, &redis_sock, 0, 0, 1))
		return;

	IF_ATOMIC() {
	  redis_long_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
	}
	REDIS_PROCESS_RESPONSE(redis_long_response);
}
/* }}} */
/* {{{ proto long Redis::zDeleteRangeByScore(string key, string start, string end)
 */
PHP_METHOD(Redis, zDeleteRangeByScore)
{
    zval *object;
    RedisSock *redis_sock;
    zend_string *key;
    char *cmd;
    int cmd_len;
    char *start, *end;
    int start_len, end_len;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OSss",
                                     &object, redis_ce, &key, &start, &start_len,
                                     &end, &end_len) == FAILURE) {
        RETURN_FALSE;
    }

    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

	key = redis_key_prefix(redis_sock, key);
    cmd_len = redis_cmd_format_static(&cmd, "ZREMRANGEBYSCORE", "sss", key->val, key->len, start, start_len, end, end_len);
	zend_string_release(key);

	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
	IF_ATOMIC() {
		redis_long_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
	}
	REDIS_PROCESS_RESPONSE(redis_long_response);

}
/* }}} */

/* {{{ proto long Redis::zDeleteRangeByRank(string key, long start, long end)
 */
PHP_METHOD(Redis, zDeleteRangeByRank)
{
    zval *object;
    RedisSock *redis_sock;
    zend_string *key;
    char *cmd;
    int cmd_len;
    long start, end;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OSll",
                                     &object, redis_ce,
                                     &key, &start, &end) == FAILURE) {
        RETURN_FALSE;
    }

    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

	key = redis_key_prefix(redis_sock, key);
    cmd_len = redis_cmd_format_static(&cmd, "ZREMRANGEBYRANK", "sdd", key->val, key->len, (int)start, (int)end);
	zend_string_release(key);

	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
	IF_ATOMIC() {
		redis_long_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
	}
	REDIS_PROCESS_RESPONSE(redis_long_response);

}
/* }}} */

/* {{{ proto array Redis::zReverseRange(string key, int start , int end, bool withscores = FALSE)
 */
PHP_METHOD(Redis, zReverseRange)
{
    zval *object;
    RedisSock *redis_sock;
    zend_string *key;
    char *cmd;
    int cmd_len;
    long start, end;
    long withscores = 0;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OSll|b",
                                     &object, redis_ce,
                                     &key, &start, &end, &withscores) == FAILURE) {
        RETURN_FALSE;
    }

    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

	key = redis_key_prefix(redis_sock, key);
    if(withscores) {
        cmd_len = redis_cmd_format_static(&cmd, "ZREVRANGE", "sdds", key->val, key->len, start, end, "WITHSCORES", 10);
    } else {
        cmd_len = redis_cmd_format_static(&cmd, "ZREVRANGE", "sdd", key->val, key->len, start, end);
    }
	zend_string_release(key);

    REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
    if(withscores) {
    	IF_ATOMIC() {
            redis_mbulk_reply_zipped_keys_dbl(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
    	}
        REDIS_PROCESS_RESPONSE(redis_mbulk_reply_zipped_keys_dbl);
    } else {
    	IF_ATOMIC() {
            if (redis_sock_read_multibulk_reply(INTERNAL_FUNCTION_PARAM_PASSTHRU,
                                                redis_sock, NULL, NULL) < 0) {
                RETURN_FALSE;
            }
    	}
    	REDIS_PROCESS_RESPONSE(redis_sock_read_multibulk_reply);
    }
}
/* }}} */

PHP_REDIS_API void
redis_generic_zrange_by_score(INTERNAL_FUNCTION_PARAMETERS, char *keyword) {

    zval *object, *z_options = NULL, *z_limit_val_p = NULL, *z_withscores_val_p = NULL;

    RedisSock *redis_sock;
    zend_string *key;
    char *cmd;
    int cmd_len;
    zend_bool withscores = 0;
    char *start, *end;
    int start_len, end_len;
    int has_limit = 0;
    long limit_low, limit_high;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OSss|a",
                                     &object, redis_ce,
                                     &key,
                                     &start, &start_len,
                                     &end, &end_len,
                                     &z_options) == FAILURE) {
        RETURN_FALSE;
    }

    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

    /* options */
    if (z_options && Z_TYPE_P(z_options) == IS_ARRAY) {
        /* add scores */
        z_withscores_val_p = zend_hash_str_find(Z_ARRVAL_P(z_options), "withscores", sizeof("withscores") - 1);
        withscores = (z_withscores_val_p ? Z_TYPE_P(z_withscores_val_p) == IS_TRUE : 0);

        /* limit offset, count:
           z_limit_val_pp points to an array($longFrom, $longCount)
        */
        if (z_limit_val_p = zend_hash_str_find(Z_ARRVAL_P(z_options), "limit", sizeof("limit") - 1)) {
            if(zend_hash_num_elements(Z_ARRVAL_P(z_limit_val_p)) == 2) {
                zval *z_offset_p, *z_count_p;
                /* get the two values from the table, check that they are indeed of LONG type */
                if((z_offset_p = zend_hash_index_find(Z_ARRVAL_P(z_limit_val_p), 0)) &&
                  (z_count_p = zend_hash_index_find(Z_ARRVAL_P(z_limit_val_p), 1)) &&
                  Z_TYPE_P(z_offset_p) == IS_LONG &&
                  Z_TYPE_P(z_count_p) == IS_LONG) {

                    has_limit = 1;
                    limit_low = Z_LVAL_P(z_offset_p);
                    limit_high = Z_LVAL_P(z_count_p);
                }
            }
        }
    }

	key = redis_key_prefix(redis_sock, key);
    if(withscores) {
        if(has_limit) {
            cmd_len = redis_cmd_format_static(&cmd, keyword, "ssssdds",
                            key->val, key->len, start, start_len, end, end_len, "LIMIT", 5, limit_low, limit_high, "WITHSCORES", 10);
        } else {
            cmd_len = redis_cmd_format_static(&cmd, keyword, "ssss",
                            key->val, key->len, start, start_len, end, end_len, "WITHSCORES", 10);
        }
    } else {
        if(has_limit) {
            cmd_len = redis_cmd_format_static(&cmd, keyword, "ssssdd",
                            key->val, key->len, start, start_len, end, end_len, "LIMIT", 5, limit_low, limit_high);
        } else {
            cmd_len = redis_cmd_format_static(&cmd, keyword, "sss", key->val, key->len, start, start_len, end, end_len);
        }
    }
	zend_string_release(key);

    REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
    if(withscores) {
            /* with scores! we have to transform the return array.
             * return_value currently holds this: [elt0, val0, elt1, val1 ... ]
             * we want [elt0 => val0, elt1 => val1], etc.
             */
            IF_ATOMIC() {
                if(redis_mbulk_reply_zipped_keys_dbl(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL) < 0) {
                    RETURN_FALSE;
                }
            }
            REDIS_PROCESS_RESPONSE(redis_mbulk_reply_zipped_keys_dbl);
    } else {
            IF_ATOMIC() {
                if(redis_sock_read_multibulk_reply(INTERNAL_FUNCTION_PARAM_PASSTHRU,
                                                    redis_sock, NULL, NULL) < 0) {
                    RETURN_FALSE;
                }
            }
            REDIS_PROCESS_RESPONSE(redis_sock_read_multibulk_reply);
    }
}

/* {{{ proto array Redis::zRangeByScore(string key, string start , string end [,array options = NULL])
 */
PHP_METHOD(Redis, zRangeByScore)
{
	redis_generic_zrange_by_score(INTERNAL_FUNCTION_PARAM_PASSTHRU, "ZRANGEBYSCORE");
}
/* }}} */
/* {{{ proto array Redis::zRevRangeByScore(string key, string start , string end [,array options = NULL])
 */
PHP_METHOD(Redis, zRevRangeByScore)
{
	redis_generic_zrange_by_score(INTERNAL_FUNCTION_PARAM_PASSTHRU, "ZREVRANGEBYSCORE");
}

/* {{{ proto array Redis::zCount(string key, string start , string end)
 */
PHP_METHOD(Redis, zCount)
{
    zval *object;

    RedisSock *redis_sock;
    zend_string *key;
    char *cmd;
    int cmd_len;
    char *start, *end;
    int start_len, end_len;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OSss",
                                     &object, redis_ce,
                                     &key,
                                     &start, &start_len,
                                     &end, &end_len) == FAILURE) {
        RETURN_FALSE;
    }

    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

	key = redis_key_prefix(redis_sock, key);
    cmd_len = redis_cmd_format_static(&cmd, "ZCOUNT", "sss", key->val, key->len, start, start_len, end, end_len);
	zend_string_release(key);

	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
	IF_ATOMIC() {
	  redis_long_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
	}
	REDIS_PROCESS_RESPONSE(redis_long_response);
}
/* }}} */

/* {{{ proto array Redis::zRangeByLex(string $key, string $min, string $max,
 *                                    [long $offset, long $count]) */
PHP_METHOD(Redis, zRangeByLex) {
    zval *object;
    RedisSock *redis_sock;
    zend_string *key;
    char *cmd, *min, *max;
    long offset, count;
    int argc, cmd_len;
    int min_len, max_len;

    /* We need either three or five arguments for this to be a valid call */
    argc = ZEND_NUM_ARGS();
    if (argc != 3 && argc != 5) {
        RETURN_FALSE;
    }

    if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(),
                                     "OSss|ll", &object, redis_ce, &key,
                                     &min, &min_len, &max, &max_len, &offset,
                                     &count) == FAILURE)
    {
        RETURN_FALSE;
    }

    /* We can do some simple validation for the user, as we know how min/max are
     * required to start */
    if (!IS_LEX_ARG(min,min_len) || !IS_LEX_ARG(max,max_len)) {
        RETURN_FALSE;
    }

    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

    key = redis_key_prefix(redis_sock, key);

    /* Construct our command depending on argc */
    if (argc == 3) {
        cmd_len = redis_cmd_format_static(&cmd, "ZRANGEBYLEX", "sss", key->val, key->len,
            min, min_len, max, max_len);
    } else {
        cmd_len = redis_cmd_format_static(&cmd, "ZRANGEBYLEX", "ssssll", key->val, key->len,
            min, min_len, max, max_len, "LIMIT", sizeof("LIMIT")-1,
            offset, count);
    }

    zend_string_release(key);

    /* Kick it off */
    REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
    IF_ATOMIC() {
        if (redis_sock_read_multibulk_reply(INTERNAL_FUNCTION_PARAM_PASSTHRU,
                                            redis_sock, NULL, NULL) < 0) {
            RETURN_FALSE;
        }
    }
    REDIS_PROCESS_RESPONSE(redis_sock_read_multibulk_reply);
}

/* {{{ proto long Redis::zCard(string key)
 */
PHP_METHOD(Redis, zCard)
{
    zval *object;
    RedisSock *redis_sock;
    zend_string *key;
    char *cmd;
    int cmd_len;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OS",
                                     &object, redis_ce,
                                     &key) == FAILURE) {
        RETURN_FALSE;
    }

    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

	key = redis_key_prefix(redis_sock, key);
    cmd_len = redis_cmd_format_static(&cmd, "ZCARD", "s", key->val, key->len);
	zend_string_release(key);

	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
	IF_ATOMIC() {
	  redis_long_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
	}
	REDIS_PROCESS_RESPONSE(redis_long_response);

}
/* }}} */

/* {{{ proto double Redis::zScore(string key, mixed member)
 */
PHP_METHOD(Redis, zScore)
{
    zval *object;
    RedisSock *redis_sock;
    zend_string *key, *val;
    char *cmd;
    int cmd_len;
    zval *z_value;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OSz",
                                     &object, redis_ce, &key,
                                     &z_value) == FAILURE) {
        RETURN_FALSE;
    }

    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

    val = redis_serialize(redis_sock, z_value);
	key = redis_key_prefix(redis_sock, key);
    cmd_len = redis_cmd_format_static(&cmd, "ZSCORE", "ss", key->val, key->len, val->val, val->len);
    zend_string_release(val);
    zend_string_release(key);

	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
	IF_ATOMIC() {
	    redis_bulk_double_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
	}
	REDIS_PROCESS_RESPONSE(redis_bulk_double_response);
}
/* }}} */


PHP_REDIS_API void generic_rank_method(INTERNAL_FUNCTION_PARAMETERS, char *keyword, int keyword_len) {
    zval *object;
    RedisSock *redis_sock;
    zend_string *key, *val;
    char *cmd;
    int cmd_len;
    zval *z_value;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OSz",
                                     &object, redis_ce, &key,
                                     &z_value) == FAILURE) {
        RETURN_FALSE;
    }

    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

    val = redis_serialize(redis_sock, z_value);
	key = redis_key_prefix(redis_sock, key);
    cmd_len = redis_cmd_format_static(&cmd, keyword, "ss", key->val, key->len, val->val, val->len);
    zend_string_release(val);
    zend_string_release(key);

	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
	IF_ATOMIC() {
	    redis_long_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
	}
	REDIS_PROCESS_RESPONSE(redis_long_response);
}


/* {{{ proto long Redis::zRank(string key, string member)
 */
PHP_METHOD(Redis, zRank) {

        generic_rank_method(INTERNAL_FUNCTION_PARAM_PASSTHRU, "ZRANK", 5);
}
/* }}} */

/* {{{ proto long Redis::zRevRank(string key, string member)
 */
PHP_METHOD(Redis, zRevRank) {

        generic_rank_method(INTERNAL_FUNCTION_PARAM_PASSTHRU, "ZREVRANK", 8);
}
/* }}} */

PHP_REDIS_API void generic_incrby_method(INTERNAL_FUNCTION_PARAMETERS, char *keyword, int keyword_len) {
    zval *object;
    RedisSock *redis_sock;

    zend_string *key, *val;
    char *cmd;
    int cmd_len;
    double add;
    zval *z_value;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OSdz",
                                     &object, redis_ce,
                                     &key, &add, &z_value) == FAILURE) {
        RETURN_FALSE;
    }

    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

    val = redis_serialize(redis_sock, z_value);
	key = redis_key_prefix(redis_sock, key);
    cmd_len = redis_cmd_format_static(&cmd, keyword, "sfs", key->val, key->len, add, val->val, val->len);
    zend_string_release(val);
    zend_string_release(key);

	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
	IF_ATOMIC() {
	    redis_bulk_double_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
	}
	REDIS_PROCESS_RESPONSE(redis_bulk_double_response);

}

/* {{{ proto double Redis::zIncrBy(string key, double value, mixed member)
 */
PHP_METHOD(Redis, zIncrBy)
{
    generic_incrby_method(INTERNAL_FUNCTION_PARAM_PASSTHRU, "ZINCRBY", sizeof("ZINCRBY")-1);
}
/* }}} */

PHP_REDIS_API void generic_z_command(INTERNAL_FUNCTION_PARAMETERS, char *command, int command_len) {
    zval *object, *z_keys, *z_weights = NULL, *z_data;
    HashTable *ht_keys, *ht_weights = NULL;
    RedisSock *redis_sock;
    smart_string cmd = {0};
    HashPosition ptr;
    zend_string *store_key, *key;
    char *agg_op = NULL;
    int cmd_arg_count = 2, agg_op_len = 0, keys_count;

    /* Grab our parameters */
    if(zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "Osa|a!s",
                                    &object, redis_ce, &store_key,
                                    &z_keys, &z_weights, &agg_op, &agg_op_len) == FAILURE)
    {
        RETURN_FALSE;
    }

    /* We'll need our socket */
    if(redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

    /* Grab our keys argument as an array */
    ht_keys = Z_ARRVAL_P(z_keys);

    /* Nothing to do if there aren't any keys */
    if((keys_count = zend_hash_num_elements(ht_keys)) == 0) {
        RETURN_FALSE;
    } else {
        /* Increment our overall argument count */
        cmd_arg_count += keys_count;
    }

    /* Grab and validate our weights array */
    if(z_weights != NULL) {
        ht_weights = Z_ARRVAL_P(z_weights);

        /* This command is invalid if the weights array isn't the same size */
        /* as our keys array. */
        if(zend_hash_num_elements(ht_weights) != keys_count) {
            RETURN_FALSE;
        }

        /* Increment our overall argument count by the number of keys */
        /* plus one, for the "WEIGHTS" argument itself */
        cmd_arg_count += keys_count + 1;
    }

    /* AGGREGATE option */
    if(agg_op_len != 0) {
        /* Verify our aggregation option */
        if(strncasecmp(agg_op, "SUM", sizeof("SUM")) &&
           strncasecmp(agg_op, "MIN", sizeof("MIN")) &&
           strncasecmp(agg_op, "MAX", sizeof("MAX")))
        {
            RETURN_FALSE;
        }

        /* Two more arguments: "AGGREGATE" and agg_op */
        cmd_arg_count += 2;
    }

    /* Command header */
    redis_cmd_init_sstr(&cmd, cmd_arg_count, command, command_len);

    /* Prefix our key if necessary and add the output key */
    store_key = redis_key_prefix(redis_sock, store_key);
    redis_cmd_append_sstr(&cmd, store_key->val, store_key->len);
    zend_string_release(store_key);

    /* Number of input keys argument */
    redis_cmd_append_sstr_int(&cmd, keys_count);

    /* Process input keys */
    ZEND_HASH_FOREACH_STR_KEY_VAL(ht_keys, key, z_data) {
        /* Apply key prefix if necessary */
        key = redis_key_prefix(redis_sock, key);

        /* Append this input set */
        redis_cmd_append_sstr(&cmd, key->val, key->len);

        /* Free our key if it was prefixed */
        zend_string_release(key);
    } ZEND_HASH_FOREACH_END();

    /* Weights */
    if(ht_weights != NULL) {
        /* Append "WEIGHTS" argument */
        redis_cmd_append_sstr(&cmd, "WEIGHTS", sizeof("WEIGHTS") - 1);

        /* Process weights */
        ZEND_HASH_FOREACH_STR_KEY_VAL(ht_weights, key, z_data) {
            /* Ignore non numeric arguments, unless they're special Redis numbers */
            if (Z_TYPE_P(z_data) != IS_LONG && Z_TYPE_P(z_data) != IS_DOUBLE &&
                 strncasecmp(Z_STRVAL_P(z_data), "inf", sizeof("inf")) != 0 &&
                 strncasecmp(Z_STRVAL_P(z_data), "-inf", sizeof("-inf")) != 0 &&
                 strncasecmp(Z_STRVAL_P(z_data), "+inf", sizeof("+inf")) != 0)
            {
                /* We should abort if we have an invalid weight, rather than pass */
                /* a different number of weights than the user is expecting */
                efree(cmd.c);
                RETURN_FALSE;
            }

            /* Append the weight based on the input type */
            switch(Z_TYPE_P(z_data)) {
                case IS_LONG:
                    redis_cmd_append_sstr_long(&cmd, Z_LVAL_P(z_data));
                    break;
                case IS_DOUBLE:
                    redis_cmd_append_sstr_dbl(&cmd, Z_DVAL_P(z_data));
                    break;
                case IS_STRING:
                    redis_cmd_append_sstr(&cmd, Z_STRVAL_P(z_data), Z_STRLEN_P(z_data));
                    break;
            }
        } ZEND_HASH_FOREACH_END();
    }

    /* Aggregation options, if we have them */
    if(agg_op_len != 0) {
        redis_cmd_append_sstr(&cmd, "AGGREGATE", sizeof("AGGREGATE") - 1);
        redis_cmd_append_sstr(&cmd, agg_op, agg_op_len);
    }

    /* Kick off our request */
    REDIS_PROCESS_REQUEST(redis_sock, cmd.c, cmd.len);
    IF_ATOMIC() {
        redis_long_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
    }
    REDIS_PROCESS_RESPONSE(redis_long_response);
}

/* zInter */
PHP_METHOD(Redis, zInter) {
	generic_z_command(INTERNAL_FUNCTION_PARAM_PASSTHRU, "ZINTERSTORE", 11);
}

/* zUnion */
PHP_METHOD(Redis, zUnion) {
	generic_z_command(INTERNAL_FUNCTION_PARAM_PASSTHRU, "ZUNIONSTORE", 11);
}

/* hashes */

PHP_REDIS_API void
generic_hset(INTERNAL_FUNCTION_PARAMETERS, char *kw, void (*fun)(INTERNAL_FUNCTION_PARAMETERS, RedisSock *, zval *, void *)) {
    zval *object;
    RedisSock *redis_sock;
    zend_string *key, *val, *member;
    char *cmd;
    int cmd_len;
    zval *z_value;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OSSz",
                                     &object, redis_ce,
                                     &key, &member, &z_value) == FAILURE) {
        RETURN_FALSE;
    }

    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

    val = redis_serialize(redis_sock, z_value);
	key = redis_key_prefix(redis_sock, key);
    cmd_len = redis_cmd_format_static(&cmd, kw, "sss", key->val, key->len,
            member->val, member->len, val->val, val->len);
    zend_string_release(val);
    zend_string_release(key);

	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
	IF_ATOMIC() {
	  fun(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
	}
	REDIS_PROCESS_RESPONSE(fun);
}
/* hSet */
PHP_METHOD(Redis, hSet)
{
	generic_hset(INTERNAL_FUNCTION_PARAM_PASSTHRU, "HSET", redis_long_response);
}
/* }}} */
/* hSetNx */
PHP_METHOD(Redis, hSetNx)
{
	generic_hset(INTERNAL_FUNCTION_PARAM_PASSTHRU, "HSETNX", redis_1_response);
}
/* }}} */


/* hGet */
PHP_METHOD(Redis, hGet)
{
    zval *object;
    RedisSock *redis_sock;
    zend_string *key, *member;
    char *cmd;
    int cmd_len;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OSS",
                                     &object, redis_ce,
                                     &key, &member) == FAILURE) {
        RETURN_FALSE;
    }

    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }
	key = redis_key_prefix(redis_sock, key);
    cmd_len = redis_cmd_format_static(&cmd, "HGET", "ss", key->val, key->len,
            member->val, member->len);
	zend_string_release(key);

	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
	IF_ATOMIC() {
		redis_string_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
	}
	REDIS_PROCESS_RESPONSE(redis_string_response);

}
/* }}} */

/* hLen */
PHP_METHOD(Redis, hLen)
{
    zval *object;
    RedisSock *redis_sock;
    zend_string *key;
    char *cmd;
    int cmd_len;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OS",
                                     &object, redis_ce,
                                     &key) == FAILURE) {
        RETURN_FALSE;
    }

    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

	key = redis_key_prefix(redis_sock, key);
    cmd_len = redis_cmd_format_static(&cmd, "HLEN", "s", key->val, key->len);
	zend_string_release(key);

	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
	IF_ATOMIC() {
	  redis_long_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
	}
	REDIS_PROCESS_RESPONSE(redis_long_response);

}
/* }}} */

PHP_REDIS_API RedisSock*
generic_hash_command_2(INTERNAL_FUNCTION_PARAMETERS, char *keyword, int keyword_len, char **out_cmd, int *out_len) {

    zval *object;
    RedisSock *redis_sock;
    zend_string *key, *member;
    char *cmd;
    int cmd_len;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OSS",
                                     &object, redis_ce,
                                     &key, &member) == FAILURE) {
            ZVAL_BOOL(return_value, 0);
            return NULL;
    }

    if (redis_sock_get(object, &redis_sock, 0) < 0) {
            ZVAL_BOOL(return_value, 0);
            return NULL;
    }
	key = redis_key_prefix(redis_sock, key);
    cmd_len = redis_cmd_format_static(&cmd, keyword, "ss", key->val, key->len, member->val, member->len);
	zend_string_release(key);

    *out_cmd = cmd;
    *out_len = cmd_len;
    return redis_sock;
}

/* hDel */
PHP_METHOD(Redis, hDel)
{
    RedisSock *redis_sock;

    if(FAILURE == generic_multiple_args_cmd(INTERNAL_FUNCTION_PARAM_PASSTHRU,
                    "HDEL", sizeof("HDEL") - 1,
					2, &redis_sock, 0, 0, 0))
		return;

	IF_ATOMIC() {
	  redis_long_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
	}
	REDIS_PROCESS_RESPONSE(redis_long_response);
}

/* hExists */
PHP_METHOD(Redis, hExists)
{
    char *cmd;
    int cmd_len;
    RedisSock *redis_sock = generic_hash_command_2(INTERNAL_FUNCTION_PARAM_PASSTHRU, "HEXISTS", 7, &cmd, &cmd_len);
	if(!redis_sock)
		RETURN_FALSE;

	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
	IF_ATOMIC() {
	  redis_1_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
	}
	REDIS_PROCESS_RESPONSE(redis_1_response);

}

PHP_REDIS_API RedisSock*
generic_hash_command_1(INTERNAL_FUNCTION_PARAMETERS, char *keyword, int keyword_len) {

    zval *object;
    RedisSock *redis_sock;
    zend_string *key;
    char *cmd;
    int cmd_len;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OS",
                                     &object, redis_ce,
                                     &key) == FAILURE) {
            ZVAL_BOOL(return_value, 0);
            return NULL;
    }

    if (redis_sock_get(object, &redis_sock, 0) < 0) {
            ZVAL_BOOL(return_value, 0);
            return NULL;
    }
	key = redis_key_prefix(redis_sock, key);
    cmd_len = redis_cmd_format_static(&cmd, keyword, "s", key->val, key->len);
	zend_string_release(key);

	/* call REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len) without breaking the return value */
	IF_MULTI_OR_ATOMIC() {
		if(redis_sock_write(redis_sock, cmd, cmd_len) < 0) {
			efree(cmd);
			return NULL;
		}
		efree(cmd);
	}
	IF_PIPELINE() {
		PIPELINE_ENQUEUE_COMMAND(cmd, cmd_len);
		efree(cmd);
	}
    return redis_sock;
}

/* hKeys */
PHP_METHOD(Redis, hKeys)
{
    RedisSock *redis_sock = generic_hash_command_1(INTERNAL_FUNCTION_PARAM_PASSTHRU, "HKEYS", sizeof("HKEYS")-1);
	if(!redis_sock)
		RETURN_FALSE;

	IF_ATOMIC() {
	    if (redis_mbulk_reply_raw(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL) < 0) {
    	    RETURN_FALSE;
	    }
	}
	REDIS_PROCESS_RESPONSE(redis_mbulk_reply_raw);


}
/* hVals */
PHP_METHOD(Redis, hVals)
{
    RedisSock *redis_sock = generic_hash_command_1(INTERNAL_FUNCTION_PARAM_PASSTHRU, "HVALS", sizeof("HVALS")-1);
	if(!redis_sock)
		RETURN_FALSE;

	IF_ATOMIC() {
	    if (redis_sock_read_multibulk_reply(INTERNAL_FUNCTION_PARAM_PASSTHRU,
    	                                    redis_sock, NULL, NULL) < 0) {
        	RETURN_FALSE;
	    }
	}
	REDIS_PROCESS_RESPONSE(redis_sock_read_multibulk_reply);

}


PHP_METHOD(Redis, hGetAll) {

    RedisSock *redis_sock = generic_hash_command_1(INTERNAL_FUNCTION_PARAM_PASSTHRU, "HGETALL", sizeof("HGETALL")-1);
	if(!redis_sock)
		RETURN_FALSE;

	IF_ATOMIC() {
	    if (redis_mbulk_reply_zipped_vals(INTERNAL_FUNCTION_PARAM_PASSTHRU,
    	                                    redis_sock, NULL, NULL) < 0) {
        	RETURN_FALSE;
	    }
	}
	REDIS_PROCESS_RESPONSE(redis_mbulk_reply_zipped_vals);
}

PHP_METHOD(Redis, hIncrByFloat)
{
	zval *object;
	RedisSock *redis_sock;
	zend_string *key, *member;
    char *cmd;
	int cmd_len;
	double val;

	/* Validate we have the right number of arguments */
	if(zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OSSd",
									&object, redis_ce,
									&key, &member, &val) == FAILURE) {
		RETURN_FALSE;
	}

	/* Grab our socket */
	if(redis_sock_get(object, &redis_sock, 0) < 0) {
		RETURN_FALSE;
	}

	key = redis_key_prefix(redis_sock, key);
	cmd_len = redis_cmd_format_static(&cmd, "HINCRBYFLOAT", "ssf", key->val, key->len, member->val, member->len, val);
	zend_string_release(key);

	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
	IF_ATOMIC() {
	    redis_bulk_double_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
	}
	REDIS_PROCESS_RESPONSE(redis_bulk_double_response);
}

PHP_METHOD(Redis, hIncrBy)
{
    zval *object;
    RedisSock *redis_sock;
    zend_string *key, *member, *val;
    char *cmd;
    int cmd_len;
	int i;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OSSS",
                                     &object, redis_ce,
                                     &key, &member, &val) == FAILURE) {
        RETURN_FALSE;
    }

    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

	/* check for validity of numeric string */
	i = 0;
	if(val->len && val->val[0] == '-') { /* negative case */
		i++;
	}
	for(; i < val->len; ++i) {
		if(val->val[i] < '0' || val->val[i] > '9') {
			RETURN_FALSE;
		}
	}

    /* HINCRBY key member amount */
	key = redis_key_prefix(redis_sock, key);
    cmd_len = redis_cmd_format_static(&cmd, "HINCRBY", "sss", key->val, key->len, member->val, member->len, val->val, val->len);
	zend_string_release(key);

	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
	IF_ATOMIC() {
	  redis_long_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
	}
	REDIS_PROCESS_RESPONSE(redis_long_response);

}

/* {{{ array Redis::hMget(string hash, array keys) */
PHP_METHOD(Redis, hMget) {
    zval *object;
    RedisSock *redis_sock;
    zend_string *key;
    zval *z_array, *z_keys, *data;
    int field_count, i, valid = 0;
    HashTable *ht_array;
    HashPosition ptr;
    smart_string cmd = {0};

    /* Make sure we can grab our arguments properly */
    if(zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OSa",
                                    &object, redis_ce, &key, &z_array)
                                    == FAILURE)
    {
        RETURN_FALSE;
    }

    /* We'll need our socket */
    if(redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

    /* Grab member count and abort if we don't have any */
    if((field_count = zend_hash_num_elements(Z_ARRVAL_P(z_array))) == 0) {
        RETURN_FALSE;
    }

    /* Prefix our key if we need to */
    key = redis_key_prefix(redis_sock, key);

    /* Allocate enough memory for the number of keys being requested */
    z_keys = ecalloc(field_count, sizeof(zval));

    /* Grab our HashTable */
    ht_array = Z_ARRVAL_P(z_array);

    /* Iterate through our keys, grabbing members that are valid */
    ZEND_HASH_FOREACH_VAL(ht_array, data) {
        /* Make sure the data is a long or string, and if it's a string that */
        /* it isn't empty.  There is no reason to send empty length members. */
        if((Z_TYPE_P(data) == IS_STRING && Z_STRLEN_P(data)>0) ||
            Z_TYPE_P(data) == IS_LONG)
        {
            /* This is a key we can ask for, copy it and set it in our array */
			ZVAL_COPY_VALUE(&z_keys[valid], data);

            /* Increment the number of valid keys we've encountered */
            valid++;
        }
    } ZEND_HASH_FOREACH_END();

    /* If we don't have any valid keys, we can abort here */
    if(valid == 0) {
        zend_string_release(key);
        efree(z_keys);
        RETURN_FALSE;
    }

    /* Build command header.  One extra argument for the hash key itself */
    redis_cmd_init_sstr(&cmd, valid+1, "HMGET", sizeof("HMGET")-1);

    /* Add the hash key */
    redis_cmd_append_sstr(&cmd, key->val, key->len);

    /* Free key memory if it was prefixed */
    zend_string_release(key);

    /* Iterate our keys, appending them as arguments */
    for(i=0;i<valid;i++) {
        redis_cmd_append_sstr(&cmd, Z_STRVAL(z_keys[i]), Z_STRLEN(z_keys[i]));
    }

    /* Kick off our request */
    REDIS_PROCESS_REQUEST(redis_sock, cmd.c, cmd.len);
    IF_ATOMIC() {
        redis_mbulk_reply_assoc(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, z_keys);
    }
    REDIS_PROCESS_RESPONSE_CLOSURE(redis_mbulk_reply_assoc, z_keys);
}

PHP_METHOD(Redis, hMset)
{
    zval *object;
    RedisSock *redis_sock;
    zend_string *key;
    char *cmd, *old_cmd = NULL;
    int cmd_len, i = 0, element_count = 2;
    zval *z_hash, *z_value_p;
    HashTable *ht_hash;
    smart_string set_cmds = {0};

    if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OSa",
                                     &object, redis_ce,
                                     &key, &z_hash) == FAILURE) {
        RETURN_FALSE;
    }

    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

    ht_hash = Z_ARRVAL_P(z_hash);

    if (zend_hash_num_elements(ht_hash) == 0) {
        RETURN_FALSE;
    }

    key = redis_key_prefix(redis_sock, key);
    cmd_len = redis_cmd_format(&cmd,
                    "$5" _NL "HMSET" _NL
                    "$%d" _NL "%s" _NL
                    , key->val, key->len);
    zend_string_release(key);

    /* looping on each item of the array */
    ZEND_HASH_FOREACH_STR_KEY_VAL(ht_hash, key, z_value_p) {
        zend_string *hval;

        element_count += 2;

        /* key is set. */
        hval = redis_serialize(redis_sock, z_value_p);

        /* Append our member and value in place */
        redis_cmd_append_sstr(&set_cmds, key->val, key->len);
        redis_cmd_append_sstr(&set_cmds, hval->val, hval->len);

        zend_string_release(hval);
    } ZEND_HASH_FOREACH_END();

    /* Now construct the entire command */
    old_cmd = cmd;
    cmd_len = redis_cmd_format(&cmd, "*%d" _NL "%s%s", element_count, cmd, cmd_len, set_cmds.c, set_cmds.len);
    efree(old_cmd);

    /* Free the HMSET bits */
    efree(set_cmds.c);

    REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
    IF_ATOMIC() {
        redis_boolean_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
    }
    REDIS_PROCESS_RESPONSE(redis_boolean_response);
}


PHP_REDIS_API int redis_response_enqueued(RedisSock *redis_sock) {

	char *response;
	int response_len, ret = 0;

	if ((response = redis_sock_read(redis_sock, &response_len)) == NULL) {
		return 0;
    }

    if(strncmp(response, "+QUEUED", 7) == 0) {
        ret = 1;
    }
    efree(response);
    return ret;
}

/* flag : get, set {ATOMIC, MULTI, PIPELINE} */

PHP_METHOD(Redis, multi)
{

    RedisSock *redis_sock;
    char *cmd;
	int response_len, cmd_len;
	char * response;
	zval *object;
	long multi_value = MULTI;

	if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "O|l",
                                     &object, redis_ce, &multi_value) == FAILURE) {
        RETURN_FALSE;
    }

    /* if the flag is activated, send the command, the reply will be "QUEUED" or -ERR */

    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

	if(multi_value == MULTI || multi_value == PIPELINE) {
		redis_sock->mode = multi_value;
	} else {
        RETURN_FALSE;
	}

    redis_sock->current = NULL;

	IF_MULTI() {
        cmd_len = redis_cmd_format_static(&cmd, "MULTI", "");

		if (redis_sock_write(redis_sock, cmd, cmd_len) < 0) {
        	efree(cmd);
	        RETURN_FALSE;
    	}
	    efree(cmd);

    	if ((response = redis_sock_read(redis_sock, &response_len)) == NULL) {
        	RETURN_FALSE;
    	}

        if(strncmp(response, "+OK", 3) == 0) {
            efree(response);
			RETURN_ZVAL(getThis(), 1, 0);
		}
        efree(response);
		RETURN_FALSE;
	}
	IF_PIPELINE() {
        free_reply_callbacks(getThis(), redis_sock);
		RETURN_ZVAL(getThis(), 1, 0);
	}
}

/* discard */
PHP_METHOD(Redis, discard)
{
    RedisSock *redis_sock;
	zval *object;

	if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "O",
                                     &object, redis_ce) == FAILURE) {
        RETURN_FALSE;
    }

    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

	redis_sock->mode = ATOMIC;
	redis_send_discard(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock);
}

PHP_REDIS_API int redis_sock_read_multibulk_pipeline_reply(INTERNAL_FUNCTION_PARAMETERS, RedisSock *redis_sock)
{
    zval z_tab;
    array_init(&z_tab);

    redis_sock_read_multibulk_multi_reply_loop(INTERNAL_FUNCTION_PARAM_PASSTHRU,
                    redis_sock, &z_tab, 0);

    zval_dtor(&z_tab);

    /* free allocated function/request memory */
    free_reply_callbacks(getThis(), redis_sock);

    return 0;

}
/* redis_sock_read_multibulk_multi_reply */
PHP_REDIS_API int redis_sock_read_multibulk_multi_reply(INTERNAL_FUNCTION_PARAMETERS,
                                      RedisSock *redis_sock)
{

    char inbuf[1024];
	int numElems;
	zval z_tab;

    redis_check_eof(redis_sock);

    php_stream_gets(redis_sock->stream, inbuf, 1024);
    if(inbuf[0] != '*') {
        return -1;
    }

	/* number of responses */
    numElems = atoi(inbuf+1);

    if(numElems < 0) {
        return -1;
    }

    zval_dtor(return_value);

    array_init(&z_tab);

    redis_sock_read_multibulk_multi_reply_loop(INTERNAL_FUNCTION_PARAM_PASSTHRU,
                    redis_sock, &z_tab, numElems);

	zval_dtor(&z_tab);

    return 0;
}

void
free_reply_callbacks(zval *z_this, RedisSock *redis_sock) {

	fold_item *fi;
    fold_item *head = redis_sock->head;
	request_item *ri;

	for(fi = head; fi; ) {
        fold_item *fi_next = fi->next;
        free(fi);
        fi = fi_next;
    }
    redis_sock->head = NULL;
    redis_sock->current = NULL;

    for(ri = redis_sock->pipeline_head; ri; ) {
        struct request_item *ri_next = ri->next;
        free(ri->request_str);
        free(ri);
        ri = ri_next;
    }
    redis_sock->pipeline_head = NULL;
    redis_sock->pipeline_current = NULL;
}

/* exec */
PHP_METHOD(Redis, exec)
{

    RedisSock *redis_sock;
    char *cmd;
	int cmd_len;
	zval *object;
    struct request_item *ri;

	if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "O",
                                     &object, redis_ce) == FAILURE) {
        RETURN_FALSE;
    }
   	if (redis_sock_get(object, &redis_sock, 0) < 0) {
       	RETURN_FALSE;
    }

	IF_MULTI() {

        cmd_len = redis_cmd_format_static(&cmd, "EXEC", "");

		if (redis_sock_write(redis_sock, cmd, cmd_len) < 0) {
			efree(cmd);
			RETURN_FALSE;
		}
		efree(cmd);

	    if (redis_sock_read_multibulk_multi_reply(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock) < 0) {
            zval_dtor(return_value);
            free_reply_callbacks(object, redis_sock);
            redis_sock->mode = ATOMIC;
            redis_sock->watching = 0;
			RETURN_FALSE;
	    }
        free_reply_callbacks(object, redis_sock);
		redis_sock->mode = ATOMIC;
        redis_sock->watching = 0;
	}

	IF_PIPELINE() {

		char *request = NULL;
		int total = 0;
		int offset = 0;

        /* compute the total request size */
		for(ri = redis_sock->pipeline_head; ri; ri = ri->next) {
            total += ri->request_size;
		}
        if(total) {
		    request = malloc(total);
        }

        /* concatenate individual elements one by one in the target buffer */
		for(ri = redis_sock->pipeline_head; ri; ri = ri->next) {
			memcpy(request + offset, ri->request_str, ri->request_size);
			offset += ri->request_size;
		}

		if(request != NULL) {
		    if (redis_sock_write(redis_sock, request, total) < 0) {
    		    free(request);
                free_reply_callbacks(object, redis_sock);
                redis_sock->mode = ATOMIC;
        		RETURN_FALSE;
		    }
		   	free(request);
		} else {
                redis_sock->mode = ATOMIC;
                free_reply_callbacks(object, redis_sock);
                array_init(return_value); /* empty array when no command was run. */
                return;
        }

	    if (redis_sock_read_multibulk_pipeline_reply(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock) < 0) {
			redis_sock->mode = ATOMIC;
            free_reply_callbacks(object, redis_sock);
			RETURN_FALSE;
	    }
		redis_sock->mode = ATOMIC;
        free_reply_callbacks(object, redis_sock);
	}
}

PHP_REDIS_API void fold_this_item(INTERNAL_FUNCTION_PARAMETERS, fold_item *item, RedisSock *redis_sock, zval *z_tab) {
	item->fun(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, z_tab, item->ctx);
}

PHP_REDIS_API int redis_sock_read_multibulk_multi_reply_loop(INTERNAL_FUNCTION_PARAMETERS,
							RedisSock *redis_sock, zval *z_tab, int numElems)
{

    fold_item *head = redis_sock->head;
    fold_item *current = redis_sock->current;
    for(current = head; current; current = current->next) {
		fold_this_item(INTERNAL_FUNCTION_PARAM_PASSTHRU, current, redis_sock, z_tab);
    }
    redis_sock->current = current;
    return 0;
}

PHP_METHOD(Redis, pipeline)
{
    RedisSock *redis_sock;
	zval *object;

	if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "O",
                                     &object, redis_ce) == FAILURE) {
        RETURN_FALSE;
    }

    /* if the flag is activated, send the command, the reply will be "QUEUED" or -ERR */
    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }
	redis_sock->mode = PIPELINE;

	/*
		NB : we keep the function fold, to detect the last function.
		We need the response format of the n - 1 command. So, we can delete when n > 2, the { 1 .. n - 2} commands
	*/

    free_reply_callbacks(getThis(), redis_sock);

	RETURN_ZVAL(getThis(), 1, 0);
}

/*
	publish channel message
	@return the number of subscribers
*/
PHP_METHOD(Redis, publish)
{
    zval *object;
    RedisSock *redis_sock;
    zend_string *key, *val;
    char *cmd;
    int cmd_len;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OSS",
                                     &object, redis_ce,
                                     &key, &val) == FAILURE) {
        RETURN_NULL();
    }

    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

	key = redis_key_prefix(redis_sock, key);
    cmd_len = redis_cmd_format_static(&cmd, "PUBLISH", "ss", key->val, key->len, val->val, val->len);
	zend_string_release(key);

	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
    IF_ATOMIC() {
		redis_long_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
    }
    REDIS_PROCESS_RESPONSE(redis_long_response);
}

PHP_REDIS_API void generic_subscribe_cmd(INTERNAL_FUNCTION_PARAMETERS, char *sub_cmd)
{
	zval *object, *array, *data;
    HashTable *arr_hash;
    HashPosition pointer;
    RedisSock *redis_sock;
    zend_string *key;
    char *cmd = "", *old_cmd = NULL;
    int cmd_len, array_count;
	zval z_tab, *tmp;
	char *type_response;

	/* Function call information */
	zend_fcall_info z_callback;
	zend_fcall_info_cache z_callback_cache;
	zval z_ret, *z_args[4];

	if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "Oaf",
									 &object, redis_ce, &array, &z_callback, &z_callback_cache) == FAILURE) {
		RETURN_FALSE;
	}

    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

    arr_hash    = Z_ARRVAL_P(array);
    array_count = zend_hash_num_elements(arr_hash);

    if (array_count == 0) {
        RETURN_FALSE;
    }

    ZEND_HASH_FOREACH_STR_KEY_VAL(arr_hash, key, data) {
        if (Z_TYPE_PP(data) == IS_STRING) {
            char *old_cmd = NULL;
            if(*cmd) {
                old_cmd = cmd;
            }

            /* Prefix our key if neccisary */
            key = redis_key_prefix(redis_sock, key);

            cmd_len = spprintf(&cmd, 0, "%s %s", cmd, key->val);

            if(old_cmd) {
                efree(old_cmd);
            }

            /* Free our key if it was prefixed */
            zend_string_release(key);
        }
    } ZEND_HASH_FOREACH_END();

    old_cmd = cmd;
    cmd_len = spprintf(&cmd, 0, "%s %s\r\n", sub_cmd, cmd);
    efree(old_cmd);
    if (redis_sock_write(redis_sock, cmd, cmd_len) < 0) {
        efree(cmd);
        RETURN_FALSE;
    }
    efree(cmd);

	/* read the status of the execution of the command `subscribe` */

	if(redis_sock_read_multibulk_reply_zval(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, &z_tab) < 0) {
		RETURN_FALSE;
	}

	if (tmp = zend_hash_index_find(Z_ARRVAL(z_tab), 0)) {
		type_response = Z_STRVAL_P(tmp);
		if(strcmp(type_response, sub_cmd) != 0) {
			zval_dtor(&z_tab);
			RETURN_FALSE;
		}
	} else {
		zval_dtor(&z_tab);
		RETURN_FALSE;
	}

	/* Set a pointer to our return value and to our arguments. */
	z_callback.retval = &z_ret;
	z_callback.params = *z_args;
	z_callback.no_separation = 0;

	/* Multibulk Response, format : {message type, originating channel, message payload} */
	while(1) {
		/* call the callback with this z_tab in argument */
	    int is_pmsg, tab_idx = 1;
		zval *type, *channel, *pattern, *data;

        /* clean up if previously used*/
        zval_dtor(&z_tab);

		if(redis_sock_read_multibulk_reply_zval(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, &z_tab) < 0 ||
                Z_TYPE(z_tab) != IS_ARRAY ||
                !(type = zend_hash_index_find(Z_ARRVAL(z_tab), 0)) || Z_TYPE_P(type) != IS_STRING)
        {
			break;
		}

		/* Make sure we have a message or pmessage */
		if(!strncmp(Z_STRVAL_P(type), "message", 7) || !strncmp(Z_STRVAL_P(type), "pmessage", 8)) {
			/* Is this a pmessage */
			is_pmsg = *Z_STRVAL_P(type) == 'p';
		} else {
			continue;  /* It's not a message or pmessage */
		}

		/* If this is a pmessage, we'll want to extract the pattern first */
		if(is_pmsg) {
			/* Extract pattern */
			if(!(pattern = zend_hash_index_find(Z_ARRVAL(z_tab), tab_idx++))) {
				break;
			}
		}

		/* Extract channel and data */
		if (!(channel = zend_hash_index_find(Z_ARRVAL(z_tab), tab_idx++))) {
			break;
		}
		if (!(data = zend_hash_index_find(Z_ARRVAL(z_tab), tab_idx++))) {
			break;
		}

		/* Always pass the Redis object through */
		z_args[0] = getThis();

		/* Set up our callback args depending on the message type */
		if(is_pmsg) {
			z_args[1] = pattern;
			z_args[2] = channel;
			z_args[3] = data;
		} else {
			z_args[1] = channel;
			z_args[2] = data;
		}

		/* Set our argument information */
		z_callback.param_count = tab_idx;

		/* Break if we can't call the function */
		if(zend_call_function(&z_callback, &z_callback_cache) != SUCCESS) {
			break;
		}

        /* Free reply from Redis */
        zval_dtor(&z_tab);

        /* Check for a non-null return value.  If we have one, return it from
         * the subscribe function itself.  Otherwise continue our loop. */
        if (Z_TYPE(z_ret) != IS_NULL) {
			zval *tmp;
			ZVAL_COPY_VALUE(tmp, &z_ret);
            RETVAL_ZVAL(tmp, 0, 1);
            break;
        }
        zval_dtor(&z_ret);
	}

    zval_dtor(&z_tab);
}

/* {{{ proto void Redis::psubscribe(Array(pattern1, pattern2, ... patternN))
 */
PHP_METHOD(Redis, psubscribe)
{
	generic_subscribe_cmd(INTERNAL_FUNCTION_PARAM_PASSTHRU, "psubscribe");
}

/* {{{ proto void Redis::subscribe(Array(channel1, channel2, ... channelN))
 */
PHP_METHOD(Redis, subscribe) {
	generic_subscribe_cmd(INTERNAL_FUNCTION_PARAM_PASSTHRU, "subscribe");
}

/**
 *	[p]unsubscribe channel_0 channel_1 ... channel_n
 *  [p]unsubscribe(array(channel_0, channel_1, ..., channel_n))
 * response format :
 * array(
 * 	channel_0 => TRUE|FALSE,
 *	channel_1 => TRUE|FALSE,
 *	...
 *	channel_n => TRUE|FALSE
 * );
 **/

PHP_REDIS_API void generic_unsubscribe_cmd(INTERNAL_FUNCTION_PARAMETERS, char *unsub_cmd)
{
    zval *object, *array, *data;
    HashTable *arr_hash;
    HashPosition pointer;
    RedisSock *redis_sock;
    char *cmd = "", *old_cmd = NULL;
    int cmd_len, array_count;

	int i;
	zval z_tab, *z_channel;

	if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "Oa",
									 &object, redis_ce, &array) == FAILURE) {
		RETURN_FALSE;
	}
    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

    arr_hash    = Z_ARRVAL_P(array);
    array_count = zend_hash_num_elements(arr_hash);

    if (array_count == 0) {
        RETURN_FALSE;
    }

    ZEND_HASH_FOREACH_VAL(arr_hash, data) {
        if (Z_TYPE_P(data) == IS_STRING) {
            char *old_cmd = NULL;
            if(*cmd) {
                old_cmd = cmd;
            }
            cmd_len = spprintf(&cmd, 0, "%s %s", cmd, Z_STRVAL_PP(data));
            if(old_cmd) {
                efree(old_cmd);
            }
        }
    } ZEND_HASH_FOREACH_END();

    old_cmd = cmd;
    cmd_len = spprintf(&cmd, 0, "%s %s\r\n", unsub_cmd, cmd);
    efree(old_cmd);

    if (redis_sock_write(redis_sock, cmd, cmd_len) < 0) {
        efree(cmd);
        RETURN_FALSE;
    }
    efree(cmd);

	i = 1;
	array_init(return_value);

	while( i <= array_count) {
	    if (redis_sock_read_multibulk_reply_zval(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, &z_tab) < 0){
            RETURN_FALSE;
        };

		if(Z_TYPE(z_tab) == IS_ARRAY) {
			if (!(z_channel = zend_hash_index_find(Z_ARRVAL(z_tab), 1))) {
                zval_dtor(&z_tab);
				RETURN_FALSE;
			}
			add_assoc_bool(return_value, Z_STRVAL_P(z_channel), 1);
		} else {
			/*error */
            zval_dtor(&z_tab);
			RETURN_FALSE;
		}
        zval_dtor(&z_tab);
		i ++;
	}
}

PHP_METHOD(Redis, unsubscribe)
{
	generic_unsubscribe_cmd(INTERNAL_FUNCTION_PARAM_PASSTHRU, "UNSUBSCRIBE");
}

PHP_METHOD(Redis, punsubscribe)
{
	generic_unsubscribe_cmd(INTERNAL_FUNCTION_PARAM_PASSTHRU, "PUNSUBSCRIBE");
}

/* {{{ proto string Redis::bgrewriteaof()
 */
PHP_METHOD(Redis, bgrewriteaof)
{
    char *cmd;
    int cmd_len = redis_cmd_format_static(&cmd, "BGREWRITEAOF", "");
    generic_empty_cmd(INTERNAL_FUNCTION_PARAM_PASSTHRU, cmd, cmd_len);

}
/* }}} */

/* {{{ proto string Redis::slaveof([host, port])
 */
PHP_METHOD(Redis, slaveof)
{
    zval *object;
    RedisSock *redis_sock;
    char *cmd = "", *host = NULL;
    int cmd_len, host_len;
    long port = 6379;

	if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "O|sl",
									 &object, redis_ce, &host, &host_len, &port) == FAILURE) {
		RETURN_FALSE;
	}
    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

    if(host && host_len) {
        cmd_len = redis_cmd_format_static(&cmd, "SLAVEOF", "sd", host, host_len, (int)port);
    } else {
        cmd_len = redis_cmd_format_static(&cmd, "SLAVEOF", "ss", "NO", 2, "ONE", 3);
    }

	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
    IF_ATOMIC() {
	  redis_boolean_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
    }
    REDIS_PROCESS_RESPONSE(redis_boolean_response);
}
/* }}} */

/* {{{ proto string Redis::object(key)
 */
PHP_METHOD(Redis, object)
{
    zval *object;
    RedisSock *redis_sock;
    zend_string *key;
    char *cmd = "", *info = NULL;
    int cmd_len, info_len;

	if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OsS",
									 &object, redis_ce, &info, &info_len, &key) == FAILURE) {
		RETURN_FALSE;
	}
    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

    cmd_len = redis_cmd_format_static(&cmd, "OBJECT", "ss", info, info_len, key->val, key->len);
	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);

	if(info_len == 8 && (strncasecmp(info, "refcount", 8) == 0 || strncasecmp(info, "idletime", 8) == 0)) {
		IF_ATOMIC() {
		  redis_long_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
		}
		REDIS_PROCESS_RESPONSE(redis_long_response);
	} else if(info_len == 8 && strncasecmp(info, "encoding", 8) == 0) {
		IF_ATOMIC() {
		  redis_string_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
		}
		REDIS_PROCESS_RESPONSE(redis_string_response);
	} else { /* fail */
		IF_ATOMIC() {
		  redis_boolean_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
		}
		REDIS_PROCESS_RESPONSE(redis_boolean_response);
	}
}
/* }}} */

/* {{{ proto string Redis::getOption($option)
 */
PHP_METHOD(Redis, getOption)  {
    RedisSock *redis_sock;
    zval *object;
    long option;

	if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "Ol",
									 &object, redis_ce, &option) == FAILURE) {
		RETURN_FALSE;
	}

    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

    switch(option) {
        case REDIS_OPT_SERIALIZER:
            RETURN_LONG(redis_sock->serializer);
        case REDIS_OPT_PREFIX:
            if(redis_sock->prefix) {
                RETURN_STRINGL(redis_sock->prefix, redis_sock->prefix_len);
            }
            RETURN_NULL();
        case REDIS_OPT_READ_TIMEOUT:
            RETURN_DOUBLE(redis_sock->read_timeout);
        case REDIS_OPT_SCAN:
            RETURN_LONG(redis_sock->scan);
        default:
            RETURN_FALSE;
    }
}
/* }}} */

/* {{{ proto string Redis::setOption(string $option, mixed $value)
 */
PHP_METHOD(Redis, setOption) {
    RedisSock *redis_sock;
    zval *object;
    long option, val_long;
	char *val_str;
	int val_len;
    struct timeval read_tv;

	if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "Ols",
									 &object, redis_ce, &option, &val_str, &val_len) == FAILURE) {
		RETURN_FALSE;
	}

    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

    switch(option) {
        case REDIS_OPT_SERIALIZER:
            val_long = atol(val_str);
            if(val_long == REDIS_SERIALIZER_NONE
#ifdef HAVE_REDIS_IGBINARY
                    || val_long == REDIS_SERIALIZER_IGBINARY
#endif
                    || val_long == REDIS_SERIALIZER_PHP) {
                        redis_sock->serializer = val_long;
                        RETURN_TRUE;
                    } else {
                        RETURN_FALSE;
                    }
                    break;
			case REDIS_OPT_PREFIX:
			    if(redis_sock->prefix) {
			        efree(redis_sock->prefix);
			    }
			    if(val_len == 0) {
			        redis_sock->prefix = NULL;
			        redis_sock->prefix_len = 0;
			    } else {
			        redis_sock->prefix_len = val_len;
			        redis_sock->prefix = ecalloc(1+val_len, 1);
			        memcpy(redis_sock->prefix, val_str, val_len);
			    }
			    RETURN_TRUE;
            case REDIS_OPT_READ_TIMEOUT:
                redis_sock->read_timeout = atof(val_str);
                if(redis_sock->stream) {
                    read_tv.tv_sec  = (time_t)redis_sock->read_timeout;
                    read_tv.tv_usec = (int)((redis_sock->read_timeout - read_tv.tv_sec) * 1000000);
                    php_stream_set_option(redis_sock->stream, PHP_STREAM_OPTION_READ_TIMEOUT,0, &read_tv);
                }
                RETURN_TRUE;
            case REDIS_OPT_SCAN:
                val_long = atol(val_str);
                if(val_long == REDIS_SCAN_NORETRY || val_long == REDIS_SCAN_RETRY) {
                    redis_sock->scan = val_long;
                    RETURN_TRUE;
                }
                RETURN_FALSE;
                break;
            default:
                RETURN_FALSE;
    }
}
/* }}} */

/* {{{ proto boolean Redis::config(string op, string key [, mixed value])
 */
PHP_METHOD(Redis, config)
{
    zval *object;
    RedisSock *redis_sock;
    zend_string *key, *val;
    char *cmd, *op = NULL;
    int cmd_len, op_len;
	enum {CFG_GET, CFG_SET} mode;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OSs|s",
                                     &object, redis_ce, &op, &op_len, &key,
                                     &val) == FAILURE) {
        RETURN_FALSE;
    }

	/* op must be GET or SET */
	if(strncasecmp(op, "GET", 3) == 0) {
		mode = CFG_GET;
	} else if(strncasecmp(op, "SET", 3) == 0) {
		mode = CFG_SET;
	} else {
		RETURN_FALSE;
	}

    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

    if (mode == CFG_GET && val == NULL) {
        cmd_len = redis_cmd_format_static(&cmd, "CONFIG", "ss", op, op_len, key->val, key->len);

		REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len)
		IF_ATOMIC() {
			redis_mbulk_reply_zipped_raw(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
		}
		REDIS_PROCESS_RESPONSE(redis_mbulk_reply_zipped_raw);

    } else if(mode == CFG_SET && val != NULL) {
        cmd_len = redis_cmd_format_static(&cmd, "CONFIG", "sss", op, op_len, key->val, key->len, val->val, val->len);

		REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len)
		IF_ATOMIC() {
			redis_boolean_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
		}
		REDIS_PROCESS_RESPONSE(redis_boolean_response);
    } else {
		RETURN_FALSE;
	}
}
/* }}} */


/* {{{ proto boolean Redis::slowlog(string arg, [int option])
 */
PHP_METHOD(Redis, slowlog) {
    zval *object;
    RedisSock *redis_sock;
    char *arg, *cmd;
    int arg_len, cmd_len;
    long option;
    enum {SLOWLOG_GET, SLOWLOG_LEN, SLOWLOG_RESET} mode;

    /* Make sure we can get parameters */
    if(zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "Os|l",
                                    &object, redis_ce, &arg, &arg_len, &option) == FAILURE)
    {
        RETURN_FALSE;
    }

    /* Figure out what kind of slowlog command we're executing */
    if(!strncasecmp(arg, "GET", 3)) {
        mode = SLOWLOG_GET;
    } else if(!strncasecmp(arg, "LEN", 3)) {
        mode = SLOWLOG_LEN;
    } else if(!strncasecmp(arg, "RESET", 5)) {
        mode = SLOWLOG_RESET;
    } else {
        /* This command is not valid */
        RETURN_FALSE;
    }

    /* Make sure we can grab our redis socket */
    if(redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

    /* Create our command.  For everything except SLOWLOG GET (with an arg) it's just two parts */
    if(mode == SLOWLOG_GET && ZEND_NUM_ARGS() == 2) {
        cmd_len = redis_cmd_format_static(&cmd, "SLOWLOG", "sl", arg, arg_len, option);
    } else {
        cmd_len = redis_cmd_format_static(&cmd, "SLOWLOG", "s", arg, arg_len);
    }

    /* Kick off our command */
    REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
    IF_ATOMIC() {
        if(redis_read_variant_reply(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL) < 0) {
            RETURN_FALSE;
        }
    }
    REDIS_PROCESS_RESPONSE(redis_read_variant_reply);
}

/* {{{ proto Redis::rawCommand(string cmd, arg, arg, arg, ...) }}} */
PHP_METHOD(Redis, rawCommand) {
    zval *z_args;
    RedisSock *redis_sock;
    int argc = ZEND_NUM_ARGS(), i;
    smart_string cmd = {0};

    /* We need at least one argument */
    z_args = emalloc(argc * sizeof(zval*));
    if (argc < 1 || zend_get_parameters_array_ex(argc, z_args) == FAILURE) {
        efree(z_args);
        RETURN_FALSE;
    }

    if (redis_sock_get(getThis(), &redis_sock, 0) < 0) {
        efree(z_args);
        RETURN_FALSE;
    }

    /* Initialize the command we'll send */
    convert_to_string(&z_args[0]);
    redis_cmd_init_sstr(&cmd,argc-1,Z_STRVAL(z_args[0]),Z_STRLEN(z_args[0]));

    /* Iterate over the remainder of our arguments, appending */
    for (i = 1; i < argc; i++) {
        convert_to_string(&z_args[i]);
        redis_cmd_append_sstr(&cmd, Z_STRVAL(z_args[i]), Z_STRLEN(z_args[i]));
    }

    efree(z_args);

    /* Kick off our request and read response or enqueue handler */
    REDIS_PROCESS_REQUEST(redis_sock, cmd.c, cmd.len);
    IF_ATOMIC() {
        if (redis_read_variant_reply(INTERNAL_FUNCTION_PARAM_PASSTHRU,
                                     redis_sock, NULL) < 0)
        {
            RETURN_FALSE;
        }
    }
    REDIS_PROCESS_RESPONSE(redis_read_variant_reply);
}

/* {{{ proto Redis::wait(int num_slaves, int ms) }}}
 */
PHP_METHOD(Redis, wait) {
    zval *object;
    RedisSock *redis_sock;
    long num_slaves, timeout;
    char *cmd;
    int cmd_len;

    /* Make sure arguments are valid */
    if(zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "Oll",
                                    &object, redis_ce, &num_slaves, &timeout)
                                    ==FAILURE)
    {
        RETURN_FALSE;
    }

    /* Don't even send this to Redis if our args are negative */
    if(num_slaves < 0 || timeout < 0) {
        RETURN_FALSE;
    }

    /* Grab our socket */
    if(redis_sock_get(object, &redis_sock, 0)<0) {
        RETURN_FALSE;
    }

    /* Construct the command */
    cmd_len = redis_cmd_format_static(&cmd, "WAIT", "ll", num_slaves, timeout);

    /* Kick it off */
    REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
    IF_ATOMIC() {
        redis_long_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
    }
    REDIS_PROCESS_RESPONSE(redis_long_response);
}

/*
 * Construct a PUBSUB command
 */
PHP_REDIS_API int
redis_build_pubsub_cmd(RedisSock *redis_sock, char **ret, PUBSUB_TYPE type,
                       zval *arg)
{
    HashTable *ht_chan;
    HashPosition ptr;
    zval *z_ele;
    zend_string *key;
    int cmd_len;
    smart_string cmd = {0};

    if(type == PUBSUB_CHANNELS) {
        if(arg) {
			zend_string *tmp = zval_get_string(arg);

            /* Get string argument and length and prefix if necessary */
            key = redis_key_prefix(redis_sock, tmp);
			zend_string_release(tmp);

            /* With a pattern */
            cmd_len = redis_cmd_format_static(ret, "PUBSUB", "ss", "CHANNELS", sizeof("CHANNELS")-1,
                                              key->val, key->len);

            /* Free the channel name if we prefixed it */
            zend_string_release(tmp);
            zend_string_release(key);

            /* Return command length */
            return cmd_len;
        } else {
            /* No pattern */
            return redis_cmd_format_static(ret, "PUBSUB", "s", "CHANNELS", sizeof("CHANNELS")-1);
        }
    } else if(type == PUBSUB_NUMSUB) {
        ht_chan = Z_ARRVAL_P(arg);

        /* Add PUBSUB and NUMSUB bits */
        redis_cmd_init_sstr(&cmd, zend_hash_num_elements(ht_chan)+1, "PUBSUB", sizeof("PUBSUB")-1);
        redis_cmd_append_sstr(&cmd, "NUMSUB", sizeof("NUMSUB")-1);

        /* Iterate our elements */
		ZEND_HASH_FOREACH_VAL(ht_chan, z_ele) {
			zend_string *tmp = zval_get_string(arg);

			/* Get key */
			tmp = zval_get_string(z_ele);

            /* Apply prefix if required */
            key = redis_key_prefix(redis_sock, tmp);

            /* Append this channel */
            redis_cmd_append_sstr(&cmd, key->val, key->len);

            /* Free key if prefixed */
            zend_string_release(tmp);
            zend_string_release(key);
        } ZEND_HASH_FOREACH_END();

        /* Set return */
        *ret = cmd.c;
        return cmd.len;
    } else if(type == PUBSUB_NUMPAT) {
        return redis_cmd_format_static(ret, "PUBSUB", "s", "NUMPAT", sizeof("NUMPAT")-1);
    }

    /* Shouldn't ever happen */
    return -1;
}

/*
 * {{{ proto Redis::pubsub("channels", pattern);
 *     proto Redis::pubsub("numsub", Array channels);
 *     proto Redis::pubsub("numpat"); }}}
 */
PHP_METHOD(Redis, pubsub) {
    zval *object;
    RedisSock *redis_sock;
    char *keyword, *cmd;
    int kw_len, cmd_len;
    PUBSUB_TYPE type;
    zval *arg=NULL;

    /* Parse arguments */
    if(zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "Os|z",
                                    &object, redis_ce, &keyword, &kw_len, &arg)
                                    ==FAILURE)
    {
        RETURN_FALSE;
    }

    /* Validate our sub command keyword, and that we've got proper arguments */
    if(!strncasecmp(keyword, "channels", sizeof("channels"))) {
        /* One (optional) string argument */
        if(arg && Z_TYPE_P(arg) != IS_STRING) {
            RETURN_FALSE;
        }
        type = PUBSUB_CHANNELS;
    } else if(!strncasecmp(keyword, "numsub", sizeof("numsub"))) {
        /* One array argument */
        if(ZEND_NUM_ARGS() < 2 || Z_TYPE_P(arg) != IS_ARRAY ||
           zend_hash_num_elements(Z_ARRVAL_P(arg))==0)
        {
            RETURN_FALSE;
        }
        type = PUBSUB_NUMSUB;
    } else if(!strncasecmp(keyword, "numpat", sizeof("numpat"))) {
        type = PUBSUB_NUMPAT;
    } else {
        /* Invalid keyword */
        RETURN_FALSE;
    }

    /* Grab our socket context object */
    if(redis_sock_get(object, &redis_sock, 0)<0) {
        RETURN_FALSE;
    }

    /* Construct our "PUBSUB" command */
    cmd_len = redis_build_pubsub_cmd(redis_sock, &cmd, type, arg);

    REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);

    if(type == PUBSUB_NUMSUB) {
        IF_ATOMIC() {
            if(redis_mbulk_reply_zipped_keys_int(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL)<0) {
                RETURN_FALSE;
            }
        }
        REDIS_PROCESS_RESPONSE(redis_mbulk_reply_zipped_keys_int);
    } else {
        IF_ATOMIC() {
            if(redis_read_variant_reply(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL)<0) {
                RETURN_FALSE;
            }
        }
        REDIS_PROCESS_RESPONSE(redis_read_variant_reply);
    }
}

/* Construct an EVAL or EVALSHA command, with option argument array and number of arguments that are keys parameter */
PHP_REDIS_API int
redis_build_eval_cmd(RedisSock *redis_sock, char **ret, char *keyword, char *value, int value_len, zval *args, int keys_count) {
	zval *elem;
	HashTable *args_hash;
	HashPosition hash_pos;
	int cmd_len, args_count = 0;
	int eval_cmd_count = 2;

	/* If we've been provided arguments, we'll want to include those in our eval command */
	if(args != NULL) {
		/* Init our hash array value, and grab the count */
	    args_hash = Z_ARRVAL_P(args);
	    args_count = zend_hash_num_elements(args_hash);

	    /* We only need to process the arguments if the array is non empty */
	    if(args_count >  0) {
	    	/* Header for our EVAL command */
	    	cmd_len = redis_cmd_format_header(ret, keyword, eval_cmd_count + args_count);

	    	/* Now append the script itself, and the number of arguments to treat as keys */
	    	cmd_len = redis_cmd_append_str(ret, cmd_len, value, value_len);
	    	cmd_len = redis_cmd_append_int(ret, cmd_len, keys_count);

			/* Iterate the values in our "keys" array */
			ZEND_HASH_FOREACH_VAL(args_hash, elem) {
				zend_string *key = zval_get_string(elem);

				/* Keep track of the old command pointer */
				char *old_cmd = *ret;

				/* If this is still a key argument, prefix it if we've been set up to prefix keys */
				key = keys_count-- > 0 ? redis_key_prefix(redis_sock, key) : 0;

				/* Append this key to our EVAL command, free our old command */
				cmd_len = redis_cmd_format(ret, "%s$%d" _NL "%s" _NL, *ret, cmd_len, key->val, key->len);
				efree(old_cmd);

				/* Free our key, old command if we need to */
				zend_string_release(key);
			} ZEND_HASH_FOREACH_END();
	    }
	}

	/* If there weren't any arguments (none passed, or an empty array), construct a standard no args command */
	if(args_count < 1) {
		cmd_len = redis_cmd_format_static(ret, keyword, "sd", value, 0);
	}

	/* Return our command length */
	return cmd_len;
}

/* {{{ proto variant Redis::evalsha(string script_sha1, [array keys, int num_key_args])
 */
PHP_METHOD(Redis, evalsha)
{
	zval *object, *args= NULL;
	char *cmd, *sha;
	int cmd_len, sha_len;
	long keys_count = 0;
	RedisSock *redis_sock;

	if(zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "Os|al",
								    &object, redis_ce, &sha, &sha_len, &args, &keys_count) == FAILURE) {
		RETURN_FALSE;
	}

	/* Attempt to grab socket */
	if(redis_sock_get(object, &redis_sock, 0) < 0) {
		RETURN_FALSE;
	}

	/* Construct our EVALSHA command */
	cmd_len = redis_build_eval_cmd(redis_sock, &cmd, "EVALSHA", sha, sha_len, args, keys_count);

	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
	IF_ATOMIC() {
		if(redis_read_variant_reply(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL) < 0) {
			RETURN_FALSE;
		}
	}
	REDIS_PROCESS_RESPONSE(redis_read_variant_reply);
}

/* {{{ proto variant Redis::eval(string script, [array keys, int num_key_args])
 */
PHP_METHOD(Redis, eval)
{
	zval *object, *args = NULL;
	RedisSock *redis_sock;
	char *script, *cmd = "";
	int script_len, cmd_len;
	long keys_count = 0;

	/* Attempt to parse parameters */
	if(zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "Os|al",
							 &object, redis_ce, &script, &script_len, &args, &keys_count) == FAILURE) {
		RETURN_FALSE;
	}

	/* Attempt to grab socket */
    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

    /* Construct our EVAL command */
    cmd_len = redis_build_eval_cmd(redis_sock, &cmd, "EVAL", script, script_len, args, keys_count);

    REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
    IF_ATOMIC() {
    	if(redis_read_variant_reply(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL) < 0) {
    		RETURN_FALSE;
    	}
    }
    REDIS_PROCESS_RESPONSE(redis_read_variant_reply);
}

PHP_REDIS_API int
redis_build_script_exists_cmd(char **ret, zval *argv, int argc) {
	zend_string *str;

	/* Our command length and iterator */
	int cmd_len = 0, i;

	/* Start building our command */
	cmd_len = redis_cmd_format_header(ret, "SCRIPT", argc + 1); /* +1 for "EXISTS" */
	cmd_len = redis_cmd_append_str(ret, cmd_len, "EXISTS", 6);

	/* Iterate our arguments */
	for(i=0;i<argc;i++) {
		/* Get our string */
		str = zval_get_string(&argv[1]);

		/* Append this script sha to our SCRIPT EXISTS command */
		cmd_len = redis_cmd_append_str(ret, cmd_len, str->val, str->len);

		/* Release it */
		zend_string_release(str);
	}

	/* Success */
	return cmd_len;
}

/* {{{ proto status Redis::script('flush')
 * {{{ proto status Redis::script('kill')
 * {{{ proto string Redis::script('load', lua_script)
 * {{{ proto int Reids::script('exists', script_sha1 [, script_sha2, ...])
 */
PHP_METHOD(Redis, script) {
	zval *z_args;
	RedisSock *redis_sock;
	int cmd_len, argc;
	char *cmd;

	/* Attempt to grab our socket */
	if(redis_sock_get(getThis(), &redis_sock, 0) < 0) {
		RETURN_FALSE;
	}

	/* Grab the number of arguments */
	argc = ZEND_NUM_ARGS();

	/* Allocate an array big enough to store our arguments */
	z_args = emalloc(argc * sizeof(zval*));

	/* Make sure we can grab our arguments, we have a string directive */
	if(zend_get_parameters_array_ex(argc, z_args) == FAILURE ||
	   (argc < 1 || Z_TYPE(z_args[0]) != IS_STRING))
	{
		efree(z_args);
		RETURN_FALSE;
	}

	/* Branch based on the directive */
	if(!strcasecmp(Z_STRVAL(z_args[0]), "flush") || !strcasecmp(Z_STRVAL(z_args[0]), "kill")) {
		/* Simple SCRIPT FLUSH, or SCRIPT_KILL command */
		cmd_len = redis_cmd_format_static(&cmd, "SCRIPT", "s", Z_STRVAL(z_args[0]), Z_STRLEN(z_args[0]));
	} else if(!strcasecmp(Z_STRVAL(z_args[0]), "load")) {
		/* Make sure we have a second argument, and it's not empty.  If it is */
		/* empty, we can just return an empty array (which is what Redis does) */
		if(argc < 2 || Z_TYPE(z_args[1]) != IS_STRING || Z_STRLEN(z_args[1]) < 1) {
			/* Free our args */
			efree(z_args);
			RETURN_FALSE;
		}

		/* Format our SCRIPT LOAD command */
		cmd_len = redis_cmd_format_static(&cmd, "SCRIPT", "ss", "LOAD", 4, Z_STRVAL(z_args[1]), Z_STRLEN(z_args[1]));
	} else if(!strcasecmp(Z_STRVAL(z_args[0]), "exists")) {
		/* Construct our SCRIPT EXISTS command */
		cmd_len = redis_build_script_exists_cmd(&cmd, &(z_args[1]), argc-1);
	} else {
		/* Unknown directive */
		efree(z_args);
		RETURN_FALSE;
	}

	/* Free our alocated arguments */
	efree(z_args);

	/* Kick off our request */
	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
	IF_ATOMIC() {
		if(redis_read_variant_reply(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL) < 0) {
			RETURN_FALSE;
		}
	}
	REDIS_PROCESS_RESPONSE(redis_read_variant_reply);
}

/* {{{ proto DUMP key
 */
PHP_METHOD(Redis, dump) {
	zval *object;
	RedisSock *redis_sock;
	zend_string *key;
	char *cmd;
	int cmd_len;

	/* Parse our arguments */
	if(zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "Os", &object, redis_ce,
									&key) == FAILURE) {
		RETURN_FALSE;
	}

	/* Grab our socket */
	if(redis_sock_get(object, &redis_sock, 0) < 0) {
		RETURN_FALSE;
	}

	/* Prefix our key if we need to */
	key = redis_key_prefix(redis_sock, key);
	cmd_len = redis_cmd_format_static(&cmd, "DUMP", "s", key->val, key->len);
	zend_string_release(key);

	/* Kick off our request */
	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
	IF_ATOMIC() {
		redis_ping_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
	}
	REDIS_PROCESS_RESPONSE(redis_ping_response);
}

/* {{{ proto Redis::DEBUG(string key) */
PHP_METHOD(Redis, debug) {
    zval *object;
    RedisSock *redis_sock;
    zend_string *key;
    char *cmd;
    int cmd_len;

    if(zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OS",
                                    &object, redis_ce, &key)==FAILURE)
    {
        RETURN_FALSE;
    }

    /* Grab our socket */
    if(redis_sock_get(object, &redis_sock, 0)<0) {
        RETURN_FALSE;
    }

    /* Prefix key, format command */
    key = redis_key_prefix(redis_sock, key);
    cmd_len = redis_cmd_format_static(&cmd, "DEBUG", "ss", "OBJECT", sizeof("OBJECT")-1, key->val, key->len);
    zend_string_release(key);

    /* Kick it off */
    REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
    IF_ATOMIC() {
        redis_debug_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
    }
    REDIS_PROCESS_RESPONSE(redis_debug_response);
}

/*
 * {{{ proto Redis::restore(ttl, key, value)
 */
PHP_METHOD(Redis, restore) {
	zval *object;
	RedisSock *redis_sock;
    zend_string *key;
	char *cmd, *value;
	int cmd_len, value_len;
	long ttl;

	/* Parse our arguments */
	if(zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OSls", &object, redis_ce,
									&key, &ttl, &value, &value_len) == FAILURE) {
		RETURN_FALSE;
	}

	/* Grab our socket */
	if(redis_sock_get(object, &redis_sock, 0) < 0) {
		RETURN_FALSE;
	}

	/* Prefix the key if we need to */
	key = redis_key_prefix(redis_sock, key);
	cmd_len = redis_cmd_format_static(&cmd, "RESTORE", "sls", key->val, key->len, ttl, value, value_len);
	zend_string_release(key);

	/* Kick off our restore request */
	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
	IF_ATOMIC() {
		redis_boolean_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
	}
	REDIS_PROCESS_RESPONSE(redis_boolean_response);
}

/*
 * {{{ proto Redis::migrate(host port key dest-db timeout [bool copy, bool replace])
 */
PHP_METHOD(Redis, migrate) {
	zval *object;
	RedisSock *redis_sock;
    zend_string *key;
	char *cmd, *host;
	int cmd_len, host_len;
    zend_bool copy=0, replace=0;
	long port, dest_db, timeout;

	/* Parse arguments */
	if(zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OslSll|bb", &object, redis_ce,
									&host, &host_len, &port, &key, &dest_db, &timeout,
                                    &copy, &replace) == FAILURE) {
		RETURN_FALSE;
	}

	/* Grabg our socket */
	if(redis_sock_get(object, &redis_sock, 0) < 0) {
		RETURN_FALSE;
	}

	/* Prefix our key if we need to, build our command */
	key = redis_key_prefix(redis_sock, key);

    /* Construct our command */
    if(copy && replace) {
        cmd_len = redis_cmd_format_static(&cmd, "MIGRATE", "sdsddss", host, host_len, port,
                                          key->val, key->len, dest_db, timeout, "COPY",
                                          sizeof("COPY")-1, "REPLACE", sizeof("REPLACE")-1);
    } else if(copy) {
        cmd_len = redis_cmd_format_static(&cmd, "MIGRATE", "sdsdds", host, host_len, port,
                                          key->val, key->len, dest_db, timeout, "COPY",
                                          sizeof("COPY")-1);
    } else if(replace) {
        cmd_len = redis_cmd_format_static(&cmd, "MIGRATE", "sdsdds", host, host_len, port,
                                          key->val, key->len, dest_db, timeout, "REPLACE",
                                          sizeof("REPLACE")-1);
    } else {
        cmd_len = redis_cmd_format_static(&cmd, "MIGRATE", "sdsdd", host, host_len, port,
                                          key->val, key->len, dest_db, timeout);
    }

    /* Free our key if we prefixed it */
	zend_string_release(key);

	/* Kick off our MIGRATE request */
	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
	IF_ATOMIC() {
		redis_boolean_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
	}
	REDIS_PROCESS_RESPONSE(redis_boolean_response);
}

/*
 * {{{ proto Redis::_prefix(key)
 */
PHP_METHOD(Redis, _prefix) {
	zval *object;
	RedisSock *redis_sock;
	zend_string *key;
	int key_len;

	/* Parse our arguments */
	if(zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OS", &object, redis_ce,
								    &key) == FAILURE) {
		RETURN_FALSE;
	}
	/* Grab socket */
	if(redis_sock_get(object, &redis_sock, 0) < 0) {
		RETURN_FALSE;
	}

	/* Prefix our key if we need to */
	if(redis_sock->prefix != NULL && redis_sock->prefix_len > 0) {
		key = redis_key_prefix(redis_sock, key);
		RETURN_STR(key);
	} else {
		RETURN_STR(key);
	}
}

/*
 * {{{ proto Redis::_serialize(value)
 */
PHP_METHOD(Redis, _serialize) {
    zval *object;
    RedisSock *redis_sock;
    zval *z_val;
    zend_string *val;

    /* Parse arguments */
    if(zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "Oz",
                                    &object, redis_ce, &z_val) == FAILURE)
    {
        RETURN_FALSE;
    }

    /* Grab socket */
    if(redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

    /* Serialize, which will return a value even if no serializer is set */
    val = redis_serialize(redis_sock, z_val);

    /* Return serialized value.  Tell PHP to make a copy as some can be interned. */
    RETURN_STR(val);
}

/*
 * {{{ proto Redis::_unserialize(value)
 */
PHP_METHOD(Redis, _unserialize) {
	zval *object;
	RedisSock *redis_sock;
	char *value;
	int value_len;

	/* Parse our arguments */
	if(zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "Os", &object, redis_ce,
									&value, &value_len) == FAILURE) {
		RETURN_FALSE;
	}
	/* Grab socket */
	if(redis_sock_get(object, &redis_sock, 0) < 0) {
		RETURN_FALSE;
	}

	/* We only need to attempt unserialization if we have a serializer running */
	if(redis_sock->serializer != REDIS_SERIALIZER_NONE) {
		zval z_ret;
		if(redis_unserialize(redis_sock, value, value_len, &z_ret) == 0) {
			/* Badly formed input, throw an execption */
			zend_throw_exception(redis_exception_ce, "Invalid serialized data, or unserialization error", 0);
			RETURN_FALSE;
		}
		RETURN_ZVAL(&z_ret, 1, 1);
	} else {
		/* Just return the value that was passed to us */
		RETURN_STRINGL(value, value_len);
	}
}

/*
 * {{{ proto Redis::getLastError()
 */
PHP_METHOD(Redis, getLastError) {
	zval *object;
	RedisSock *redis_sock;

	/* Grab our object */
	if(zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "O", &object, redis_ce) == FAILURE) {
		RETURN_FALSE;
	}
	/* Grab socket */
	if(redis_sock_get(object, &redis_sock, 0) < 0) {
		RETURN_FALSE;
	}

	/* Return our last error or NULL if we don't have one */
	if(redis_sock->err != NULL && redis_sock->err_len > 0) {
		RETURN_STRINGL(redis_sock->err, redis_sock->err_len);
	} else {
		RETURN_NULL();
	}
}

/*
 * {{{ proto Redis::clearLastError()
 */
PHP_METHOD(Redis, clearLastError) {
	zval *object;
	RedisSock *redis_sock;

	/* Grab our object */
	if(zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "O", &object, redis_ce) == FAILURE) {
		RETURN_FALSE;
	}
	/* Grab socket */
	if(redis_sock_get(object, &redis_sock, 0) < 0) {
		RETURN_FALSE;
	}

	/* Clear error message */
	if(redis_sock->err) {
		efree(redis_sock->err);
	}
	redis_sock->err = NULL;

	RETURN_TRUE;
}

/*
 * {{{ proto long Redis::getMode()
 */
PHP_METHOD(Redis, getMode) {
    zval *object;
    RedisSock *redis_sock;

    /* Grab our object */
    if (zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "O", &object, redis_ce) == FAILURE) {
        RETURN_FALSE;
    }

    /* Grab socket */
    if (redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

    RETVAL_LONG(redis_sock->mode);
}

/*
 * {{{ proto Redis::time()
 */
PHP_METHOD(Redis, time) {
	zval *object;
	RedisSock *redis_sock;
	char *cmd;
	int cmd_len;

	/* Grab our object */
	if(zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "O", &object, redis_ce) == FAILURE) {
		RETURN_FALSE;
	}
	/* Grab socket */
	if(redis_sock_get(object, &redis_sock, 0) < 0) {
		RETURN_FALSE;
	}

	/* Build TIME command */
	cmd_len = redis_cmd_format_static(&cmd, "TIME", "");

	/* Execute or queue command */
	REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
	IF_ATOMIC() {
		if(redis_mbulk_reply_raw(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL) < 0) {
			RETURN_FALSE;
		}
	}
	REDIS_PROCESS_RESPONSE(redis_mbulk_reply_raw);
}

/*
 * Introspection stuff
 */

/*
 * {{{ proto Redis::IsConnected
 */
PHP_METHOD(Redis, isConnected) {
    RedisSock *redis_sock;

    if((redis_sock = redis_sock_get_connected(INTERNAL_FUNCTION_PARAM_PASSTHRU))) {
        RETURN_TRUE;
    } else {
        RETURN_FALSE;
    }
}

/*
 * {{{ proto Redis::getHost()
 */
PHP_METHOD(Redis, getHost) {
    RedisSock *redis_sock;

    if((redis_sock = redis_sock_get_connected(INTERNAL_FUNCTION_PARAM_PASSTHRU))) {
		zend_string *str = zend_string_init(redis_sock->host, strlen(redis_sock->host)-1, 0);
        RETURN_STR(str);
    } else {
        RETURN_FALSE;
    }
}

/*
 * {{{ proto Redis::getPort()
 */
PHP_METHOD(Redis, getPort) {
    RedisSock *redis_sock;

    if((redis_sock = redis_sock_get_connected(INTERNAL_FUNCTION_PARAM_PASSTHRU))) {
        /* Return our port */
        RETURN_LONG(redis_sock->port);
    } else {
        RETURN_FALSE;
    }
}

/*
 * {{{ proto Redis::getDBNum
 */
PHP_METHOD(Redis, getDBNum) {
    RedisSock *redis_sock;

    if((redis_sock = redis_sock_get_connected(INTERNAL_FUNCTION_PARAM_PASSTHRU))) {
        /* Return our db number */
        RETURN_LONG(redis_sock->dbNumber);
    } else {
        RETURN_FALSE;
    }
}

/*
 * {{{ proto Redis::getTimeout
 */
PHP_METHOD(Redis, getTimeout) {
    RedisSock *redis_sock;

    if((redis_sock = redis_sock_get_connected(INTERNAL_FUNCTION_PARAM_PASSTHRU))) {
        RETURN_DOUBLE(redis_sock->timeout);
    } else {
        RETURN_FALSE;
    }
}

/*
 * {{{ proto Redis::getReadTimeout
 */
PHP_METHOD(Redis, getReadTimeout) {
    RedisSock *redis_sock;

    if((redis_sock = redis_sock_get_connected(INTERNAL_FUNCTION_PARAM_PASSTHRU))) {
        RETURN_DOUBLE(redis_sock->read_timeout);
    } else {
        RETURN_FALSE;
    }
}

/*
 * {{{ proto Redis::getPersistentID
 */
PHP_METHOD(Redis, getPersistentID) {
    RedisSock *redis_sock;

    if((redis_sock = redis_sock_get_connected(INTERNAL_FUNCTION_PARAM_PASSTHRU))) {
        if(redis_sock->persistent_id != NULL) {
			zend_string *str = zend_string_init(redis_sock->persistent_id, strlen(redis_sock->persistent_id), 0);
	        RETURN_STR(str);
        } else {
            RETURN_NULL();
        }
    } else {
        RETURN_FALSE;
    }
}

/*
 * {{{ proto Redis::getAuth
 */
PHP_METHOD(Redis, getAuth) {
    RedisSock *redis_sock;

    if((redis_sock = redis_sock_get_connected(INTERNAL_FUNCTION_PARAM_PASSTHRU))) {
        if(redis_sock->auth != NULL) {
            zend_string *str = zend_string_init(redis_sock->auth, strlen(redis_sock->auth), 0);
            RETURN_STR(str);
        } else {
            RETURN_NULL();
        }
    } else {
        RETURN_FALSE;
    }
}

/*
 * $redis->client('list');
 * $redis->client('kill', <ip:port>);
 * $redis->client('setname', <name>);
 * $redis->client('getname');
 */
PHP_METHOD(Redis, client) {
    zval *object;
    RedisSock *redis_sock;
    char *cmd, *opt=NULL, *arg=NULL;
    int cmd_len, opt_len, arg_len;

    /* Parse our method parameters */
    if(zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "Os|s",
        &object, redis_ce, &opt, &opt_len, &arg, &arg_len) == FAILURE)
    {
        RETURN_FALSE;
    }

    /* Grab our socket */
    if(redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

    /* Build our CLIENT command */
    if(ZEND_NUM_ARGS() == 2) {
        cmd_len = redis_cmd_format_static(&cmd, "CLIENT", "ss", opt, opt_len,
                                          arg, arg_len);
    } else {
        cmd_len = redis_cmd_format_static(&cmd, "CLIENT", "s", opt, opt_len);
    }

    /* Execute our queue command */
    REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);

    /* We handle CLIENT LIST with a custom response function */
    if(!strncasecmp(opt, "list", 4)) {
        IF_ATOMIC() {
            redis_client_list_reply(INTERNAL_FUNCTION_PARAM_PASSTHRU,redis_sock,NULL);
        }
        REDIS_PROCESS_RESPONSE(redis_client_list_reply);
    } else {
        IF_ATOMIC() {
            redis_read_variant_reply(INTERNAL_FUNCTION_PARAM_PASSTHRU,redis_sock,NULL);
        }
        REDIS_PROCESS_RESPONSE(redis_read_variant_reply);
    }
}

/**
 * Helper to format any combination of SCAN arguments
 */
PHP_REDIS_API int
redis_build_scan_cmd(char **cmd, REDIS_SCAN_TYPE type, zend_string *key,
                     int iter, zend_string *pattern, int count)
{
    char *keyword;
    int arg_count, cmd_len;

    /* Count our arguments +1 for key if it's got one, and + 2 for pattern */
    /* or count given that they each carry keywords with them. */
    arg_count = 1 + (key->len>0) + (pattern->len>0?2:0) + (count>0?2:0);

    /* Turn our type into a keyword */
    switch(type) {
        case TYPE_SCAN:
            keyword = "SCAN";
            break;
        case TYPE_SSCAN:
            keyword = "SSCAN";
            break;
        case TYPE_HSCAN:
            keyword = "HSCAN";
            break;
        case TYPE_ZSCAN:
		default:
            keyword = "ZSCAN";
            break;
    }

    /* Start the command */
    cmd_len = redis_cmd_format_header(cmd, keyword, arg_count);

    /* Add the key in question if we have one */
    if(key->len) {
        cmd_len = redis_cmd_append_str(cmd, cmd_len, key->val, key->len);
    }

    /* Add our iterator */
    cmd_len = redis_cmd_append_int(cmd, cmd_len, iter);

    /* Append COUNT if we've got it */
    if(count) {
        cmd_len = redis_cmd_append_str(cmd, cmd_len, "COUNT", sizeof("COUNT")-1);
        cmd_len = redis_cmd_append_int(cmd, cmd_len, count);
    }

    /* Append MATCH if we've got it */
    if(pattern->len) {
        cmd_len = redis_cmd_append_str(cmd, cmd_len, "MATCH", sizeof("MATCH")-1);
        cmd_len = redis_cmd_append_str(cmd, cmd_len, pattern->val, pattern->len);
    }

    /* Return our command length */
    return cmd_len;
}

/**
 * {{{ proto redis::scan(&$iterator, [pattern, [count]])
 */
PHP_REDIS_API void
generic_scan_cmd(INTERNAL_FUNCTION_PARAMETERS, REDIS_SCAN_TYPE type) {
    zval *object, *z_iter;
    RedisSock *redis_sock;
    HashTable *hash;
    zend_string *key, *pattern;
    char *cmd;
    int cmd_len, num_elements=0;
    long count=0, iter;

    /* Different prototype depending on if this is a key based scan */
    if(type != TYPE_SCAN) {
        /* Requires a key */
        if(zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "OSz/|S!l",
                                        &object, redis_ce, &key, &z_iter,
                                        &pattern, &count)==FAILURE)
        {
            RETURN_FALSE;
        }
    } else {
        /* Doesn't require a key */
        if(zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "Oz/|S!l",
                                        &object, redis_ce, &z_iter, &pattern,
                                        &count) == FAILURE)
        {
            RETURN_FALSE;
        }
    }

    /* Grab our socket */
    if(redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

    /* Calling this in a pipeline makes no sense */
    IF_NOT_ATOMIC() {
        php_error_docref(NULL, E_ERROR, "Can't call SCAN commands in multi or pipeline mode!");
        RETURN_FALSE;
    }

    /* The iterator should be passed in as NULL for the first iteration, but we can treat */
    /* any NON LONG value as NULL for these purposes as we've seperated the variable anyway. */
    if(Z_TYPE_P(z_iter) != IS_LONG || Z_LVAL_P(z_iter)<0) {
        /* Convert to long */
        convert_to_long(z_iter);
        iter = 0;
    } else if(Z_LVAL_P(z_iter)!=0) {
        /* Update our iterator value for the next passthru */
        iter = Z_LVAL_P(z_iter);
    } else {
        /* We're done, back to iterator zero */
        RETURN_FALSE;
    }

    /* Prefix our key if we've got one and we have a prefix set */
    if(key->len) {
        key = redis_key_prefix(redis_sock, key);
    }

    /**
     * Redis can return to us empty keys, especially in the case where there are a large
     * number of keys to scan, and we're matching against a pattern.  PHPRedis can be set
     * up to abstract this from the user, by setting OPT_SCAN to REDIS_SCAN_RETRY.  Otherwise
     * we will return empty keys and the user will need to make subsequent calls with
     * an updated iterator.
     */
    do {
        /* Free our previous reply if we're back in the loop.  We know we are
         * if our return_value is an array. */
        if(Z_TYPE_P(return_value) == IS_ARRAY) {
            zval_dtor(return_value);
            ZVAL_NULL(return_value);
        }

        /* Format our SCAN command */
        cmd_len = redis_build_scan_cmd(&cmd, type, key, (int)iter, pattern, count);

        /* Execute our command getting our new iterator value */
        REDIS_PROCESS_REQUEST(redis_sock, cmd, cmd_len);
        if(redis_sock_read_scan_reply(INTERNAL_FUNCTION_PARAM_PASSTHRU,
                                      redis_sock,type,&iter)<0)
        {
            zend_string_release(key);
            RETURN_FALSE;
        }

        /* Get the number of elements */
        hash = Z_ARRVAL_P(return_value);
        num_elements = zend_hash_num_elements(hash);
    } while(redis_sock->scan == REDIS_SCAN_RETRY && iter != 0 && num_elements == 0);

    /* Free our key if it was prefixed */
    zend_string_release(key);

    /* Update our iterator reference */
    Z_LVAL_P(z_iter) = iter;
}

PHP_METHOD(Redis, scan) {
    generic_scan_cmd(INTERNAL_FUNCTION_PARAM_PASSTHRU, TYPE_SCAN);
}
PHP_METHOD(Redis, hscan) {
    generic_scan_cmd(INTERNAL_FUNCTION_PARAM_PASSTHRU, TYPE_HSCAN);
}
PHP_METHOD(Redis, sscan) {
    generic_scan_cmd(INTERNAL_FUNCTION_PARAM_PASSTHRU, TYPE_SSCAN);
}
PHP_METHOD(Redis, zscan) {
    generic_scan_cmd(INTERNAL_FUNCTION_PARAM_PASSTHRU, TYPE_ZSCAN);
}

/*
 * HyperLogLog based commands
 */

/* {{{ proto Redis::pfAdd(string key, array elements) }}} */
PHP_METHOD(Redis, pfadd) {
    zval *object;
    RedisSock *redis_sock;
    zend_string *key;
    int argc=1;
    zval *z_mems, *z_mem;
    HashTable *ht_mems;
    HashPosition pos;
    smart_string cmd = {0};

    if(zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "Osa",
                                    &object, redis_ce, &key, &z_mems)
                                    ==FAILURE)
    {
        RETURN_FALSE;
    }

    if(redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

    // Grab members as an array
    ht_mems = Z_ARRVAL_P(z_mems);

    // Total arguments we'll be sending
    argc += zend_hash_num_elements(ht_mems);

    // If the array was empty we can just exit
    if(argc < 2) {
        RETURN_FALSE;
    }

    // Start constructing our command
    redis_cmd_init_sstr(&cmd, argc, "PFADD", sizeof("PFADD")-1);

    // Prefix our key if we're prefixing
    key = redis_key_prefix(redis_sock, key);
    redis_cmd_append_sstr(&cmd, key->val, key->len);
    zend_string_release(key);

    // Iterate over members we're adding
    ZEND_HASH_FOREACH_VAL(ht_mems, z_mem) {
        zend_string *mem;

        // Serialize if requested
        mem = redis_serialize(redis_sock, z_mem);

        // Append this member
        redis_cmd_append_sstr(&cmd, mem->val, mem->len);

        // Free memory if we serialized or converted types
        zend_string_release(mem);
    } ZEND_HASH_FOREACH_END();


    REDIS_PROCESS_REQUEST(redis_sock, cmd.c, cmd.len);
    IF_ATOMIC() {
        redis_1_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
    }
    REDIS_PROCESS_RESPONSE(redis_1_response);
}

/* {{{ proto Redis::pfCount(string key) }}}
 *     proto Redis::pfCount(array keys) }}} */
PHP_METHOD(Redis, pfcount) {
    zval *object, *z_keys, *z_key;
    HashTable *ht_keys;
    HashPosition ptr;
    RedisSock *redis_sock;
    smart_string cmd = {0};
    int num_keys;
    zend_string *key;

    if(zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "Oz",
                                    &object, redis_ce, &z_keys)==FAILURE)
    {
        RETURN_FALSE;
    }

    if(redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

    /* If we were passed an array of keys, iterate through them prefixing if
     * required and capturing lengths and if we need to free them.  Otherwise
     * attempt to treat the argument as a string and just pass one */
    if (Z_TYPE_P(z_keys) == IS_ARRAY) {
        /* Grab key hash table and the number of keys */
        ht_keys = Z_ARRVAL_P(z_keys);
        num_keys = zend_hash_num_elements(ht_keys);

        /* There is no reason to send zero keys */
        if (num_keys == 0) {
            RETURN_FALSE;
        }

        /* Initialize the command with our number of arguments */
        redis_cmd_init_sstr(&cmd, num_keys, "PFCOUNT", sizeof("PFCOUNT")-1);

        /* Append our key(s) */
        ZEND_HASH_FOREACH_VAL(ht_keys, z_key) {
            /* Append this key to our command */
            key = redis_key_prefix(redis_sock, zval_get_string(z_key));
            redis_cmd_append_sstr(&cmd, key->val, key->len);

            /* Cleanup */
            zend_string_release(key);
        } ZEND_HASH_FOREACH_END();
    } else {
        /* Construct our whole command */
        redis_cmd_init_sstr(&cmd, 1, "PFCOUNT", sizeof("PFCOUNT")-1);
        key = redis_key_prefix(redis_sock, zval_get_string(z_keys));
        redis_cmd_append_sstr(&cmd, key->val, key->len);

        /* Cleanup */
        zend_string_release(key);
    }

    REDIS_PROCESS_REQUEST(redis_sock, cmd.c, cmd.len);
    IF_ATOMIC() {
        redis_long_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
    }
    REDIS_PROCESS_RESPONSE(redis_long_response);
}
/* }}} */

/* {{{ proto Redis::pfMerge(array keys) }}}*/
PHP_METHOD(Redis, pfmerge) {
    zval *object;
    RedisSock *redis_sock;
    zval *z_keys, *z_key;
    HashTable *ht_keys;
    HashPosition pos;
    smart_string cmd = {0};
    int argc=1;
    zend_string *key;

    if(zend_parse_method_parameters(ZEND_NUM_ARGS(), getThis(), "Osa",
                                    &object, redis_ce, &key, &z_keys)==FAILURE)
    {
        RETURN_FALSE;
    }

    if(redis_sock_get(object, &redis_sock, 0) < 0) {
        RETURN_FALSE;
    }

    // Grab keys as an array
    ht_keys = Z_ARRVAL_P(z_keys);

    // Total arguments we'll be sending
    argc += zend_hash_num_elements(ht_keys);

    // If no keys were passed we can abort
    if(argc<2) {
        RETURN_FALSE;
    }

    // Initial construction of our command
    redis_cmd_init_sstr(&cmd, argc, "PFMERGE", sizeof("PFMERGE")-1);

    // Add our destination key (prefixed if necessary)
    key = redis_key_prefix(redis_sock, key);
    redis_cmd_append_sstr(&cmd, key->val, key->len);
    zend_string_release(key);

    // Iterate our keys array
    ZEND_HASH_FOREACH_VAL(ht_keys, z_key) {
        // Prefix our key if necessary and append this key
        key = redis_key_prefix(redis_sock, zval_get_string(z_key));
        redis_cmd_append_sstr(&cmd, key->val, key->len);
        zend_string_release(key);
    } ZEND_HASH_FOREACH_END();

    REDIS_PROCESS_REQUEST(redis_sock, cmd.c, cmd.len);
    IF_ATOMIC() {
        redis_boolean_response(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, NULL, NULL);
    }
    REDIS_PROCESS_RESPONSE(redis_boolean_response);
}

/* vim: set tabstop=4 softtabstops=4 noexpandtab shiftwidth=4: */
