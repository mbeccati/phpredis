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

#include "common.h"

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#ifdef PHP_SESSION
#include "common.h"
#include "ext/standard/info.h"
#include "php_redis.h"
#include "redis_session.h"
#include <zend_exceptions.h>

#include "library.h"

#include "php.h"
#include "php_ini.h"
#include "php_variables.h"
#include "SAPI.h"
#include "ext/standard/url.h"

ps_module ps_mod_redis = {
	PS_MOD(redis)
};

typedef struct redis_pool_member_ {

	RedisSock *redis_sock;
	int weight;
	int database;

    char *prefix;
    size_t prefix_len;

    char *auth;
    size_t auth_len;

	struct redis_pool_member_ *next;

} redis_pool_member;

typedef struct {

	int totalWeight;
	int count;

	redis_pool_member *head;

} redis_pool;

PHP_REDIS_API redis_pool*
redis_pool_new() {
	return ecalloc(1, sizeof(redis_pool));
}

PHP_REDIS_API void
redis_pool_add(redis_pool *pool, RedisSock *redis_sock, int weight,
                int database, char *prefix, char *auth) {

	redis_pool_member *rpm = ecalloc(1, sizeof(redis_pool_member));
	rpm->redis_sock = redis_sock;
	rpm->weight = weight;
	rpm->database = database;

    rpm->prefix = prefix;
    rpm->prefix_len = (prefix?strlen(prefix):0);

    rpm->auth = auth;
    rpm->auth_len = (auth?strlen(auth):0);

	rpm->next = pool->head;
	pool->head = rpm;

	pool->totalWeight += weight;
}

PHP_REDIS_API void
redis_pool_free(redis_pool *pool) {

	redis_pool_member *rpm, *next;
    rpm = pool->head;
	while(rpm) {
		next = rpm->next;
		redis_sock_disconnect(rpm->redis_sock);
		efree(rpm->redis_sock);
		if(rpm->prefix) efree(rpm->prefix);
		if(rpm->auth) efree(rpm->auth);
		efree(rpm);
		rpm = next;
	}
	efree(pool);
}

void
redis_pool_member_auth(redis_pool_member *rpm) {
    RedisSock *redis_sock = rpm->redis_sock;
    char *response, *cmd;
    int response_len, cmd_len;

    if(!rpm->auth || !rpm->auth_len) { /* no password given. */
            return;
    }
    cmd_len = redis_cmd_format_static(&cmd, "AUTH", "s", rpm->auth, rpm->auth_len);

    if(redis_sock_write(redis_sock, cmd, cmd_len) >= 0) {
            if ((response = redis_sock_read(redis_sock, &response_len))) {
                    efree(response);
            }
    }
    efree(cmd);
}

static void
redis_pool_member_select(redis_pool_member *rpm) {
    RedisSock *redis_sock = rpm->redis_sock;
    char *response, *cmd;
    int response_len, cmd_len;

    cmd_len = redis_cmd_format_static(&cmd, "SELECT", "d", rpm->database);

    if(redis_sock_write(redis_sock, cmd, cmd_len) >= 0) {
        if ((response = redis_sock_read(redis_sock, &response_len))) {
            efree(response);
        }
    }
    efree(cmd);
}

PHP_REDIS_API redis_pool_member *
redis_pool_get_sock(redis_pool *pool, zend_string *key) {
	redis_pool_member *rpm = pool->head;
	unsigned int pos, i;

	memcpy(&pos, key->val, sizeof(pos));
	pos %= pool->totalWeight;

	for(i = 0; i < pool->totalWeight;) {
		if(pos >= i && pos < i + rpm->weight) {
			int needs_auth = 0;
            if(rpm->auth && rpm->auth_len && rpm->redis_sock->status != REDIS_SOCK_STATUS_CONNECTED) {
                    needs_auth = 1;
            }
            redis_sock_server_open(rpm->redis_sock, 0);
            if(needs_auth) {
                redis_pool_member_auth(rpm);
            }
            if(rpm->database >= 0) { /* default is -1 which leaves the choice to redis. */
                redis_pool_member_select(rpm);
            }

			return rpm;
		}
		i += rpm->weight;
        rpm = rpm->next;
	}

	return NULL;
}

/* {{{ PS_OPEN_FUNC
 */
PS_OPEN_FUNC(redis)
{

	php_url *url;
	zval params, *param;
	int i, j, path_len;

	redis_pool *pool = redis_pool_new();

	for (i=0,j=0,path_len=strlen(save_path); i<path_len; i=j+1) {
		/* find beginning of url */
		while (i<path_len && (isspace(save_path[i]) || save_path[i] == ','))
			i++;

		/* find end of url */
		j = i;
		while (j<path_len && !isspace(save_path[j]) && save_path[j] != ',')
			 j++;

		if (i < j) {
			int weight = 1;
			double timeout = 86400.0;
			int persistent = 0;
            int database = -1;
            char *prefix = NULL, *auth = NULL, *persistent_id = NULL;
            long retry_interval = 0;
			RedisSock *redis_sock;

            /* translate unix: into file: */
			if (!strncmp(save_path+i, "unix:", sizeof("unix:")-1)) {
				int len = j-i;
				char *path = estrndup(save_path+i, len);
				memcpy(path, "file:", sizeof("file:")-1);
				url = php_url_parse_ex(path, len);
				efree(path);
			} else {
				url = php_url_parse_ex(save_path+i, j-i);
			}

			if (!url) {
				char *path = estrndup(save_path+i, j-i);
				php_error_docref(NULL, E_WARNING,
					"Failed to parse session.save_path (error at offset %d, url was '%s')", i, path);
				efree(path);

				redis_pool_free(pool);
				PS_SET_MOD_DATA(NULL);
				return FAILURE;
			}

			/* parse parameters */
			if (url->query != NULL) {
				array_init(&params);

				sapi_module.treat_data(PARSE_STRING, estrdup(url->query), &params);

				if (param = zend_hash_str_find(Z_ARRVAL(params), "weight", sizeof("weight") - 1)) {
					weight = zval_get_long(param);
				}
				if (param = zend_hash_str_find(Z_ARRVAL(params), "timeout", sizeof("timeout") - 1)) {
					timeout = zval_get_double(param);
				}
				if (param = zend_hash_str_find(Z_ARRVAL(params), "persistent", sizeof("persistent") - 1)) {
					persistent = zend_is_true(param);
				}
				if (param = zend_hash_str_find(Z_ARRVAL(params), "persistent_id", sizeof("persistent_id") - 1)) {
                    persistent_id = estrndup(Z_STRVAL_P(param), Z_STRLEN_P(param));
				}
				if (param = zend_hash_str_find(Z_ARRVAL(params), "prefix", sizeof("prefix") - 1)) {
                    prefix = estrndup(Z_STRVAL_P(param), Z_STRLEN_P(param));
				}
				if (param = zend_hash_str_find(Z_ARRVAL(params), "auth", sizeof("auth") - 1)) {
                    auth = estrndup(Z_STRVAL_P(param), Z_STRLEN_P(param));
				}
				if (param = zend_hash_str_find(Z_ARRVAL(params), "database", sizeof("database") - 1)) {
                    database = zval_get_long(param);
				}
				if (param = zend_hash_str_find(Z_ARRVAL(params), "retry_interval", sizeof("retry_interval") - 1)) {
                    retry_interval = zval_get_long(param);
				}

				zval_dtor(&params);
			}

			if ((url->path == NULL && url->host == NULL) || weight <= 0 || timeout <= 0) {
				php_url_free(url);
				redis_pool_free(pool);
				PS_SET_MOD_DATA(NULL);
				return FAILURE;
			}

            if(url->host) {
                    redis_sock = redis_sock_create(url->host, strlen(url->host), url->port, timeout, persistent, persistent_id, retry_interval, 0);
            } else { /* unix */
                    redis_sock = redis_sock_create(url->path, strlen(url->path), 0, timeout, persistent, persistent_id, retry_interval, 0);
            }
			redis_pool_add(pool, redis_sock, weight, database, prefix, auth);

			php_url_free(url);
		}
	}

	if (pool->head) {
		PS_SET_MOD_DATA(pool);
		return SUCCESS;
	}

	return FAILURE;
}
/* }}} */

/* {{{ PS_CLOSE_FUNC
 */
PS_CLOSE_FUNC(redis)
{
	redis_pool *pool = PS_GET_MOD_DATA();

	if(pool){
		redis_pool_free(pool);
		PS_SET_MOD_DATA(NULL);
	}
	return SUCCESS;
}
/* }}} */

static zend_string *
redis_session_key(redis_pool_member *rpm, zend_string *key) {

	zend_string *session;
    char default_prefix[] = "PHPREDIS_SESSION:";
    char *prefix = default_prefix;
    size_t prefix_len = sizeof(default_prefix)-1;

    if(rpm->prefix) {
        prefix = rpm->prefix;
        prefix_len = rpm->prefix_len;
    }

	/* build session key */
	session = zend_string_alloc(key->len + prefix_len, 0);
    memcpy(session->val, prefix, prefix_len);
	memcpy(session->val + prefix_len, key->val, key->len);

	return session;
}


/* {{{ PS_READ_FUNC
 */
PS_READ_FUNC(redis)
{
	zend_string *session;
	char *cmd;
	int cmd_len;

	redis_pool *pool = PS_GET_MOD_DATA();
    redis_pool_member *rpm = redis_pool_get_sock(pool, key);
	RedisSock *redis_sock = rpm?rpm->redis_sock:NULL;
	if(!rpm || !redis_sock){
		return FAILURE;
	}

	/* send GET command */
	session = redis_session_key(rpm, key);
	cmd_len = redis_cmd_format_static(&cmd, "GET", "s", session->val, session->len);
	zend_string_release(session);
	if(redis_sock_write(redis_sock, cmd, cmd_len) < 0) {
		efree(cmd);
		return FAILURE;
	}
	efree(cmd);

	/* read response */
	if ((cmd = redis_sock_read(redis_sock, &cmd_len)) == NULL) {
		return FAILURE;
	}

	*val = zend_string_init(cmd, cmd_len, 0);

	return SUCCESS;
}
/* }}} */

/* {{{ PS_WRITE_FUNC
 */
PS_WRITE_FUNC(redis)
{
	zend_string *session;
	char *cmd, *response;
	int cmd_len, response_len;

	redis_pool *pool = PS_GET_MOD_DATA();
    redis_pool_member *rpm = redis_pool_get_sock(pool, key);
	RedisSock *redis_sock = rpm?rpm->redis_sock:NULL;
	if(!rpm || !redis_sock){
		return FAILURE;
	}

	/* send SET command */
	session = redis_session_key(rpm, key);
	cmd_len = redis_cmd_format_static(&cmd, "SETEX", "sds", session->val, session->len, INI_INT("session.gc_maxlifetime"), val->val, val->len);
	zend_string_release(session);
	if(redis_sock_write(redis_sock, cmd, cmd_len) < 0) {
		efree(cmd);
		return FAILURE;
	}
	efree(cmd);

	/* read response */
	if ((response = redis_sock_read(redis_sock, &response_len)) == NULL) {
		return FAILURE;
	}

	if(response_len == 3 && strncmp(response, "+OK", 3) == 0) {
		efree(response);
		return SUCCESS;
	} else {
		efree(response);
		return FAILURE;
	}
}
/* }}} */

/* {{{ PS_DESTROY_FUNC
 */
PS_DESTROY_FUNC(redis)
{
	zend_string *session;
	char *cmd, *response;
	int cmd_len, response_len;

	redis_pool *pool = PS_GET_MOD_DATA();
    redis_pool_member *rpm = redis_pool_get_sock(pool, key);
	RedisSock *redis_sock = rpm?rpm->redis_sock:NULL;
	if(!rpm || !redis_sock){
		return FAILURE;
	}

    /* send DEL command */
	session = redis_session_key(rpm, key);
	cmd_len = redis_cmd_format_static(&cmd, "DEL", "s", session->val, session->len);
	zend_string_release(session);
	if(redis_sock_write(redis_sock, cmd, cmd_len) < 0) {
		efree(cmd);
		return FAILURE;
	}
	efree(cmd);

	/* read response */
	if ((response = redis_sock_read(redis_sock, &response_len)) == NULL) {
		return FAILURE;
	}

	if(response_len == 2 && response[0] == ':' && (response[1] == '0' || response[1] == '1')) {
		efree(response);
		return SUCCESS;
	} else {
		efree(response);
		return FAILURE;
	}
}
/* }}} */

/* {{{ PS_GC_FUNC
 */
PS_GC_FUNC(redis)
{
	return SUCCESS;
}
/* }}} */

#endif
/* vim: set tabstop=4 expandtab: */

