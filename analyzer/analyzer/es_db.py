
import time
import logging
import elasticsearch as es

from . import config
from . import models
from .log import logger

ES_CLIENT = None

# ES_CLIENT = es.Elasticsearch([f"http://{ip}:{port}"], timeout=10, retry_on_timeout=True, max_retries=10)

def save(index, id=None, data=None):
    try:
        if id:
            res = ES_CLIENT.index(index=index,
                                   doc_type=models.ES_DOC_TYPE,
                                   id=id,
                                   body=data,
                                   timeout="10s")
        else:
            res = ES_CLIENT.index(index=index,
                                  doc_type=models.ES_DOC_TYPE,
                                  body=data,
                                  timeout="10s")

    except Exception as e:
        logger.error(f"save to es fail, reason: {e}")


def update(index, id, data):
    try:
        res = ES_CLIENT.update(index=index,
                               doc_type=models.ES_DOC_TYPE,
                               id=id,
                               body={'doc': data, 'upsert': data},
                               timeout="10s")
    except Exception as e:
        logger.error(f"save to es fail, reason: {e}")


def delete(type, key):
    try:
        res = ES_CLIENT.delete(index=type, doc_type='doc_type', id=key, ignore=[404, 400])
    except Exception as e:
        logging.error(f"es delete fail, reason: {e}")


def exists(index, id):
    return ES_CLIENT.exists(index, models.ES_DOC_TYPE, id)


def bulk(body, doc_type=None, index=None, **kwargs):
    ES_CLIENT.bulk(body=body, index=index, doc_type=doc_type, params=kwargs)


def get(index, id):
    res = ES_CLIENT.get(index, models.ES_DOC_TYPE, id, ignore=[404, 400])
    if res.get('found'):
        return res.get('_source')
    else:
        return None


def search(index, body, timeout='10s'):
    return ES_CLIENT.search(index=index, timeout=timeout, body=body)


def scroll_search(index, body, scroll='1m', timeout='10s',  page=1, size=10):
    """
    scroll机制检索，适用于用于查询大量数据
    :param index: 索引
    :param body: ES API原型参数(用以适应各种情景的检索)
    :param scroll: 滚屏快照保持时间
    :param timeout: 单次API交互超时时间
    :param size: 单次API获取大小
    :return: _source内容组成的列表
    """
    try:
        data = ES_CLIENT.search(index=index, scroll=scroll, timeout=timeout, size=size, body=body)
        total = data["hits"]["total"]
        if total <= 0:
            return 0, []
        else:
            pages = int(total / size) + 1

            scroll_id = data["_scroll_id"]

            if page == 1:
                return total, [item['_source'] for item in data.get("hits").get("hits")]

            for i in range(2, pages+1):
                res = ES_CLIENT.scroll(scroll_id=scroll_id, scroll=scroll)
                scroll_id = res['_scroll_id']
                if page == i:
                    return total, [item['_source'] for item in res["hits"]["hits"]]

    except Exception as e:
        logger.error(f"elasticsearch scroll_search error, reason: {e}")
        return 0, []

    return 0, []


def create_template(index, template):
    ES_CLIENT.indices.put_template(name=index, body=template)


def es_db_init():
    global ES_CLIENT
    while True:
        ip, port = config.get_elasticsearch_address()
        if not ip:
            time.sleep(10)
        else:
            break

    count = 0
    while True:
        count += 1
        try:
            ES_CLIENT = es.Elasticsearch([f"http://{ip}:{port}"], timeout=10, retry_on_timeout=True, max_retries=10)
        except Exception as e:
            logger.error(f"connect to elasticsearch error, reason: {e}, try {count}")
            time.sleep(10)
        else:
            break


