#!/usr/bin/env python
# -*- coding:utf-8 -*-

import pymongo
import time
import sys
import getopt
import math
import queue
from threading import Thread

'''
参考：https://github.com/alibaba/MongoShake/blob/develop/scripts/comparison.py
由于有mongodb副本集需要做一个版本升级操作，跨了大版本，按照官方的逐步升级不方便，于是使用阿里巴巴开源的mongoshake进行数据同步升级，在对比数据时发现，使用自带的comparison.py脚本，
无法使用collection.aggregate([{"$sample": {"size": xx}}])函数，所以基于自带脚本进行了改进，改进内容主要集中在对比部分，通过使用collection.find().limit()来分段对比数据，并添加了汇总展示信息
以及命令行的部分，总体而言用这个脚本对比总行数速度能够接受，但是无论原版还是改版，在使用sample模式对比数据时还是比较慢的，使用时注意这点

本脚本作为非营利性使用，如有侵权留言我删除

'''

COMPARISION_COUNT = "comparison_count"
COMPARISION_MODE = "comparisonMode"
EXCLUDE_DBS = "excludeDbs"
EXCLUDE_COLLS = "excludeColls"
INCLUDE_DBS = 'includeDbs'
CONTINUE = 'continue'
LATEST_SIZE, FULL_CHECK_SIZE, CHECK_PERC = 'latest', 'lessthan', 'check_percent'
SAMPLE = "sample"
THREADS = 'threads'
BATCH_SIZE = 'batch_size'
SS = 'sample_skip'
# we don't check collections and index here because sharding's collection(`db.stats`) is splitted.
CheckList = {"objects": 1, "numExtents": 1, "ok": 1}
configure = dict()
configure['compare_result'] = {}
configure['compare_info'] = {}
SRC_VERSION = 0


def log_info(message):
    print("INFO  [%s] %s " % (time.strftime('%Y-%m-%d %H:%M:%S'), message))


def log_error(message):
    print("ERROR [%s] %s " % (time.strftime('%Y-%m-%d %H:%M:%S'), message))


class MongoCluster(object):
    # connection string
    url = ""

    def __init__(self, url):
        self.url = url
        self.conn = None

    def connect(self):
        self.conn = pymongo.MongoClient(self.url)
        return self.conn

    def close(self):
        self.conn.close()


class QThreads(Thread):
    # 多线程
    def __init__(self, iq, func, **kwargs):
        Thread.__init__(self)
        self.q = iq
        self.func = func
        self.kwargs = kwargs
        self._return = None

    def run(self):
        try:
            self._return = self.func(**self.kwargs)
        except Exception as e:
            print(e)
        finally:
            self.q.get()
            self.q.task_done()


def check(src, dst):
    """
    检查mongodb的同步源和目标端的总行数 索引数 以及数据是否一致
    :param src:源的MongoClient实例
    :param dst:目标的MongoClient实例
    :return:
    """
    log_info('**************** src database and dest database database diff info . . .')
    srcDbNames = src.list_database_names()
    dstDbNames = dst.list_database_names()
    srcDbNames = [db for db in srcDbNames if db not in configure[EXCLUDE_DBS]]
    dstDbNames = [db for db in dstDbNames if db not in configure[EXCLUDE_DBS]]
    if len(srcDbNames) != len(dstDbNames):
        log_error("DIFF => database count not equals src[%s] != dst[%s].\nsrc: %s\ndst: %s" % (len(srcDbNames),
                                                                                               len(dstDbNames),
                                                                                               srcDbNames,
                                                                                               dstDbNames))
        if len(configure[INCLUDE_DBS]) == 0:
            return False
        else:
            log_info('Checks the specified database  {} .'.format(configure[INCLUDE_DBS]))
    else:
        log_info("EQUL => database count equals")

    # Compare against the given database
    if configure[INCLUDE_DBS] != '':
        checkdbs = configure[INCLUDE_DBS]
    else:
        checkdbs = srcDbNames
    # check database names and collections

    for db in checkdbs:
        if db in configure[EXCLUDE_DBS]:
            log_info("IGNR => ignore database [%s]" % db)
            continue
        if db not in dstDbNames:
            log_error("DIFF => database [%s] only in srcDb" % (db))
            if not configure['compare_result'].get('dst_not_exists_db'):
                configure['compare_result']['dst_not_exists_db'] = []
            configure['compare_result']['dst_not_exists_db'].append(
                'XXXX [%s],db only in srcDb,include collections: %s' % (db, src[db].list_collection_names()))
            # Whether to exit after an error
            if not configure[CONTINUE]:
                return False
        # db.stats() comparison
        srcDb = src[db]
        dstDb = dst[db]
        version = srcDb.command('buildinfo')['version']
        global SRC_VERSION
        SRC_VERSION = int(''.join(version.split('.')[:2]))

        # for collections in db
        srcColls = srcDb.list_collection_names()
        dstColls = dstDb.list_collection_names()
        srcColls = [coll for coll in srcColls if coll not in configure[EXCLUDE_COLLS] and srcColls.count(coll) > 0]
        dstColls = [coll for coll in dstColls if coll not in configure[EXCLUDE_COLLS] and dstColls.count(coll) > 0]

        if len(srcColls) != len(dstColls):
            log_error("DIFF => database [%s] collections count not equals, src[%s], dst[%s]" % (db, srcColls, dstColls))
            if not configure[CONTINUE]:
                return False
        else:
            log_info("EQUL => database [%s] collections count equals = %d " % (db, len(srcColls)))

        for coll in srcColls:
            compare_coll_status = 0
            if coll in configure[EXCLUDE_COLLS]:
                log_info("IGNR => ignore collection [%s]" % coll)
                continue

            if coll not in dstColls:
                if not configure['compare_result'].get('dst_not_exists_coll'):
                    configure['compare_result']['dst_not_exists_coll'] = []
                configure['compare_result']['dst_not_exists_coll'].append(
                    'XXXX [%s.%s],collection only in source,doc count:%s' % (db, coll, srcDb[coll].count()))
                log_error("DIFF => collection only in source [%s]" % (coll))
                if not configure[CONTINUE]:
                    return False

            srcColl = srcDb[coll]
            dstColl = dstDb[coll]
            # comparison collection records number
            if srcColl.count() != dstColl.count():
                log_error("DIFF => collection [%s.%s] record count not equals , src[%d] -> dst[%d]" % (db, coll, srcColl.count(), dstColl.count()))
                if not configure[CONTINUE]:
                    return False
            else:
                log_info("EQUL => collection [%s.%s] record count equals , src:%d = dst:%d" % (db, coll, srcColl.count(), dstColl.count()))
                compare_coll_status += 1
            # comparison collection index number
            src_index_length = len(srcColl.index_information())
            dst_index_length = len(dstColl.index_information())
            if src_index_length != dst_index_length:
                log_error("DIFF => collection [%s.%s] index number not equals: src[%r], dst[%r]" % (db, coll, src_index_length, dst_index_length))
                if not configure[CONTINUE]:
                    return False
            else:
                log_info("EQUL => collection [%s.%s] index number equals = %d" % (db, coll, src_index_length))
                compare_coll_status += 0.5

            # check sample data
            qtname = db + '.' + coll
            if configure[THREADS] <= 1:
                status, check_rowcount, cost_time = data_comparison(srcColl, dstColl, configure[COMPARISION_MODE], configure[LATEST_SIZE],
                                                                    configure[FULL_CHECK_SIZE],
                                                                    configure[CHECK_PERC], qtname)
                if not status:
                    log_error("DIFF => collection [%s.%s] data comparison not equals, check row count: %s, cost time(s): %s, src rowcount:%s" % (
                        db, coll, check_rowcount, cost_time, srcColl.count()))
                    if not configure[CONTINUE]:
                        return False
                else:
                    log_info("EQUL => collection [%s.%s] data data comparison exactly eauals, check row count: %s, cost time(s): %s, src rowcount:%s" % (
                        db, coll, check_rowcount, cost_time, srcColl.count()))
                    compare_coll_status += 2

                add_info = "datacompare info: {}/{} check_percentage:{}%, cost time:{}s".format(check_rowcount, srcColl.count(),
                                                                                                round((
                                                                                                          check_rowcount / srcColl.count() if srcColl.count() != 0 else 1) * 100,
                                                                                                      3), cost_time)
                if compare_coll_status > 3:
                    # compare ok
                    configure['compare_result'][qtname] = '=== [%s.%s],record=%r,index=%r,datacompare=%s (%s)' % (
                        db, coll, srcColl.count(), src_index_length, 'ok', add_info)
                else:
                    configure['compare_result'][qtname] = 'XXX [%s.%s],src_dst_record=[%d:%d],src_dst_index=[%d:%d],datacompare=%s (%s)' % (
                        db, coll, srcColl.count(), dstColl.count(), src_index_length, dst_index_length, 'err', add_info)
            else:
                q.put(0)
                qt = QThreads(q, data_comparison,
                              **{"srcColl": srcColl, "dstColl": dstColl, "mode": configure[COMPARISION_MODE], "check_latest_size": configure[LATEST_SIZE],
                                 "full_check_size": configure[FULL_CHECK_SIZE], "check_perc": configure[CHECK_PERC], "qtname": qtname})
                qt.start()
                if compare_coll_status < 1:
                    configure['compare_result'][qtname] = 'XXX [%s.%s],src_dst_record=[%d:%d],src_dst_index=[%d:%d]' % (
                        db, coll, srcColl.count(), dstColl.count(), src_index_length, dst_index_length)
                else:
                    configure['compare_result'][qtname] = '=== [%s.%s],record=%r,index=%r' % (db, coll, srcColl.count(), src_index_length)

    return True


def data_comparison(srcColl, dstColl, mode, check_latest_size=0, full_check_size=1000, check_perc=0, qtname='nodb.nocoll'):
    '''
    源端和目标端的数据对比
    :param srcColl: 源端collection实例
    :param dstColl: 目标端collection实例
    :param mode: 对比模式
    :param check_latest_size:对比最新的多少条行数
    :param full_check_size: 对比数据的最小行数，小于则全量对比
    :param check_perc: 对比数据的百分比
    :param qtname: 数据库.集合的名字
    :return: bool 对比是否成功
    '''
    if mode == "no":
        configure['compare_info'][qtname] = (True, srcColl.count(), 0, 0)
        return True, 0, 0
    elif mode == "sample":
        # srcColl.count() mus::t equals to dstColl.count()
        count = configure[COMPARISION_COUNT] if configure[COMPARISION_COUNT] <= srcColl.count() else srcColl.count()
        if 0 < check_perc <= 100:
            count = int(srcColl.count() * (check_perc / 100))
    else:  # all
        count = srcColl.count()

    if count == 0:
        configure['compare_info'][qtname] = (True, srcColl.count(), 0, 0)
        return True, 0, 0

    global SRC_VERSION
    rec_count = count
    batch = configure[BATCH_SIZE]
    # 经测试，每批次对比在20-50之间效果好，这里进行了限制
    if configure[BATCH_SIZE] > 50:
        configure[BATCH_SIZE] = 50
    if configure[BATCH_SIZE] < 20:
        configure[BATCH_SIZE] = 20
    large_skip_max = 10000
    show_progress = (batch * 25)
    total = 0
    # batch size
    size = int(count / batch)
    # step factor
    step_factor = int(srcColl.count() / count)
    skip_step_rows = batch * step_factor
    if skip_step_rows > large_skip_max:
        # skip factor , improve skip efficiency
        size = int(srcColl.count() / large_skip_max)
        # batch factor
        batch = int(math.ceil(count / size)) + 1
        skip_step_rows = large_skip_max
    log_info("[%s],total:%s,size:%s,step_factor:%s,skip_step_rows:%s,batch:%s" % (qtname, total, size, step_factor, skip_step_rows, batch))

    start = int(time.time())
    # 对比最新的n行数据
    if check_latest_size > 0:
        docs = srcColl.find({}).sort([("_id", -1)]).limit(check_latest_size)
        checked_row_count = 0
        for doc in docs:
            migrated = dstColl.find_one(doc["_id"])
            # both origin and migrated bson is Map . so use ==
            checked_row_count += 1
            if doc != migrated:
                log_error("DIFF => [%s] src_record[%s], dst_record[%s]" % (qtname, doc, migrated))
                configure['compare_info'][qtname] = (False, srcColl.count(), checked_row_count, int(time.time()) - start)
                return False, checked_row_count, int(time.time()) - start

    # 集合没有超过对比行数的最小值则全量对比
    if count < full_check_size:
        docs = srcColl.find({})
        checked_row_count = 0
        for doc in docs:
            migrated = dstColl.find_one(doc["_id"])
            checked_row_count += 1
            # both origin and migrated bson is Map . so use ==
            if doc != migrated:
                log_error("DIFF => [%s] src_record[%s], dst_record[%s]" % (qtname, doc, migrated))
                configure['compare_info'][qtname] = (False, srcColl.count(), checked_row_count, int(time.time()) - start)
                return False, checked_row_count, int(time.time()) - start
        configure['compare_info'][qtname] = (True, total, checked_row_count, int(time.time()) - start)
        return True, total, int(time.time()) - start

    while count > 0:
        # 高版本对比，原版的对比方法
        if SRC_VERSION >= configure[SS]:
            docs = srcColl.aggregate([{"$sample": {"size": batch}}])
            checked_row_count = 0
            while docs.alive:
                doc = docs.next()
                migrated = dstColl.find_one(doc["_id"])
                checked_row_count += 1
                # both origin and migrated bson is Map . so use ==
                if doc != migrated:
                    log_error("DIFF => [%s] src_record[%s], dst_record[%s]" % (qtname, doc, migrated))
                    configure['compare_info'][qtname] = (False, srcColl.count(), total + checked_row_count, int(time.time()) - start)
                    return False, total + checked_row_count, int(time.time()) - start
            total += batch
            count -= batch

            if total % show_progress == 0:
                log_info(" [%s] ... process %d docs, %.2f %% !" % (qtname, total, total * 100.0 / rec_count))
        else:
            # 低版本的对比方法 主要改进的地方
            last_id = 0
            checked_row_count = 0
            is_over_num = 0
            for j in range(size):
                condition = {} if last_id == 0 else {"_id": {"$gte": last_id}}
                # docs = srcColl.find(condition).sort([("_id", 1)]).skip(j if j == 0 else skip_step_rows).limit(batch)
                docs = srcColl.find(condition).skip(j if j == 0 else skip_step_rows).limit(batch)
                for doc in docs:
                    is_over_num = j
                    migrated = dstColl.find_one(doc["_id"])
                    checked_row_count += 1
                    # both origin and migrated bson is Map . so use ==
                    if doc != migrated:
                        log_error("DIFF => [%s] src_record[%s], dst_record[%s]" % (qtname, doc, migrated))
                        configure['compare_info'][qtname] = (False, srcColl.count(), checked_row_count, int(time.time()) - start)
                        return False, checked_row_count, int(time.time()) - start
                    last_id = doc['_id']
                if is_over_num != j:
                    count = -1
                    break
                total += batch
                count -= batch
                if total % show_progress == 0:
                    log_info(" [%s] ... process %d docs, %.2f %% !" % (qtname, total, total * 100.0 / rec_count))
    configure['compare_info'][qtname] = (True, srcColl.count(), total, int(time.time()) - start)
    return True, total, int(time.time()) - start


def usage():
    print()
    print()
    print(
        """Usage: 
./compare_mongodb_data.py   \\
--src=mongodb://root:pwd@127.0.0.1:22001,127.0.0.1:22002/admin?replicaSet=repl1&readPreference=secondaryPreferred  \\
--dest=mongodb://root:pwd@127.0.0.1:22003,127.0.0.1:22004/admin?replicaSet=repl2  \\
--count=10000 | --check-perc=2 (要对比数据的总行数或者总行数的百分比，[0.001-100]) ( 确定对比的总行数参数的优先级顺序为 : --comparisonMode=all,--check_perc --count)  \\
--include-dbs='db1,db2,db3'   | --exclude-dbs='db4,db5'   (要对比的数据库 或者 要排除对比的数据库) \\  
--continue=True  (遇到错误是否继续)\\
--latest-size=0 (检查最新的n行数据，默认为0) \\
--full-less-than=1000 (抽样对比数据的最小行数，低于此值则全量对比)  \\
--threads=1  (指定要对比数据使用的线程数，默认单线程)\\
--batch-size=20 (每批对比多少行数据，默认30 范围20-50)  \\
--sample-version=40 (指定mongodb版本为多少时使用sample函数，默认4.0.x使用collection.aggregate([{"$sample": {"size": xx}}])函数取数据对比，则这里要填写40，低于此版本用find().limit()取函数对比 建议调大此参数)   \\
--comparison-mode=sample/all/no (sample: 采样检查 默认, all: 全量对比; no: 仅对比总行数和索引量)""")
    print()


def parse_args():
    opts, args = getopt.getopt(sys.argv[1:], "hs:d:n:e:x:i:c:ls:fl:cp:t:b:ss:",
                               ["help", "src=", "dest=", "count=", "exclude-dbs=", "exclude-collections=", "comparison-mode=", 'include-dbs=', 'continue=',
                                'latest-size=', 'full-less-than=', 'check-perc=', 'threads=', "batch-size=", 'sample-version='])

    configure[SAMPLE] = True
    configure[EXCLUDE_DBS] = []
    configure[EXCLUDE_COLLS] = []
    configure[INCLUDE_DBS] = []
    configure[CONTINUE] = False
    configure[LATEST_SIZE], configure[FULL_CHECK_SIZE], configure[CHECK_PERC] = 0, 1000, 0
    configure[THREADS] = 1
    configure[BATCH_SIZE] = 30
    configure[SS] = 40
    srcUrl, dstUrl = "", ""

    for key, value in opts:
        if key in ("-h", "--help"):
            usage()
        if key in ("-s", "--src"):
            srcUrl = value
        if key in ("-d", "--dest"):
            dstUrl = value
        if key in ("-n", "--count"):
            configure[COMPARISION_COUNT] = int(value)
        if key in ("-e", "--exclude-dbs"):
            configure[EXCLUDE_DBS] = value.split(",")
        if key in ("-x", "--exclude-collections"):
            configure[EXCLUDE_COLLS] = value.split(",")
        if key in ('-i', '--include-dbs'):
            configure[INCLUDE_DBS] = value.split(",")
        if key in ('-c', '--continue'):
            configure[CONTINUE] = 1 if str(value).lower() == 'true' else 0
        if key in ("-ls", "--latest-size"):
            configure[LATEST_SIZE] = int(value)
        if key in ("-fl", "--full-less-than"):
            configure[FULL_CHECK_SIZE] = int(value)
        if key in ("-cp", "--check-perc"):
            configure[CHECK_PERC] = float(value)
        if key in ("-t", '--threads'):
            configure[THREADS] = int(value)
        if key in ("-b", '--batch-size'):
            configure[BATCH_SIZE] = int(value)
        if key in ("-ss", '--sample-version'):
            configure[SS] = int(value)
        if key in ("--comparison-mode"):
            if value != "all" and value != "no" and value != "sample":
                log_info("comparisonMode[%r] illegal" % (value))
                sys.exit()
            configure[COMPARISION_MODE] = value
    if COMPARISION_MODE not in configure:
        configure[COMPARISION_MODE] = "sample"

    # params verify
    if len(srcUrl) == 0 or len(dstUrl) == 0:
        usage()
        sys.exit()

    # default count is 10000
    if configure.get(COMPARISION_COUNT) is None or configure.get(COMPARISION_COUNT) <= 0:
        configure[COMPARISION_COUNT] = 10000

    # ignore databases
    configure[EXCLUDE_DBS] += ["admin", "local", "test"]
    configure[EXCLUDE_COLLS] += ["system.profile"]

    # dump configuration
    log_info(
        "Configuration [sample=%s, count=%d, exclude-dbs=%s, exclude-colls=%s, include-dbs=%s,continue={}, latest_size=%d, full_check_less_than=%d, check_perc=%d ,threads=%s, batchsize=%d, sample-skip=%d]".format(
            configure[CONTINUE]) % (
            configure[SAMPLE], configure[COMPARISION_COUNT], configure[EXCLUDE_DBS], configure[EXCLUDE_COLLS], configure[INCLUDE_DBS],
            configure[LATEST_SIZE], configure[FULL_CHECK_SIZE], configure[CHECK_PERC], configure[THREADS], configure[BATCH_SIZE], configure[SS]))
    return srcUrl, dstUrl


def start_compare(sc, dc, q):
    '''
    对比数据
    :param sc: 源端连接实例
    :param dc: 目标端连接实例
    :param q: 队列
    :return:
    '''

    # check data
    check(sc, dc)

    # multi threads
    if configure[THREADS] > 1:
        while True:
            if q.empty(): break
        for i in configure['compare_result']:
            if i in configure['compare_info']:
                com_info = configure['compare_info'].get(i)
                tmp = configure['compare_result'][i].replace('====', '====' if com_info[0] else 'XXX')
                add_info = "Data comparison results: {}/{}, Percentage of inspection:{}%, Time consuming:{}s".format(com_info[2], com_info[1],
                                                                                                                     round(
                                                                                                                         (com_info[2] / com_info[1] if com_info[
                                                                                                                                                           1] != 0 else 1) * 100,
                                                                                                                         3), com_info[3])
                configure['compare_result'][i] = tmp + ",datacompare={} ({})".format('ok' if com_info[0] else 'err', add_info)

    log_info('summary info\n----------------------------------------')
    for i in configure['compare_result']:
        if i not in ['dst_not_exists_db', 'dst_not_exists_coll']:
            print(configure['compare_result'][i])
    for i in configure['compare_result'].get('dst_not_exists_db') or []:
        print(i)
    for i in configure['compare_result'].get('dst_not_exists_coll') or []:
        print(i)


if __name__ == "__main__":
    srcUrl, dstUrl = parse_args()
    q = queue.Queue(configure[THREADS])
    src, dst = MongoCluster(srcUrl), MongoCluster(dstUrl)
    print("[src = %s]" % srcUrl)
    print("[dst = %s]" % dstUrl)
    sc = src.connect()
    dc = dst.connect()

    start_compare(sc, dc, q)

    src.close()
    dst.close()
