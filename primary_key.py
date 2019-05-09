# -*- coding: utf-8 -*-


import multiprocessing as mp
import os
import sys
import time
from math import floor

import MySQLdb
import cx_Oracle  # 引用模块cx_Oracle
import numpy as np
import xlwt


class Logger(object):
    def __init__(self, filename="Default.log"):
        self.terminal = sys.stdout
        self.log = open(filename, "a")

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)

    def flush(self):
        pass


os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'


def screen_table(t_name_list):
    print(time.asctime(time.localtime(time.time())), '[process ', os.getpid(), '] screen_table starts')
    conn = cx_Oracle.connect('c##LKX/0000@219.216.69.63:1521/orcl')  # 连接数据库
    cur = conn.cursor()
    res = [name for name in t_name_list if cur.execute("select count(*) from C##SCYW." + name).fetchone()[0] > 0]
    cur.close()  # 关闭cursor
    conn.close()  # 关闭连接
    print(time.asctime(time.localtime(time.time())), '[process ', os.getpid(), '] screen_table is over,deletes ', len(t_name_list) - len(res),
          'sheets')
    return res


def cut(l, n):
    length = int(floor(len(l) / n) + 1)
    list = [l[i:i + length] for i in range(0, len(l), length)]
    return list


# 判断候选码函数 传入表名列表 将结果存入mysql
def Candidate_Key(part_t_names):
    print(time.asctime(time.localtime(time.time())), '[process ', os.getpid(), '] Candidate_Key starts,', len(part_t_names), 'sheets')
    global module_pre
    table_list = {}
    oracle_conn = cx_Oracle.connect('c##LKX/0000@219.216.69.63:1521/orcl')  # 连接数据库
    oracle_cur = oracle_conn.cursor()
    mysql_conn = MySQLdb.connect("localhost", "root", "0000", "pms", charset='utf8')
    mysql_cur = mysql_conn.cursor()
    # table_list = {
    #     table:{
    #         'cols':{
    #              'c1':列类型，
    #         }，
    #         'primary_key':[主键列表]
    #     }
    # }
    # 获取表自详细信息结构 存入table_list
    if module_pre == 'all':
        oracle_cur.execute(
                "SELECT TABLE_NAME,COLUMN_NAME,DATA_TYPE from  all_tab_cols where  OWNER='C##SCYW'")  # 用cursor进行各种操作
    else:
        oracle_cur.execute(
                "SELECT TABLE_NAME,COLUMN_NAME,DATA_TYPE from  all_tab_cols where TABLE_NAME like '" + module_pre + "%' and OWNER='C##SCYW'")
    results = oracle_cur.fetchall()
    for result in results:
        if result[0] in part_t_names:
            if result[0] in table_list:
                table_list[result[0]]['cols'][result[1]] = result[2]
            else:
                table_list[result[0]] = {}
                table_list[result[0]]['cols'] = {}
                table_list[result[0]]['primary_key'] = []
                table_list[result[0]]['cols'][result[1]] = result[2]
    # 根据table_list来判断主键
    # 获取已定义主键信息
    if module_pre == 'all':
        oracle_cur.execute(
                "select CC.COLUMN_NAME,C.TABLE_NAME from all_constraints c,ALL_CONS_COLUMNS cc where C.OWNER = CC.OWNER AND C.OWNER = 'C##SCYW' AND C.CONSTRAINT_TYPE = 'P' AND C.CONSTRAINT_NAME = CC.CONSTRAINT_NAME   AND C.TABLE_NAME = CC.TABLE_NAME")
    else:
        oracle_cur.execute(
                "select CC.COLUMN_NAME,C.TABLE_NAME from all_constraints c,ALL_CONS_COLUMNS cc where C.OWNER = CC.OWNER AND C.OWNER = 'C##SCYW'   AND C.TABLE_NAME like '" + module_pre + "%'   AND C.CONSTRAINT_TYPE = 'P'   AND C.CONSTRAINT_NAME = CC.CONSTRAINT_NAME   AND C.TABLE_NAME = CC.TABLE_NAME")
    for result in oracle_cur:
        if result[1] in table_list:
            mysql_cur.execute(
                "INSERT INTO condidate_key(t_name,col_name,type,p_or_c)VALUES ('" + result[1] + "', '" + result[0] + "', '" + table_list[result[1]]['cols'][result[0]] + "','p')")
            mysql_conn.commit()
            if 'primary_key' in table_list[result[1]]:
                table_list[result[1]]['primary_key'].append(result[0])
            else:
                table_list[result[1]]['primary_key'] = []
                table_list[result[1]]['primary_key'].append(result[0])
    # 对于没有主键的表进行主键判断
    for table in table_list:
        if len(table_list[table]['primary_key']) == 0:
            for col_name in list(table_list[table]['cols']):
                oracle_cur.execute(
                        "SELECT " + col_name + ",  COUNT(" + col_name + ")FROM c##SCYW." + table + " GROUP BY " + col_name + " HAVING  COUNT(" + col_name + ") > 1")
                duplicate_count = len(oracle_cur.fetchall())
                if (duplicate_count == 0):
                    oracle_cur.execute(
                        "SELECT COUNT(" + col_name + ")FROM c##SCYW." + table + " where " + col_name + " is not null")
                    if (oracle_cur.fetchone()[0] > 0):
                        if table_list[table]['cols'][col_name] != 'NUMBER':
                            mysql_cur.execute(
                                "INSERT INTO condidate_key(t_name,col_name,type,p_or_c)VALUES ('" + table + "', '" + col_name + "', '" +
                                table_list[table]['cols'][col_name] + "','c')")
                            mysql_conn.commit()
                            table_list[table]['primary_key'].append(col_name)
                        else:
                            oracle_cur.execute(
                                    "select DATA_SCALE from all_tab_cols WHERE TABLE_NAME='" + table + "' and COLUMN_NAME = '" + col_name + "' and OWNER = 'C##SCYW'")
                            # print(time.asctime(time.localtime(time.time())),'scale:', cur.fetchone()[0])
                            if (oracle_cur.fetchone()[0] == 0):
                                mysql_cur.execute(
                                    "INSERT INTO condidate_key(t_name,col_name,type,p_or_c)VALUES ('" + table + "', '" + col_name + "', '" +
                                    table_list[table]['cols'][col_name] + "','c')")
                                mysql_conn.commit()
                                table_list[table]['primary_key'].append(col_name)
    print(time.asctime(time.localtime(time.time())), '[process ', os.getpid(), '] Candidate_Key is over')
    return table_list


def Judging_PK():
    # 获取condidate_key表中表列表t_list
    # 遍历t_list，对表中每一个候选码搜索索引个数和外键个数
    data = {}
    mysql_conn = MySQLdb.connect("localhost", "root", "0000", "pms", charset='utf8')
    mysql_cur = mysql_conn.cursor()
    mysql_cur.execute("select * from condidate_key where p_or_c = 'c'")
    results = mysql_cur.fetchall()
    for result in results:
        if result[1] in data:
            data[result[1]].append(result[2])
        else:
            data[result[1]] = []
            data[result[1]].append(result[2])
    for table in data:
        col_list = data[table]
        index_num_list = []
        FK_num_list = []
        for i in range(len(col_list)):
            mysql_cur.execute("select count(*) from index_table where t_name ='"+table+"' and col_name='"+col_list[i]+"'")
            index_table_num = mysql_cur.fetchone()[0]
            mysql_cur.execute("select count(*) from foreign_key where Parent_Table ='"+table+"' and Primary_Key='"+col_list[i]+"' OR Child_Table ='"+table+"' and Foreign_Key='"+col_list[i]+"'")
            FK_num = mysql_cur.fetchone()[0]
            index_num_list.append(index_table_num)
            FK_num_list.append(FK_num)
        score_list = [0 for i in range(len(col_list))]

        for i in range(len(col_list)):
            try:
                score_list[i] = index_num_list[i]/sum(index_num_list) +FK_num_list[i]/sum(FK_num_list)
            except:
                score_list[i] = 0
            else:
                score_list[i] = index_num_list[i]/sum(index_num_list) +FK_num_list[i]/sum(FK_num_list)

            # if sum(index_num_list)==0:
            #     score_list.append(0)
            # else:
            #     score_list.append(index_num_list[i]/sum(index_num_list))
            #
            # if sum(FK_num_list) == 0:
            #     score_list[i] += 0
            # else:
            #     score_list[i] += FK_num_list[i]/sum(FK_num_list)
        print(index_num_list,FK_num_list)
        print(table,':',score_list,'max:',score_list.index(max(score_list)))
        if len(set(score_list))==1:
            mysql_cur.execute("update condidate_key set p_or_c = 'The score is the same' where t_name='"+table+"'")
        else:
            print(int(max(score_list)))
            mysql_cur.execute("update condidate_key set p_or_c = 'c_p' where t_name='"+table+"' and col_name='"+col_list[score_list.index(max(score_list))]+"'")

        mysql_conn.commit()
    return





module_pre = 'all'
sys.stdout = Logger("log.txt")
if __name__ == '__main__':
    conn = cx_Oracle.connect('c##LKX/0000@219.216.69.63:1521/orcl')  # 连接数据库
    cur = conn.cursor()
    num_core = 4  # 核心数
    table_dict = {}  # 表结构字典

    # table_dict = {
    #     table_name:{
    #         'cols':{
    #              'c1':列类型，
    #         }，
    #         'primary_key':[主键列表]
    #     }
    # }
    # 获取表名列表并去除空表
    # st = time.time()
    # if module_pre == 'all':
    #     cur.execute(
    #             "SELECT TABLE_NAME from all_tables where  OWNER='C##SCYW'")
    # else:
    #     cur.execute(
    #             "SELECT TABLE_NAME from all_tables where TABLE_NAME like '" + module_pre + "%' and OWNER='C##SCYW'")
    # res = list(np.array(cur.fetchall())[:, 0])
    # pool = mp.Pool(processes=num_core)
    # t_name_list = [pool.apply_async(screen_table, [i]) for i in cut(res, num_core)]
    # et = time.time()
    # pool.close()
    # pool.join()
    # t_names = []
    # for i in t_name_list:
    #     t_names += i.get()
    # print(time.asctime(time.localtime(time.time())), '[main] Filtered empty sheet:', et - st)
    # # 根据t_names,判断主键,将T_names分割后分配给每个process
    # print(time.asctime(time.localtime(time.time())), '[main] Candidate_Key starts')
    # st = time.time()
    # pool = mp.Pool(processes=num_core)
    # PK_multi_res = [pool.apply_async(Candidate_Key, [part_t_names]) for part_t_names in cut(t_names, num_core)]
    # pool.close()
    # pool.join()
    # # 结果合并,填充table_dict
    # for res in PK_multi_res:
    #     table_dict.update(res.get())
    # et = time.time()
    # print(time.asctime(time.localtime(time.time())), '[main] Candidate_Key is over:', et - st)
    # print(time.asctime(time.localtime(time.time())), table_dict)

    print(time.asctime(time.localtime(time.time())), '[main] Judging_PK starts')
    st = time.time()
    Judging_PK()

    # print(time.asctime(time.localtime(time.time())), '[main] Judging_PK is over:', et - st)



    # print(time.asctime(time.localtime(time.time())),Judging_FK_2(table_list,value_list,cur))
    # cur.close()  # 关闭cursor
    # conn.close()  # 关闭连接




# pms('T_CMS')
# pms2('T_PWGC')
