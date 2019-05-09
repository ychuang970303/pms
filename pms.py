# -*- coding: utf-8 -*-


import multiprocessing as mp
import os
import time

import cx_Oracle  # 引用模块cx_Oracle
import numpy as np
import xlwt
import sys
from math import floor

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
    print(time.asctime(time.localtime(time.time())),'[process',os.getpid(),'] screen_table starts')
    conn = cx_Oracle.connect('c##LKX/0000@219.216.69.63:1521/orcl')  # 连接数据库
    cur = conn.cursor()
    res = [name for name in t_name_list if cur.execute("select count(*) from C##SCYW." + name).fetchone()[0] > 0]
    cur.close()  # 关闭cursor
    conn.close()  # 关闭连接
    print(time.asctime(time.localtime(time.time())),'[process',os.getpid(),'] screen_table is over,deletes ', len(t_name_list) - len(res), 'sheets')
    return res


def cut(l, n):
    length = int(floor(len(l)/n) + 1)
    list = [l[i:i + length] for i in range(0, len(l), length)]
    return list


# 判断主键函数 传入表名列表 输出table_list
def FIND_PK(part_t_names):
    print(time.asctime(time.localtime(time.time())),'[process',os.getpid(),']FIND_PK starts,', len(part_t_names),'sheets')
    global module_pre
    table_list = {}
    conn = cx_Oracle.connect('c##LKX/0000@219.216.69.63:1521/orcl')  # 连接数据库
    cur = conn.cursor()
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
        cur.execute(
                "SELECT TABLE_NAME,COLUMN_NAME,DATA_TYPE from  all_tab_cols where  OWNER='C##SCYW'")  # 用cursor进行各种操作
    else:
        cur.execute(
                "SELECT TABLE_NAME,COLUMN_NAME,DATA_TYPE from  all_tab_cols where TABLE_NAME like '" + module_pre + "%' and OWNER='C##SCYW'")
    results = cur.fetchall()
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
        cur.execute(
                "select CC.COLUMN_NAME,C.TABLE_NAME from all_constraints c,ALL_CONS_COLUMNS cc where C.OWNER = CC.OWNER AND C.OWNER = 'C##SCYW' AND C.CONSTRAINT_TYPE = 'P' AND C.CONSTRAINT_NAME = CC.CONSTRAINT_NAME   AND C.TABLE_NAME = CC.TABLE_NAME")
    else:
        cur.execute(
                "select CC.COLUMN_NAME,C.TABLE_NAME from all_constraints c,ALL_CONS_COLUMNS cc where C.OWNER = CC.OWNER AND C.OWNER = 'C##SCYW'   AND C.TABLE_NAME like '" + module_pre + "%'   AND C.CONSTRAINT_TYPE = 'P'   AND C.CONSTRAINT_NAME = CC.CONSTRAINT_NAME   AND C.TABLE_NAME = CC.TABLE_NAME")
    for result in cur:
        if result[1] in table_list:
            if 'primary_key' in table_list[result[1]]:
                table_list[result[1]]['primary_key'].append(result[0])
            else:
                table_list[result[1]]['primary_key'] = []
                table_list[result[1]]['primary_key'].append(result[0])
    # 对于没有主键的表进行主键判断
    for table in table_list:
        if len(table_list[table]['primary_key']) == 0:
            for col_name in list(table_list[table]['cols']):
                cur.execute(
                        "SELECT " + col_name + ",  COUNT(" + col_name + ")FROM c##SCYW." + table + " GROUP BY " + col_name + " HAVING  COUNT(" + col_name + ") > 1")
                duplicate_count = len(cur.fetchall())
                if (duplicate_count == 0):
                    cur.execute("SELECT COUNT("+col_name+")FROM c##SCYW."+table+" where "+col_name+" is not null")
                    if(cur.fetchone()[0] > 0):
                        if table_list[table]['cols'][col_name] != 'NUMBER':
                            table_list[table]['primary_key'].append(col_name)
                        else:
                            cur.execute(
                                    "select DATA_SCALE from all_tab_cols WHERE TABLE_NAME='" + table + "' and COLUMN_NAME = '" + col_name + "' and OWNER = 'C##SCYW'")
                            # print(time.asctime(time.localtime(time.time())),'scale:', cur.fetchone()[0])
                            if (cur.fetchone()[0] == 0):
                                table_list[table]['primary_key'].append(col_name)
    print(time.asctime(time.localtime(time.time())),'[process',os.getpid(),'] FIND_PK is over')
    return table_list


def Judging_FK_2(p_t_names, table_dict):
    print(time.asctime(time.localtime(time.time())),'[process',os.getpid(),'] Judging_FK starts,', len(p_t_names), 'sheets')
    oracle_conn = cx_Oracle.connect('c##LKX/0000@219.216.69.63:1521/orcl')
    cur = oracle_conn.cursor()
    FK_dic = []
    # 开始判断
    for T1 in p_t_names:  # 对每一个表 a
        for C1 in table_dict[T1]['primary_key']:  # 对表a的每一个候选码c1
            for T2 in table_dict:  # 对每一个表b

                if T2 != T1:  # 如果b与a为不同表则继续
                    tmp_count = 0
                    for C2 in table_dict[T2]['cols']:  # 对b的每一列c2
                        print(time.asctime(time.localtime(time.time())),'[process',os.getpid(),'] Judging_FK: ',T1,':',C1,'-',T2,':',C2)
                        if table_dict[T2]['cols'][C2] == table_dict[T1]['cols'][C1]:  # 如果c1和c2类型相同则继续
                            cur.execute(
                                    "select count(DISTINCT t2." + C2 + ") from C##SCYW." + T1 + " t1, C##SCYW." + T2 + " t2 where t1." + C1 + " = t2." + C2 + " and t2." + C2 + " is not null")
                            INTER_COUNTER = cur.fetchone()[0]
                            cur.execute(
                                    "select count(DISTINCT t." + C1 + ") from C##SCYW." + T1 + " t where t." + C1 + " is not null")
                            C1_CONUTER = cur.fetchone()[0]
                            cur.execute(
                                    "select count(DISTINCT t." + C2 + ") from C##SCYW." + T2 + " t where t." + C2 + " is not null")
                            C2_CONUTER = cur.fetchone()[0]
                            if C2_CONUTER >= 100 and INTER_COUNTER / C2_CONUTER >= 0.9 and tmp_count <= len(
                                    table_dict[T1]['primary_key']):
                                tmp_count += 1
                                tmp = {
                                    'T1_NAME': T1,
                                    'T2_NAME': T2,
                                    'C1_NAME': C1,
                                    'C2_NAME': C2,
                                    'type': table_dict[T1]['cols'][C1],
                                    'C1_COUNT': C1_CONUTER,
                                    'INTER_COUNT': INTER_COUNTER,
                                    'C2_CONUT': C2_CONUTER,
                                    'RATE': INTER_COUNTER / C2_CONUTER,
                                }
                                FK_dic.append(tmp)
            table_dict[T1]['cols'].pop(C1)
    print(time.asctime(time.localtime(time.time())),'[process',os.getpid(),'] Judging_FK is over,FK_COUNT:', len(FK_dic))
    return FK_dic


module_pre = 'T_CMS'
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
    st = time.time()
    if module_pre == 'all':
        cur.execute(
                "SELECT TABLE_NAME from all_tables where  OWNER='C##SCYW'")
    else:
        cur.execute(
                "SELECT TABLE_NAME from all_tables where TABLE_NAME like '" + module_pre + "%' and OWNER='C##SCYW'")
    res = list(np.array(cur.fetchall())[:, 0])
    pool = mp.Pool(processes=num_core)
    t_name_list = [pool.apply_async(screen_table, [i]) for i in cut(res, num_core)]
    et = time.time()
    pool.close()
    pool.join()
    t_names = []
    for i in t_name_list:
        t_names += i.get()
    print(time.asctime(time.localtime(time.time())),'[main] Filtered empty sheet:', et - st)
    # 根据t_names,判断主键,将T_names分割后分配给每个process
    print(time.asctime(time.localtime(time.time())),'[main] FIND_PK starts')
    st = time.time()
    pool = mp.Pool(processes=num_core)
    PK_multi_res = [pool.apply_async(FIND_PK, [part_t_names]) for part_t_names in cut(t_names, num_core)]
    pool.close()
    pool.join()
    # 结果合并,填充table_dict
    for res in PK_multi_res:
        table_dict.update(res.get())
    et = time.time()
    print(time.asctime(time.localtime(time.time())),'[main] FIND_PK is over:', et - st)

    print(time.asctime(time.localtime(time.time())),'[main] Judging_FK starts')
    st = time.time()
    pool = mp.Pool(processes=num_core)
    FK_multi_res = [pool.apply_async(Judging_FK_2, [p_t_names, table_dict]) for p_t_names in cut(t_names, 4)]

    pool.close()
    pool.join()
    # 结果合并,填充FK_dict
    FK_list = []
    for res in FK_multi_res:
        FK_list += res.get()
    et = time.time()
    print(time.asctime(time.localtime(time.time())),FK_list)
    print(time.asctime(time.localtime(time.time())),'[main] Judging_FK is over:', et - st)

    wb = xlwt.Workbook()
    ws = wb.add_sheet('sheet1', cell_overwrite_ok=True)
    wp = xlwt.Pattern()
    wp.pattern = xlwt.Pattern.SOLID_PATTERN
    ws.write(0, 0, 'Parent_Table')
    ws.write(0, 1, 'Child_Table')
    ws.write(0, 2, 'Primary_Key')
    ws.write(0, 3, 'Foreign_Key')
    ws.write(0, 4, 'Type')
    ws.write(0, 5, 'lenth_PK')
    ws.write(0, 6, 'lenth_intersection')
    ws.write(0, 7, 'lenth_FK')
    ws.write(0, 8, 'Consistency')
    ws.col(0).width = 8888
    ws.col(1).width = 8888
    ws.col(2).width = 4444
    ws.col(3).width = 3333
    ws.col(4).width = 4444
    ws.panes_frozen = True
    ws.horz_split_pos = 1
    FK = []
    for i in range(len(FK_list)):
        FK.append(tuple(FK_list[i].values()))
    for i in range(len(FK)):
        ws.write(i + 1, 0, FK[i][0])
        ws.write(i + 1, 1, FK[i][1])
        ws.write(i + 1, 2, FK[i][2])
        ws.write(i + 1, 3, FK[i][3])
        ws.write(i + 1, 4, FK[i][4])
        ws.write(i + 1, 5, FK[i][5])
        ws.write(i + 1, 6, FK[i][6])
        ws.write(i + 1, 7, FK[i][7])
        ws.write(i + 1, 8, FK[i][8])
    wb.save('./' + module_pre + '.xls')

    # print(time.asctime(time.localtime(time.time())),Judging_FK_2(table_list,value_list,cur))
    # cur.close()  # 关闭cursor
    # conn.close()  # 关闭连接




# pms('T_CMS')
# pms2('T_PWGC')
