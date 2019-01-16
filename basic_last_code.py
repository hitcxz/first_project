#_*_ coding:utf8 _*_

from optparse import OptionParser
import pandas as pd
import numpy as np
import datetime
import MySQLdb
import time
import sys
import datetime
reload(sys)
sys.setdefaultencoding('utf-8')


#from sys import argv

from pandas.io import sql

os_p='android'
N=10
date=20150101
data_size=1
table_name='myxj_basic_analysis'
def print_time(start_time,end_time):
    print 'time consuming(minute):{}'.format(int((end_time-start_time)/60))
    print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
    sys.stdout.flush()

def drop_table():
    db = MySQLdb.connect(host="192.168.147.98", user="hello", passwd="hellomtlab", db="bi_data", port=3306)
    cursor = db.cursor()
    drop_sql = "DROP TABLE IF EXISTS {}".format(table_name)
    cursor.execute(drop_sql)

def create_table():
    db = MySQLdb.connect(host="192.168.147.98", user="hello", passwd="hellomtlab", db="bi_data", port=3306)
    cursor = db.cursor()
    #drop_sql = "DROP TABLE IF EXISTS {}".format(table_name)
    #cursor.execute(drop_sql)
    create_sql = "CREATE TABLE IF NOT EXISTS `{}` (`Date` BIGINT DEFAULT NULL,`os_p` VARCHAR(50) DEFAULT NULL,`base_feat` VARCHAR(50) DEFAULT NULL,`base_value` VARCHAR(200) DEFAULT NULL,`base_cnt` FLOAT DEFAULT NULL,`base_per` FLOAT DEFAULT NULL,`ana_feat` VARCHAR(100) DEFAULT NULL,`ana_value` VARCHAR(200) DEFAULT NULL,`ana_cnt` FLOAT DEFAULT NULL,`ana_per` FLOAT DEFAULT NULL,`ana_avg` FLOAT DEFAULT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8".format(table_name)
    cursor.execute(create_sql)

#写入表
def write_data(data):
    print len(data)
    print "writing data"
    sys.stdout.flush()
    start_time=time.time()
    db = MySQLdb.connect(host="192.168.147.98", user="hello", passwd="hellomtlab", db="bi_data", port=3306)
    cursor = db.cursor()
    for index,line in data.iterrows():
        insert_sql = "INSERT INTO {} (Date,os_p,base_feat,base_value,base_cnt,base_per,ana_feat,ana_value,ana_cnt,ana_per,ana_avg) VALUES ('%d','%s','%s','%s','%f','%f','%s','%s','%f','%f','%f');".format(table_name)%(date,os_p,line['base_feat'],line['base_value'], line['base_cnt'],line['base_per'],line['ana_feat'], line['ana_value'],line['ana_cnt'],line['ana_per'],line['ana_avg'])
        cursor.execute(insert_sql)
        db.commit()
    end_time=time.time()
    print_time(start_time,end_time)


#基础属性的统计量
def dis_base_count(data,col_list_dis):
    result=pd.DataFrame(columns=['base_value','base_cnt','base_feat'])
    for s in col_list_dis:
        cou=data.groupby(s).size().reset_index(name='base_cnt')
        cou.columns=['base_value','base_cnt']
        cou['base_feat']=s
        result=pd.concat([result,cou],ignore_index=True)
    result.loc[:,'base_per']=result.loc[:,'base_cnt']/data_size
    return result

# 离散x离散
def deal_dis2(data,col_list_dis1,col_list_dis2):
    start_time=time.time()
    result=pd.DataFrame(columns=['base_value','ana_value','ana_cnt','base_feat','ana_feat'])#初始化
    #进行两两交叉
    for s in col_list_dis1:
        for t in col_list_dis2:
            if s<>t:
                result_tmp=data.groupby([s,t]).size().reset_index(name='user_cnt')
                result_tmp.columns=['base_value','ana_value','ana_cnt']
                result_tmp['base_feat']=s
                result_tmp['ana_feat']=t
                result=pd.concat([result,result_tmp],ignore_index=True)
    base_info=dis_base_count(data,col_list_dis1)
    #print base_info.dtypes
    #print result.dtypes
    result=result.merge(base_info)
    result.loc[:,'ana_per']=result.loc[:,'ana_cnt']/result.loc[:,'base_cnt']
    result['ana_avg']=0
    end_time=time.time()
    print_time(start_time,end_time)
    return result.loc[:,['base_feat','base_value','base_cnt','base_per','ana_feat','ana_value','ana_cnt','ana_per','ana_avg']]

# 离散x连续
def deal_dis_con(data,col_list_dis,col_list_con):
    start_time=time.time()
    result=pd.DataFrame(columns=['base_value','ana_value','ana_cnt','action_cnt','base_feat','ana_feat'])#初始化
    for s in col_list_dis:
        for t in col_list_con:
            result_tmp1=data.groupby([s]).agg({t:'sum'}).reset_index()
            result_tmp1.columns=['base_value','action_cnt']
            result_tmp2=data[data[t]>0].groupby([s]).size().reset_index(name='cnt')
            result_tmp2.columns=['base_value','ana_cnt']
            result_tmp=result_tmp1.merge(result_tmp2)
            result_tmp['base_feat']=s
            result_tmp['ana_feat']=t
            result_tmp['ana_value']=t
            result=pd.concat([result,result_tmp],ignore_index=True,sort=False)
    result['action_cnt']=result['action_cnt']/result['ana_cnt']
    result=result.rename(columns={'action_cnt':'ana_avg'})
    base_info=dis_base_count(data,col_list_dis)
    #print base_info.dtypes
    #print result.dtypes
    result=result.merge(base_info)
    result.loc[:,'ana_per']=result.loc[:,'ana_cnt']/result.loc[:,'base_cnt']
    end_time=time.time()
    print_time(start_time,end_time)
    return result.loc[:,['base_feat','base_value','base_cnt','base_per','ana_feat','ana_value','ana_cnt','ana_per','ana_avg']]


#对素材行进行预处理

#将一行素材使用记录拆分成dict形式，key是素材ID，value是使用次数(或设备数=1)
def str_dict(x):
    k = x.split('|')[0].split(',')
    #v = map(int,x.split('|')[1].split(','))#将这里的v改成恒等于1，则可以得到设备数
    v=[1]*len(k)
    return dict(zip(k,v))

# 两个内层dict相加
# 将两个素材dict进行合并计算，得到还是一个素材dict，key是素材ID，value是使用次数(或设备数)
def dict_in_add(dict1,dict2):
    for k,v in dict2.items():
        if dict1.has_key(k):
            dict1[k]=dict1[k]+v
        else:
            dict1[k]=v
    return dict1
#两个外层dict相加
#将两个
# 输入：dict_out[key,dict_in[k,v]]，key,dict_in[k,v]
def dict_out_add(dict_out,key,dict_in):
    if dict_out.has_key(key):
        dict_out[key]=dict_in_add(dict_out[key],dict_in)
    else:
        dict_out[key]=dict_in

def material_to_dict(data):
    rows=len(data)
    result_dict={}
    for s in range(rows):
        if len(data[s])>1:
            tmp_dict=str_dict(data[s])
            dict_in_add(result_dict,tmp_dict)
    return result_dict
def data_to_dict(data):
    result={}
    rows=len(data)
    for s in range(rows):#针对每个用户(每行记录)
        k=data.iloc[s,0]#获取基础属性取值作为外层的key
        if len(data.iloc[s,1])>1:#至少有使用过一款素材
            v=str_dict(data.iloc[s,1])#形成素材使用记录的字典，key是素材ID，value是使用次数
            dict_out_add(result,k,v)#针对
        else:
            v=''
    return result


# 离散型x素材
def dis_material(data,col_list_dis,col_list_material):
    list_result=[]
    for s in col_list_dis:#每个基础属性列
        print "s={}".format(s)
        start_time=time.time()
        for t in col_list_material:#每个素材列
            #print "t={}".format(t)
            d_part_result=data_to_dict(data[[s,t]])#这里得到了指定基础列的各个取值的素材使用情况，字典格式，key是取值，
                                                                        #value是字典(key是素材ID，value是该素材的使用次数)
            f_part_result=pd.DataFrame(d_part_result) # 将嵌套字典转换成dataframe，列是基础属性的取值，行index是素材ID，行value是统计次数
            for i in f_part_result.columns.values.tolist():#每个基础属性列的取值
                #print "i={}".format(i)
                s_part_result=f_part_result[i][~pd.isnull(f_part_result[i])].sort_values(ascending=False).iloc[0:N,]#剔除空值后，按照统计次数进行降序排序，取top10
                for j in range(len(s_part_result.index)):
                    list_result.append([s,t,i,s_part_result.index[j],s_part_result.values[j]])#后面添加list(s_part_result.values)则可以得到次数或设备数
        end_time=time.time()
        print_time(start_time,end_time)

    result=pd.DataFrame(list_result,columns=['base_feat','ana_feat','base_value','ana_value','ana_cnt'])
    base_info=dis_base_count(data,col_list_dis)
    #print base_info.dtypes
    if result['base_value'].dtypes=='int64':
        base_info['base_value']=base_info['base_value'].apply(int)
    else:
        result['base_value']=result['base_value'].apply(str)
    #print result.dtypes
    result=result.merge(base_info)
    result.loc[:,'ana_per']=result.loc[:,'ana_cnt']/result.loc[:,'base_cnt']
    result['ana_avg']=0
    return result.loc[:,['base_feat','base_value','base_cnt','base_per','ana_feat','ana_value','ana_cnt','ana_per','ana_avg']]

# 离散x机型品牌
def dis_dism(data,col_list_dis,col_list_dism):
    start_time=time.time()
    result=pd.DataFrame(columns=['base_value','ana_value','ana_cnt','base_feat','ana_feat'])#初始化
    for s in col_list_dis:
        for t in col_list_dism:
            result_tmp=data.groupby([s,t]).size().reset_index(name='ana_cnt').groupby(s, as_index=False).apply(lambda x:x.nlargest(N,'ana_cnt'))
            result_tmp.columns=['base_value','ana_value','ana_cnt']
            result_tmp['base_feat']=s
            result_tmp['ana_feat']=t
            result=pd.concat([result,result_tmp],ignore_index=True)
    base_info=dis_base_count(data,col_list_dis)
    #print base_info.dtypes
    #print result.dtypes
    result=result.merge(base_info)
    result.loc[:,'ana_per']=result.loc[:,'ana_cnt']/result.loc[:,'base_cnt']
    result['ana_avg']=0
    end_time=time.time()
    print_time(start_time,end_time)
    return result.loc[:,['base_feat','base_value','base_cnt','base_per','ana_feat','ana_value','ana_cnt','ana_per','ana_avg']]


# 连续型预处理
def con_subsect(data,col_list_con):
    for s in col_list_con:
        data.loc[:,'{}_dis'.format(s)]=data.loc[:,s]
        data.loc[(data['{}_dis'.format(s)]>=10),'{}_dis'.format(s)]=10
        data.loc[((data['{}_dis'.format(s)]<10) & (data['{}_dis'.format(s)]>=3)),'{}_dis'.format(s)]=3
        data.loc[((data['{}_dis'.format(s)]<3) & (data['{}_dis'.format(s)]>=1)),'{}_dis'.format(s)]=1
    return data

# 获取topN的机型/品牌
def get_dism_topN(data,col_list_dism):
    for s in col_list_dism:
        print s
        topN_data=data.groupby(s).size().reset_index(name='user_cnt').sort_values(by='user_cnt',ascending=False).iloc[0:N,]
        data=pd.merge(data,topN_data,how='left')
        data.loc[:,'{}_N'.format(s)]=data.loc[:,s]
        mask=(pd.isnull(data['user_cnt']))
        data.loc[mask,'{}_N'.format(s)]='other'
        data=data.drop(columns='user_cnt')
    return data

# #得到素材的topN，同时增加topN对应的列
# def material_topN(data,col_list_material,col_list_dis,col_list_con,col_list_dism,part,test):
#     topN_mat=[]
#     all_result=pd.DataFrame(columns=['base_feat','base_value','base_cnt','base_per','ana_feat','ana_value','ana_cnt','ana_per','ana_avg'])#初始化
#     for s in col_list_material:
#         print s
#         start_time=time.time()
#         d_result=material_to_dict(data[s])#统计每个素材使用设备数(次数)得到一个字典，key是素材ID，value是使用设备数(次数)
#         f_result=pd.DataFrame({'ID':d_result.keys(),'user_cnt':d_result.values()})#将字典转换成dataframe
#         topN_material=f_result.sort_values(by='user_cnt',ascending=False).iloc[0:N,0]#取top10的素材
#         for t in topN_material:
#             print t
#             data.loc[:,'{}'.format(t)] =data[s].apply(lambda x: 0 if len(x)<=1 else 2 if  len(set([t])&set(x.split('|')[0].split(',')))>0 else 1)
#             # 素材x离散
#             print "素材x离散"
#             result=deal_dis2(data,[t],col_list_dis)
#             all_result=all_result.append(result,ignore_index=True)
#             end_time=time.time()
#             print 'until now'
#             print_time(start_time,end_time)
#
#             # 素材x连续
#             print "素材x连续"
#             result=deal_dis_con(data,[t],col_list_con)
#
#             all_result=all_result.append(result,ignore_index=True)
#             end_time=time.time()
#             print 'until now'
#             print_time(start_time,end_time)
#
#             # 素材x机型
#             print "素材x机型"
#             result=dis_dism(data,[t],col_list_dism)
#
#             all_result=all_result.append(result,ignore_index=True)
#             end_time=time.time()
#             print 'until now'
#             print_time(start_time,end_time)
#
#             data.drop([t],axis=1,inplace=True)
#         data.loc[:,'{}_other'.format(s)] =data[s].apply(lambda x: 0 if len(x)<=1 else 1 if  len(set(topN_material)&set(x.split('|')[0].split(',')))>0 else 2)
#         # 素材x离散
#         print "素材x离散"
#         result=deal_dis2(data,['{}_other'.format(s)],col_list_dis)
#
#         all_result=all_result.append(result,ignore_index=True)
#         end_time=time.time()
#         print 'until now'
#         print_time(start_time,end_time)
#
#         # 素材x连续
#         print "素材x连续"
#         result=deal_dis_con(data,['{}_other'.format(s)],col_list_con)
#
#         all_result=all_result.append(result,ignore_index=True)
#         end_time=time.time()
#         print 'until now'
#         print_time(start_time,end_time)
#
#         # 素材x机型
#         print "素材x机型"
#         result=dis_dism(data,['{}_other'.format(s)],col_list_dism)
#
#         all_result=all_result.append(result,ignore_index=True)
#         end_time=time.time()
#         print 'until now'
#         print_time(start_time,end_time)
#
#         topN_mat.append(list(topN_material))
#         topN_mat.append(['{}_other'.format(s)])
#         data.drop(['{}_other'.format(s)],axis=1,inplace=True)
#
#         end_time=time.time()
#         print 'until now'
#         print_time(start_time,end_time)
#     write_data(all_result)
#     return sum(topN_mat,[])


def material_run_result(data,s,all_result,col_list_dis,col_list_con,col_list_dism,start_time):
    # 素材x离散
    print "素材x离散"
    result=deal_dis2(data,['{}_N'.format(s)],col_list_dis)

    all_result=all_result.append(result,ignore_index=True)

    end_time=time.time()
    print 'until now'
    print_time(start_time,end_time)

    # 素材x连续
    print "素材x连续"
    result=deal_dis_con(data,['{}_N'.format(s)],col_list_con)

    all_result=all_result.append(result,ignore_index=True)

    end_time=time.time()
    print 'until now'
    print_time(start_time,end_time)

    # 素材x机型
    print "素材x机型"
    result=dis_dism(data,['{}_N'.format(s)],col_list_dism)

    all_result=all_result.append(result,ignore_index=True)
    end_time=time.time()
    print 'until now'
    print_time(start_time,end_time)
    return all_result
#素材与其他类型的交叉
def material_topN_new(data,col_list_material,col_list_dis,col_list_con,col_list_dism,part,test):
    topN_mat=[]
    start_time=time.time()
    all_result=pd.DataFrame(columns=['base_feat','base_value','base_cnt','base_per','ana_feat','ana_value','ana_cnt','ana_per','ana_avg'])#初始化
    for s in col_list_material:
        print s
        d_result=material_to_dict(data[s])#统计每个素材使用设备数(次数)得到一个字典，key是素材ID，value是使用设备数(次数)
        f_result=pd.DataFrame({'ID':d_result.keys(),'user_cnt':d_result.values()})#将字典转换成dataframe
        topN_material=f_result.sort_values(by='user_cnt',ascending=False).iloc[0:N,0]#取top10的素材
        for t in topN_material:
            print t
            data.loc[:,'{}'.format(t)] =data[s].apply(lambda x: 0 if len(x)<=1 else 2 if  len(set([t])&set(x.split('|')[0].split(',')))>0 else 1)
            #tmp_data=data[data['{}'.format(t)]==2]
            #tmp_data['{}_N'.format(s)] =t
            data.loc[:,'{}_N'.format(s)]=t
            all_result=material_run_result(data[data['{}'.format(t)]==2],s,all_result,col_list_dis,col_list_con,col_list_dism,start_time)
            data.drop([t],axis=1,inplace=True)
        data.loc[:,'{}_other'.format(s)] =data[s].apply(lambda x: 0 if len(x)<=1 else 1 if len(set(topN_material)&set(x.split('|')[0].split(',')))>0 else 2)

        # tmp_data=data[data['{}_other'.format(s)]==2]
        # tmp_data['{}_N'.format(s)] ='other'
        # tmp_data1=data[data['{}_other'.format(s)]==0]
        # tmp_data1['{}_N'.format(s)] ='no'
        # tmp_data=tmp_data.append(tmp_data1)
        data['{}_N'.format(s)] ='other'
        all_result=material_run_result(data[data['{}_other'.format(s)]==2],s,all_result,col_list_dis,col_list_con,col_list_dism,start_time)
        data['{}_N'.format(s)] ='no'
        all_result=material_run_result(data[data['{}_other'.format(s)]==0],s,all_result,col_list_dis,col_list_con,col_list_dism,start_time)

        topN_mat.append(list(topN_material))
        topN_mat.append(['{}_other'.format(s)])
        data.drop(['{}_other'.format(s)],axis=1,inplace=True)

        end_time=time.time()
        print 'until now'
        print_time(start_time,end_time)
    write_data(all_result)
    end_time=time.time()
    print 'until now'

    print_time(start_time,end_time)
    print 'part c done'
    return sum(topN_mat,[])

def main():
    global os_p
    global N
    global date
    global data_size
    global table_name

    #table_name='myxj_basic_analysis_partbb'
    #drop_table()
    #create_table()

    # insert into myxj_basic_analysis select * from myxj_basic_analysis_partb;
    usage = "usage: %prog [options] arg1 arg2"
    parser = OptionParser(usage)
    parser.add_option("-d","--date", type="int", dest="date",help='date,e.g.20181101,if not be set, it will use the current date instead', default="20181001",metavar='date')
    parser.add_option("-o","--os_p", dest="os_p", help='os_p,e.g.android', default="android",metavar='os_p')
    parser.add_option("-p","--part", dest="part", help='data part,e.g.c,ab,ac,abc', default="abc",metavar='data part')
    parser.add_option("-t","--test", type="int", dest="test", help='test data num ,if test=0 then take all data,if test=1 then take the small sample data(10w)', default=0)
    parser.add_option("-N", type="int", dest="N", help='number：the top of device model or the top of material', default=10)

    (options, args) = parser.parse_args() #默认输入sys.argv[1:]

    os_p=options.os_p
    part=options.part
    test=options.test
    N=options.N
    if options.date<>20150101:
        date=options.date
    else:
        date=int(datetime.date.today().strftime('%Y%m%d'))
        date=(date/100)*100+1

    print "date={}".format(date)
    print "os_p={}".format(os_p)
    print "part={}".format(part)
    print "test={}".format(test)
    print "N={}".format(N)

    if test>0:
        table_name='tmp_basic_analysis'
        drop_table()
        create_table()

    print "table_name={}".format(table_name)
#    if (date/100)%100==12:
#        date=((date/10000+1)*100+1)*100+1
#    else:
#        date=(date/100+1)*100+1
    print "loading data"
    sys.stdout.flush()
    start_time=time.time()
    if test==1:
        data_test=pd.read_table('/data2/puckdata/android_test_info.20181228.gz',sep=u'\u0001')
    else:
        data_test=pd.read_table('/data2/puckdata/{}_all_user_features_monthly.{}.gz'.format(os_p,date/100),sep=u'\u0001')
    end_time=time.time()
    print_time(start_time,end_time)

    data_size=len(data_test)
    if test<=1:
        part_data=data_test
    else:
        part_data=data_test.head(test)
    col_list_dis=['gender','age_stage','city_tier','consumption_level','platform','device_level','is_app_new','channel']
    col_list_con=['active_days','tp_cnt','front_tp_cnt', 'back_tp_cnt', 'ar_tp_cnt', 'film_cnt', 'tv_cnt','ltv_cnt', 'gif_cnt', 'ai_tp_cnt', 'gjmy_edit_cnt']
    col_list_con_dis=['active_days_dis','tp_cnt_dis','front_tp_cnt_dis', 'back_tp_cnt_dis', 'ar_tp_cnt_dis', 'film_cnt_dis', 'tv_cnt_dis','ltv_cnt_dis', 'gif_cnt_dis', 'ai_tp_cnt_dis', 'gjmy_edit_cnt_dis']
    col_list_material=['tp_ar','tp_filter','film_theme']
    col_list_dism=['device_model','device_brand']
    col_list_dismN=['device_model_N','device_brand_N']

    if part.find('a')>=0:
        all_result=pd.DataFrame(columns=['base_feat','base_value','base_cnt','base_per','ana_feat','ana_value','ana_cnt','ana_per','ana_avg'])#初始化
        print "离散x离散"
        result=deal_dis2(part_data,col_list_dis,col_list_dis)
        all_result=all_result.append(result,ignore_index=True)
        end_time=time.time()
        print 'until now'
        print_time(start_time,end_time)
        #离散x连续
        print "离散x连续"
        result=deal_dis_con(part_data,col_list_dis,col_list_con)

        all_result=all_result.append(result,ignore_index=True)

        end_time=time.time()
        print 'until now'
        print_time(start_time,end_time)

        # 离散x机型(品牌)
        print "离散x机型(品牌)"
        result=dis_dism(part_data,col_list_dis,col_list_dism)

        all_result=all_result.append(result,ignore_index=True)
        end_time=time.time()
        print_time(start_time,end_time)
        #连续型离散化
        print "连续型离散化"
        part_data=con_subsect(part_data,col_list_con)

        # 连续x离散
        print "连续x离散"
        result=deal_dis2(part_data,col_list_con_dis,col_list_dis)

        all_result=all_result.append(result,ignore_index=True)

        end_time=time.time()
        print 'until now'
        print_time(start_time,end_time)
        # 连续x连续
        print "连续x连续"
        result=deal_dis_con(part_data,col_list_con_dis,col_list_con)
        all_result=all_result.append(result,ignore_index=True)

        end_time=time.time()
        print 'until now'
        print_time(start_time,end_time)
        # 连续x机型(品牌)
        print "连续x机型(品牌)"
        result=dis_dism(part_data,col_list_con_dis,col_list_dism)

        all_result=all_result.append(result,ignore_index=True)

        end_time=time.time()
        print 'until now'
        print_time(start_time,end_time)

        #获取topN机型，同时得到新的列
        print "获取topN机型，同时得到新的列"
        part_data=get_dism_topN(part_data,col_list_dism)

        # 机型(品牌)x离散
        print "机型(品牌)x离散"
        result=deal_dis2(part_data,col_list_dismN,col_list_dis)

        all_result=all_result.append(result,ignore_index=True)

        end_time=time.time()
        print 'until now'
        print_time(start_time,end_time)
        # 机型(品牌)x连续
        print "机型(品牌)x连续"
        result=deal_dis_con(part_data,col_list_dismN,col_list_con)

        all_result=all_result.append(result,ignore_index=True)

        end_time=time.time()
        print 'until now'
        print_time(start_time,end_time)
        # 机型(品牌)x机型品牌
        print "机型(品牌)x机型品牌"
        result=dis_dism(part_data,col_list_dismN,col_list_dism)

        all_result=all_result.append(result,ignore_index=True)
        write_data(all_result)
        end_time=time.time()
        print 'until now'
        print_time(start_time,end_time)
        print 'part a done'
        sys.stdout.flush()
    if part.find('b')>=0:
        all_result=pd.DataFrame(columns=['base_feat','base_value','base_cnt','base_per','ana_feat','ana_value','ana_cnt','ana_per','ana_avg'])#初始化
        ###素材相关
        if part.find('a')==-1:
            # #获取topN机型，同时得到新的列
            print "获取topN机型，同时得到新的列"
            part_data=get_dism_topN(part_data,col_list_dism)
        # 机型(品牌)x素材
        print "机型(品牌)x素材"

        result=dis_material(part_data,col_list_dismN,col_list_material)

        part_data.drop(col_list_dismN,axis=1,inplace=True)

        all_result=all_result.append(result,ignore_index=True)
        end_time=time.time()
        print 'until now'
        print_time(start_time,end_time)

        if part.find('a')==-1:
            #连续型离散化
            print "连续型离散化"
            part_data=con_subsect(part_data,col_list_con)
        # 连续x素材
        print "连续x素材"

        result=dis_material(part_data,col_list_con_dis,col_list_material)

        part_data.drop(col_list_con_dis,axis=1,inplace=True)
        all_result=all_result.append(result,ignore_index=True)
        end_time=time.time()
        print 'until now'
        print_time(start_time,end_time)
        #离散x素材
        print "离散x素材"

        result=dis_material(part_data,col_list_dis,col_list_material)
        all_result=all_result.append(result,ignore_index=True)

        write_data(all_result)
        end_time=time.time()
        print 'until now'
        print_time(start_time,end_time)

        print 'part b done'
        sys.stdout.flush()

    if part.find('c')>=0:
        material_topN_new(part_data,col_list_material,col_list_dis,col_list_con,col_list_dism,part,test)

if __name__ == '__main__':
    main()

