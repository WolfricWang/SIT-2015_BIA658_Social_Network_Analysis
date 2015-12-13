# -*- coding: utf-8 -*-
"""
Created on Wed Nov  4 13:43:24 2015

@author: Wolfric Wang @Stevens 
For more infor connect: dwang20@stevens.edu

Nov 12 Update
@author: Wolfric
change the database name
change for loop to while loop in insert_followers_id()

Nov 15 Update
@Wolfric
Fix some bugs of db related.

Dec 5
@Wolfric
Created part for collected sencond degree followers' id

Dec13
Upload to Github
"""

import time
import tweepy
#Twitter key , enter your key info here
consumer_key = '[consumer_key]'
consumer_secret='[consumer_secret]'
access_token='[access_token]'
access_secret='[access_secret]'

import pymysql.cursors
#insert your database infor here
db_info={
    'db_host': '[host name]',
    'db_user':'[user name]', 
    'db_password':'[password]',
    'db_name':'[db name]'}

#a dic of all candidates
candidates_dic = {
    '1339835893':{
        'name':'Hillary Rodham Clinton',
        'handle':'HillaryClinton',
        'id': 1339835893,
        'party': 'Democratic'
    },
    '11388132':{
        'name': 'Lawrence Lessig',
        'handle': 'lessig',
        'id': 11388132,
        'party':'Democratic'
    },
    '15824288':{
    	'name': "Martin O'Malley",
    	'handle': 'MartinOMalley',
    	'id': 15824288,
    	'party':'Democratic'
    },
    '216776631':{
    'name': 'Bernie Sanders',
    'handle': 'Bernie Sanders',
    'id': 216776631,
    'party': 'Democratic'
    },
    '113047940':{
    'name':'Jeb Bush',
    'handle': 'JebBush',
    'id': 113047940,
    'party':'Republican'
    },
    '1180379185':{
    'name': 'Ben Carson',
    'handle': 'RealBenCarson',
    'id': 1180379185,
    'party': 'Republican'
    },
    '1347285918':{
    'name': 'Chris Christie',
    'handle': 'ChrisChristie',
    'id': 1347285918,
    'party': 'Republican'
    },
    '23022687': {
    'name': 'Ted Cruz',
    'handle': 'tedcruz',
    'id': 23022687,
    'party': 'Republican'
    },
    '65691824': {
    'name': 'Carly Fiorina',
    'handle': 'CarlyFiorina',
    'id': 65691824,
    'party': 'Republican'
    },
    '3021632183': {
    'name': 'Jim Gilmore',
    'handle': 'gov_gilmore',
    'id': 3021632183,
    'party': 'Republican'
    },
    '432895323': {
    'name': 'Lindsey Graham',
    'handle': 'LindseyGrahamSC', 
    'id': 432895323,
    'party': 'Republican'
    },
    '15416505': {
    'name': 'Mike Huckabee',
    'handle': 'GovMikeHuckabee',
    'id': 15416505,
    'party':'Republican'
    },
    '17078632': {
    'name': 'Bobby Jindal',
    'handle':'BobbyJindal',
    'id': 17078632,
    'party':'Republican'
    },
    '18020081':{
    'name': 'John Kasich',
    'handle': 'JohnKasich',
    'id': 18020081,
    'party': 'Republican'
    },
    '2865560724': {
    'name': 'George Pataki',
    'handle': 'GovernorPataki',
    'id': 2865560724,
    'party':'Republican'
    },
    '216881337':{
    'name':'Rand Paul',
    'handle': 'RandPaul',
    'id': 216881337,
    'party': 'Republican'
    },
    '15745368':{
    'name': 'Marco Rubio',
    'handle': 'marcorubio',
    'id': 15745368,
    'party':'Republican'
    },
    '58379000': {
    'name':'Rick Santorum',
    'handle': 'RickSantorum',
    'id': 58379000,
    'party': 'Republican'
    },
    '25073877': {
    'name': 'Donald J. Trump',
    'handle':'realDonaldTrump',
    'id':25073877,
    'party': 'Republican'}
    }
    
#MySQL part
def db_sql_run(sql_code):
    try:
        con = pymysql.connect(host = db_info['db_host'],
                              user = db_info['db_user'], 
                              password = db_info['db_password'], 
                              db = db_info['db_name']);
        cur = con.cursor()
        cur.execute(sql_code)
        result = cur.fetchall() 
        #print(result)
        return result
        con.close()
    except Exception as e:
        #print("Error %d: %s" % (e.args[0],e.args[1]))
        print("Error:",e)

def show_db_tables():
    try:
        con = pymysql.connect(host = db_info['db_host'],
                              user = db_info['db_user'], 
                              password = db_info['db_password'], 
                              db = db_info['db_name']);
        sql = "Show tables in"+ db_name
        cur = con.cursor()
        cur.execute(sql)
        result = cur.fetchall() 
        print(result)
        con.close()
    except Exception as e:
        #print("Error %d: %s" % (Exception.args[0],Exception.args[1]))
        print("Error:", e)

    
def data_insert_candidate_list(connection, value_dic):
    try:
        with connection.cursor() as cursor:
            # Create a new record
            sqlbase = 'INSERT INTO'+ db_name+'''.candidate_list 
            (candidate_name,twitter_handle, twitter_Id, party ) 
            VALUES ("%(name)s", "%(handle)s", %(id)s, "%(party)s")'''
            sql=sqlbase % value_dic
            cursor.execute(sql)
        connection.commit()            
    except Exception:
        print("Error %d: %s" % (Exception.args[0],Exception.args[1]))

        
def candidate_info_insert():
    try:
        con = pymysql.connect(host = db_info['db_host'],
                              user = db_info['db_user'], 
                              password = db_info['db_password'], 
                              db = db_info['db_name']);
        for candi in candidates_dic:
            data_insert_candidate_list(con, candidates_dic[candi])
        con.close()
    except Exception as e:
        print("Error %d: %s" % (e.args[0],e.args[1]))

#untest yet nov 12
def insert_followers_id(table_name, target_twitter_id, followers_ids,start_n=0):
    con = pymysql.connect(host = db_info['db_host'],
                            user = db_info['db_user'], 
                            password = db_info['db_password'], 
                            db = db_info['db_name']);
    sqlbase = 'INSERT INTO'+ db_name+'.'+table_name+''' (follower_Id, candidate_Id)
                VALUES
                %s;'''    
    sql_values=' '
    sql_value_base='(%s,'+str(target_twitter_id)+')'
    len_ids=len(followers_ids)-1
    start_time = time.ctime()    
    i=start_n
    if i==0:
        print("Let's do this", start_time)
    try:
        #for i in followers_ids:
        while i <= len_ids:
            
            #with con.cursor() as cursor:
                # insert id
            one_value=sql_value_base % (followers_ids[i])                
                #print(sql)
                #cursor.execute(sql)
                #con.commit()
            if i!=len_ids:
                if i==0 or i%1000!=0:
                    sql_values=sql_values+one_value+','                
                elif i%1000==0 and i!=0:
                    sql_values=sql_values+one_value
                    sql = sqlbase%sql_values
                    #print(sql)                    
                    with con.cursor() as cursor:
                        cursor.execute(sql)                
                    con.commit()                
                    now_time=time.ctime()                
                    print('Followers id from %s to %s inserted: %s'%(i-1000,i,now_time))
                    sql_values=' '
            elif i==len_ids:
                sql_values=sql_values+one_value
                sql=sqlbase%sql_values
                #print(sql)                
                with con.cursor() as cursor:
                    cursor.execute(sql)                
                con.commit()
                now_time=time.ctime()                
                print('Finished at number: %s'%(i+1))
 
            i+=1
    except Exception as e:
        print("Error %d: %s" % (e.args[0],e.args[1]))
        print("End at number:", i)
     
    con.close()
    end_time = time.ctime()
    print('start:',start_time)
    print('end:', end_time)
    print('Total ids inserted:',i)
    return i
 
#Twitter writer part
def get_user_info(twitter_id):
    auth = tweepy.auth.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    api = tweepy.API(auth)
    result= api.get_user(id=twitter_id)
    return result

def get_location(twitter_id):
    twitter_user = get_user_info(twitter_id)
    return twitter_user.location

def get_followers_id(target_twitter_id,pages_limit=None):
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)    
    #auth = tweepy.AppAuthHandler(consumer_key, consumer_secret) #A way said more quickly. NO!!!not quick at all!
    auth.set_access_token(access_token, access_secret)
    #api = tweepy.API(auth,wait_on_rate_limit=True,
#				   wait_on_rate_limit_notify=True)
    api = tweepy.API(auth)
    ids = [] 
    try:
        if pages_limit == None:
            for page in tweepy.Cursor(api.followers_ids, id=target_twitter_id).pages():
                ids.extend(page)
                now_time=time.ctime()                    
                print('%s, Avoid Limit, need a sleep, colected:%s' %(now_time, len(ids)))
                time.sleep(60) #sleep() in seconds! 60 s, if in 2 seconds, hit limit at 75000,15 requests per 15m
        else:
            for page in tweepy.Cursor(api.followers_ids, id=target_twitter_id).pages(pages_limit):
                ids.extend(page)
                now_time=time.ctime()                    
                print('%s, Avoid Limit, need a sleep, colected:%s' %(now_time, len(ids)))
                time.sleep(60)
    except Exception:
        print("DAMN, Not Again!!Error")
    return ids


#something may need lots time!!!   
def get_followers_id_write(table_name, target_twitter_id,start_n=0,pages_limit=None):
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)    
    auth.set_access_token(access_token, access_secret)
    api = tweepy.API(auth)
    len_ids=0    
    ids_remain=[]
    the_start_time=time.ctime()    
    try:
        if pages_limit == None:
            for page in tweepy.Cursor(api.followers_ids, id=target_twitter_id).pages():
                ids_p=page                
                len_ids+=len(page)
                #for sleep
                now_time=time.ctime()                    
                print('--> %s, Avoid Limit, need a sleep, collected: %s' %(now_time,len_ids))                   
                time.sleep(60) #sleep() in seconds! 60 s, if in 2 seconds, hit limit at 75000,15 requests per 15m
                #else:
                if len(page)!=5000:
                    now_time=time.ctime()                    
                    print('--> %s, Almost Done, collected: %s'%(now_time,len_ids))
                #for insert    
                check_i=insert_followers_id(table_name, target_twitter_id, followers_ids=ids_p,start_n=0)
                if check_i==len(page):
                    pass
                else:
                    print('something wrong, expended ids_remain,check it later')
                    ids_remain.expend(ids_p[check_i:])
        else:
            for page in tweepy.Cursor(api.followers_ids, id=target_twitter_id).pages(pages_limit):
                ids_p=page                
                len_ids+=len(page)
                #for sleep
                now_time=time.ctime()
                print('--> %s, Avoid Limit, need a sleep, collected: %s' %(now_time,len_ids))                    
                time.sleep(60) #sleep() in seconds! 60 s, if in 2 seconds, hit limit at 75000,15 requests per 15m
                #else:
                if len(page)!=5000:
                    now_time=time.ctime()                    
                    print('--> %s, Almost Done, collected: %s'%(now_time,len_ids))
                #for insert
                check_i=insert_followers_id(table_name, target_twitter_id, followers_ids=ids_p,start_n=0)
                if check_i==len(page):
                    pass
                else:
                    print('something wrong, expended ids_remain,check it later')
                    ids_remain.expend(ids_p[check_i:])
    except Exception:
        print("DAMN, Not Again!!Error")
    the_end_time=time.ctime()
    print('The work started at %s, finished at %s'%(the_start_time,the_end_time))
    print('Total collected:%s' %len_ids)
    print('Remaind not inserted:%s'%(len(ids_remain)))
    return ids_remain

#The function do all    
def call_exec(work_table_list, work_id_list):
    remain_dic={}    
    for i in range(len(work_table_list)):
        a=work_table_list[i]
        b=work_id_list[i]
        remain_dic[a]=get_followers_id_write(a,b)        
        #print(a,b)        
        print(remain_dic)
    return remain_dic
    
#lookup mutiple ids, limit 100 ids per request
def look_up_users(ids=None, names=None, entities=None):
    auth = tweepy.auth.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    api = tweepy.API(auth)    
    if ids is not None:
        try:            
            result=api.lookup_users(user_ids=ids)
        except Exception:
            result=None
    elif names:
        try:
            result=api.lookup_users(screen_names=names)
        except Exception:
            result=None      
    elif entities:
        try:
            result=api.lookup_users(include_entities=entities)
        except Exception:
            result=None      
    return result

#insert info of an id_list
def users_info_insert(insert_table_name, ids):
    con = pymysql.connect(host = db_info['db_host'],
                            user = db_info['db_user'], 
                            password = db_info['db_password'], 
                            db = db_info['db_name']);
    sql_base='INSERT INTO'+ db_name+'.'+insert_table_name+''' (twitter_Id, created_at,
        favourites_count, followers_count, friends_count, location, tweets_count, rep_lable,
        dem_lable, party_lable) 
        VALUES ('%(twitter_Id)s', "%(created_at)s", '%(favourites_count)s', '%(followers_count)s', '%(friends_count)s', '%(location)s', '%(tweets_count)s', '%(rep_lable)s', '%(dem_lable)s', '%(party_lable)s'); 
        '''
    len_ids=len(ids)
    count_l=range(len_ids//100+1)
    for i in count_l:
        start_i=i*100
        end_i=start_i+100
        ids_temp=ids[start_i:end_i]
        #len_ids_temp
        users_info_holder=look_up_users(ids=ids_temp)
        
        sql_holder=''' '''
            #sql_values=' '
        if users_info_holder == None:
            print('Drop fake id')            
            pass
        else:
#            half_len=len(users_info_holder)//2
#            info_holder=[]
#            info_holder_a=[a for a in users_info_holder[0:half_len]]
#            info_holder_b=[b for b in users_info_holder[half_len:]]
#           info_holder.append(info_holder_a)
#            info_holder.append(info_holder_b)
            #for ii in range(2):
                #sql_holder=''' '''
            for u in users_info_holder:
                    #location_uc=u._json['location']
                    #location_c=location_uc.encode('latin1','ignore')
                    #sql_holder=''' '''
                user_info={'twitter_Id':u._json['id'], 
                                   'created_at': u._json['created_at'], 
                                   'favourites_count':u._json['favourites_count'], 
                                   'followers_count': u._json['followers_count'], 
                                   'friends_count': u._json['friends_count'], 
                                   'location': u._json['location'], 
                                   'tweets_count': u._json['statuses_count'], 
                                   'rep_lable':0, 
                                   'dem_lable':0, 
                                   'party_lable':0}
                sql_holder=sql_holder+sql_base % user_info
                    #print(sql_holder)
            now_time=time.ctime()
            try:
                with con.cursor() as cursor:
                    cursor.execute(sql_holder)
                con.commit()#the () is so important!!!!!!!!
                print('%s The %s to %s is finished'%(now_time,start_i,end_i))
            except Exception:
                    #print("Error %d: %s" % (e.args[0],e.args[1]))
                print('%s Drop something arond %s' %(now_time, start_i))                    
                pass
            #print('%s The %s to %s is finished'%(now_time,start_i,end_i))
        time.sleep(5)
    
    con.close()
    end_time=time.ctime()
    return end_time


#get user info by id, and write into database
def look_up_users_write(insert_table_name,sample_table_name):
    sql_1="select distinct follower_Id from"+ db_name+'.'+sample_table_name
    sql_result=db_sql_run(sql_1)
    ids_list=[a[0] for a in sql_result]
    end_time=users_info_insert(insert_table_name,ids_list)    
    print('%s, Done!'%end_time)


#Insert party,location in one table
def got_insert_party_location(table_a, table_b, output_table):
    dem_l=[1339835893, 11388132, 15824288,216776631]
    rep_l=[113047940,1180379185,1347285918,23022687,65691824,3021632183,432895323,15416505,17078632,
           18020081,2865560724,216881337,15745368,58379000,25073877]
    dem='Democratic'
    rep='Republican'
    sql_1='''select a.follower_Id, a.candidate_Id, b.location from''' 
    +db_name+'.'+table_a+' as a left join' +db_name+'.'+table_b+' as b ON a.follower_Id=b.twitter_Id'''
    result_holder=db_sql_run(sql_1)
    len_n=len(result_holder)
    i=0
    #j=0
    result_list=[]
    while i<len_n:
        if i%100==0 or i==len_n-1 and i!=0:
            insert_party_location(output_table, result_list)
            print('%s to %s ,inserting...'%(i-len(result_list), i))
            result_list=[]
        else:
            candi_id=result_holder[i][1]
            if candi_id in dem_l:
                party_lable=dem
            elif candi_id in rep_l:
                party_lable=rep
            else:
                party_lable=None
            id_info_temp={'twitter_Id': result_holder[i][0],
                          'candidate_Id':candi_id,
                          'location': result_holder[i][2], 
                          'party': party_lable}
            result_list.append(id_info_temp)
        i+=1
    print('Total %s Done'%(i))
    end_time=time.ctime()
    return end_time

def insert_party_location(table_name, data_list):
    len_list=len(data_list)
    con = pymysql.connect(host = db_info['db_host'],
                            user = db_info['db_user'], 
                            password = db_info['db_password'], 
                            db = db_info['db_name']);
    sqlbase = 'INSERT INTO'+ db_name+'.'+table_name+''' (twitter_Id, candidate_Id, location, party) 
                VALUES
                %s;'''    
    sql_value=''' '''
    value_base='''(%(twitter_Id)s, %(candidate_Id)s, "%(location)s", "%(party)s")'''
    i=0    
    while i<len_list:
        if i!=len_list-1: 
            sql_value=sql_value+value_base%data_list[i] +','
        else:
            sql_value=sql_value+value_base%data_list[i]
            sql=sqlbase % sql_value
            with con.cursor() as cursor: 
                cursor.execute(sql)                
            con.commit()
        i+=1
    print("%s data inserted"%i)                

#insert the party lable to id_info
def got_insert_party_lable(table_a, table_b):
    sql_1="select twitter_Id, rep_lable, dem_lable, party_lable from db_name."+table_a     
    holder_a=db_sql_run(sql_1)
    sql_2="select twitter_Id, party from"+ db_name+'.'+table_b     
    holder_b=db_sql_run(sql_2)
    i=0
    a_dic={}    
    while i<len(holder_a):
        a_dic[str(holder_a[i][0])]={'twitter_Id': holder_a[i][0], 
                                    'rep_lable': 0, 
                                    'dem_lable': 0, 
                                    'party_lable':0}
        i+=1
    print('Dic a with %s created'%i)
    j=0
    while j<len(holder_b):
        if holder_b[j][1]=='Democratic':
            id_str=str(holder_b[j][0])            
            if a_dic[id_str]!=None:            
                a_dic[id_str]['dem_lable']+=1
                a_dic[id_str]['party_lable']=a_dic[id_str]['dem_lable']+a_dic[id_str]['rep_lable']
            else:
                pass
        elif holder_b[j][1]=='Republican':
            if a_dic[id_str]!=None:            
                a_dic[id_str]['rep_lable']-=1
                a_dic[id_str]['party_lable']=a_dic[id_str]['dem_lable']+a_dic[id_str]['rep_lable']
            else:
                pass
        j+=1
    
    print('%s Dic a lable renew'%j)
    k=0
    while k<len(holder_a):
        id_str2=str(holder_a[k][0])
        one_dic=a_dic[id_str2]
        insert_party_lable(table_a, one_dic)
        print('%s is inserted'%k)
        k+=1
    print('Done! %s'%k)    
    end_time=time.ctime()
    return end_time

def insert_party_lable(target_table, dic):
    con = pymysql.connect(host = db_info['db_host'],
                            user = db_info['db_user'], 
                            password = db_info['db_password'], 
                            db = db_info['db_name']);
    sqlbase = 'UPDATE'+ db_name+'.'+target_table+''' SET rep_lable=%(rep_lable)s, dem_lable=%(dem_lable)s, party_lable=%(party_lable)s 
                WHERE twitter_Id=%(twitter_Id)s;'''
    sql=''' '''
    #for id_str in dics:
    #sql=sql+sqlbase % dics[id_str]
    sql=sqlbase % dic
    with con.cursor() as cursor: 
        cursor.execute(sql)                
    con.commit()

#1.5 degree follower_ids get and insert
def insert_followers_id_2(table_name, target_twitter_id, followers_ids,start_n=0):
    con = pymysql.connect(host = db_info['db_host'],
                            user = db_info['db_user'], 
                            password = db_info['db_password'], 
                            db = db_info['db_name']);
    sqlbase = 'INSERT INTO'+ db_name+'.'+table_name+''' (twitter_Id,follower_Id)
                VALUES
                %s;'''    
    sql_values=' '
    sql_value_base='('+str(target_twitter_id)+', %s)' #changed something
    len_ids=len(followers_ids)-1
    start_time = time.ctime()    
    i=start_n
    if i==0:
        print("Let's do this", start_time)
    try:
        #for i in followers_ids:
        while i <= len_ids:
            
            #with con.cursor() as cursor:
                # insert id
            one_value=sql_value_base % (followers_ids[i])
            #print(one_value)                
                #print(sql)
                #cursor.execute(sql)
                #con.commit()
            if i!=len_ids:
                if i==0 or i%100!=0:
                    sql_values=sql_values+one_value+','                
                elif i%100==0 and i!=0:
                    sql_values=sql_values+one_value
                    sql = sqlbase%sql_values
                    #print(sql)                    
                    with con.cursor() as cursor:
                        cursor.execute(sql)                
                    con.commit()                
                    now_time=time.ctime()                
                    print('%s followers id from %s to %s inserted: %s'%(target_twitter_id, i-100,i,now_time))
                    sql_values=' '
            elif i==len_ids:
                sql_values=sql_values+one_value
                sql=sqlbase%sql_values
                #print(sql)                
                with con.cursor() as cursor:
                    cursor.execute(sql)                
                con.commit()
                now_time=time.ctime()                
                print('Finished at number: %s'%(i+1))
 
            i+=1
    except Exception as e:
        print("Error %d: %s" % (e.args[0],e.args[1]))
        print("End at number:", i)
     
    con.close()
    end_time = time.ctime()
    print('start:',start_time)
    print('end:', end_time)
    print('Total ids inserted:',i)
    return i


def insert_second_degree_follower_id(target_table, location='New York, NY'):
    con = pymysql.connect(host = db_info['db_host'],
                            user = db_info['db_user'], 
                            password = db_info['db_password'], 
                            db = db_info['db_name']);    
    #location='New York, NY'
    sql1 ='''select twitter_Id, candidate_Id from'''+ db_name+'''.id_info_location_party 
     where location=%s'''%location
    sql_return=db_sql_run(sql1)
    for r in sql_return:
        sql2="INSERT INTO"+ db_name+'.'++target_table+''' (twitter_Id, follower_Id) 
                VALUES ("%s", "%s")'''%(r[1],r[0])
        with con.cursor() as cursor: 
            cursor.execute(sql2)                
        con.commit()
    con.close()
    print('First degree data inserted')    
    target_id_l=[a[0] for a in sql_return]
    i=0
    while i<len(target_id_l):
        return_1=get_followers_id(target_id_l[i])
        #time.sleep(60)
        #print('Sleeping...')
        #f_ids=[]
        f_ids=return_1
        #j=0
        #while j<len(return_1):
            #if return_1[j] in target_id_l:
                #f_ids.append(return_1[j])
            #else:
                #pass
            #j+=1
        t=insert_followers_id_2(target_table, target_id_l[i], f_ids)
        i+=1        
        print('%s followers inserted %s followers'%(target_id_l[i],t))
