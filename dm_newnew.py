#!/usr/bin/env python
#coding:utf-8
import urllib.request
from bs4 import BeautifulSoup
import urllib.parse
import json
import math
import pymysql
import time
import random
import re
import qlog

#正则表达式
reg = []
#家庭套票900(380x3) || 英文括号 乘号*|x|X|×
reg.append(re.compile(r".*\((\d+)[×x\*X](\d+)\)"))
#家庭套票900（380*3）||中文括号 乘号*|x|X|×
reg.append(re.compile(r".*（(\d+)[×x\*X](\d+)）"))
#（280元×2张）||中文括号 && 乘号*|x|X|×
reg.append(re.compile(r".*（(\d+)元[×x\*X](\d+)张）"))
#黄金时段买7送2套票   ||故意捕获三个，区别上述捕获两个的情况
reg.append(re.compile(r".*(买)(\d+)送(\d+)"))
#中文数字 买一送一     ||中文数字匹配一到二十
reg.append(re.compile(r".*(买)([\u4e00\u4e8c\u4e09\u56db\u4e94\u516d\u4e03\u516b\u4e5d\u5341]{1,2})(送)([\u4e00\u4e8c\u4e09\u56db\u4e94\u516d\u4e03\u516b\u4e5d\u5341]{1,2})"))

numConvert = {'十一':11, '十二':12, '十三':13, '十四':14, '十五':15, '十六':16, '十七':17, '十八':18, '十九':19, '二十':20,'四': 4, '八': 8, '三': 3, '九': 9, '六': 6, '二': 2, '七': 7, '十': 10, '一': 1, '五': 5}
#Convert the Chinese numbers to the normal numbers [一二三四五六七八九十...]   to   1-20
#
# e.g. '一'          =>  1
#      '二'          =>  2
#     not match      =>  0  
def convertChar(ch):
    if ch in numConvert.keys():
        return numConvert[ch]
    else:
        return 0

#get the Volume of the pkg tickets through the remark filed
#e.g.       买一送一                return 2
#           买二送一                return 3
#           国庆大酬宾（380*3）     return 3
#           买1送1                  return 2  
#           not match               return 0
#           
def getVolume(remark):
    for j in reg:
        m = j.match(remark)
        if m:
            if len(m.groups())==2:
                return int(m.group(2))
            elif len(m.groups())==3:
                return int(m.group(2))+int(m.group(3))
            elif len(m.groups())==4 and convertChar(m.group(2)) and convertChar(m.group(4)):
                return convertChar(m.group(2))+convertChar(m.group(4))
    return 0

#if the event is pass ticket
# if matched the chars list below:
#       通票|常年|全年|每天         return 1
# not matched                       return 0
# 
def isPassEvent(eventTitle):
    if re.match(r".*通票|常年|全年|每天",eventTitle):
        return 1
    else:
        return 0

#get the pid by sid & priceRegion from [mp3_price_section]
#
#   exist           => pid
#   not_existed     => 0
#   
def getPid(sid,priceRegion):
    try:
        conn = pymysql.connect(host='localhost', user='root', passwd='root', db='xishiqu',charset='utf8')
        cur = conn.cursor()
        cur.execute("select pid from mp3_price_section  where sid=%s and priceRegion=%s",(sid,priceRegion))
        if cur.rowcount:
            return cur.fetchone()[0]
        else:
            return 0
    except Exception as err:
        qlog.logger.error('[error] 8')
        qlog.logger.error("sql : \t select pid from mp3_price_section  where sid=%s and priceRegion=%s"%(sid,priceRegion))
        qlog.logger.error(err)   
    finally:
        cur.close()
        conn.commit()
        conn.close()

#get seid by sid & eventTitle from [mp3_spider_event]
#
#   exist           => seid
#   not_existed     => 0
#   
def getSeidByTitle(sid,eventTitle):
    try:
        conn = pymysql.connect(host='localhost', user='root', passwd='root', db='xishiqu',charset='utf8')
        cur = conn.cursor()
        cur.execute("select seid from mp3_spider_event where sid=%s and eventTitle=%s",(sid,eventTitle))
        if cur.rowcount:
            return cur.fetchone()[0]
        else:
            return 0
    except Exception as err:
        qlog.logger.error('[error] 14')
        qlog.logger.error("sql : \t select seid from mp3_spider_event where sid=%s and eventTitle=%s"%(sid,eventTitle))
        qlog.logger.error(err)
    finally:
        cur.close()
        conn.commit()
        conn.close()

#get the veid from [mp3_spider_activity]
#
#   exist           =>  veid
#   not_existed     =>  0
#   
def getVeid(veName,siteUrl='http://www.damai.cn/'):
    try:
        conn = pymysql.connect(host='localhost', user='root', passwd='root', db='xishiqu',charset='utf8')
        cur=conn.cursor()
        cur.execute("SELECT veid FROM mp3_spider_vemapping WHERE spiderVename=%s and siteUrl=%s;",(veName,siteUrl))
        if cur.rowcount:
            return cur.fetchone()[0]
        else:
            return 0
    except Exception as err:
        qlog.logger.error('[error] 4')
        qlog.logger.error("sql : \t SELECT veid FROM mp3_spider_vemapping WHERE spiderVename=%s and siteUrl=%s;"%(veName,siteUrl))
        qlog.logger.error(err)
    finally:
        cur.close()
        conn.commit()
        conn.close()

#get the sid from [mp3_spider_activity]
#
#   exist           =>  sid
#   not_existed     =>  0
#   
def getSid(siteUrl,actName,fromUrl):
    try:
        conn = pymysql.connect(host='localhost', user='root', passwd='root', db='xishiqu',charset='utf8')
        cur=conn.cursor()
        cur.execute("select sid from mp3_spider_activity  where siteUrl=%s and actName=%s and fromUrl=%s;",(siteUrl,actName,fromUrl))
        if cur.rowcount:
            return cur.fetchone()[0]
        else:
            return 0
    except Exception as err:
        qlog.logger.error('[error] 3')
        qlog.logger.error("sql : \t select sid from mp3_spider_activity  where siteUrl=%s and actName=%s and fromUrl=%s;"%(siteUrl,actName,fromUrl))
        qlog.logger.error(err)
    finally:
        cur.close()
        conn.commit()
        conn.close()

#get the current time accoiding to the customized format
#   e.g.    2014-5-14 19:20:35
#   
def getNow():
    return time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))

#get current time by a customized format
#   e.g.    2014-05-06 12:00:46
#   
def randSleep(start =7,end =10):
    t = random.randint(start,end)
    qlog.qPrint('----------------------------------')
    qlog.qPrint("waiting for %d seconds"%t)
    qlog.qPrint('----------------------------------')
    time.sleep(t)
    qlog.qPrint('----------------------------------')
    qlog.qPrint("waiting finnished %d seconds"%t)
    qlog.qPrint('----------------------------------')

#faked xhr to request for the json data
#   passing a base url 
#   passing a list of params {key:value,key:value,key:value,key:value}
#   
#   faking a faked user_agent
#   return the data [xml | json | html | else ]
#   
def getAjax(url,data):
    url=url
    values=data
    user_agent = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/30.0.1599.114 Chrome/30.0.1599.114 Safari/537.36'
    headers = {'User-Agent':user_agent,'Content-Type':'application/x-www-form-urlencoded; charset=UTF-8','X-Requested-With':'XMLHttpRequest'}
    data = urllib.parse.urlencode(values).encode(encoding='UTF8')
    try:
        req = urllib.request.Request(url,data,headers)
        response = urllib.request.urlopen(req,timeout=10)
        the_page = response.read().decode('utf8')
    except Exception as err:
        qlog.logger.error("[error] 88 %s"%err)
        return None
        # randSleep()
    return the_page

#convert json status to the DB status in the db tables  [saleStatus][mp3_spider_activity]
#
#   json    db      for referance
#   0       5       待定
#   1       0       预售
#   2       1       销售中
#   others  100
#   
def getSalestatus(status):
    if status == 0:
        return 5
    # elif status == 4:
    #     return 2
    elif status == 1:
        return 0
    elif status == 2:
        return 1
    else:
        return 100
#START OF THE SPIDER
qlog.qPrint("---------------------------------")
qlog.qPrint("START time : %s "%getNow())
qlog.qPrint("---------------------------------")

#base siteUrl [damai.cn]
siteUrl = "damai.cn"

#var for storing the all the urls links
urls = []

#var for storing the urls with some errors
urls_error = []

#START OF GETTING URLS

#the entrance | gate of the spider
url="http://search.damai.cn/searchajax.html"

#faked user_agent
user_agent = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/30.0.1599.114 Chrome/30.0.1599.114 Safari/537.36'
#faked header content
headers = {'User-Agent':user_agent,'Content-Type':'application/x-www-form-urlencoded; charset=UTF-8','X-Requested-With':'XMLHttpRequest'}
#params to be passed
data = {"keyword":"", "cty":"上海", "ctl":"","singleChar":"", "order":""}

f = getAjax(url,data)
#load the result returned by the XHR
result = json.loads(f)


#record the sum of the pages
sum = result["pageData"]["factMap"]["cityname"][0]["count"]

#the start of the page to be requested
start =1

#the end of the page to be requested
end = math.ceil(sum/10)+1
qlog.qPrint(end)


# from the start to the end but not included   ==>   page : [start,end)
for i in list(range(start,end)):
# [for debug only]
# for i in list(range(1,2)):
    index = list(range(start,end)).index(i)+1
    qlog.qPrint("开始第%d个连接"%index)
    values={"keyword":"", "cty":"上海", "ctl":"", "currPage":i, "singleChar":"", "order":""}
    data = urllib.parse.urlencode(values).encode(encoding='UTF8')
    try:
        req = urllib.request.Request(url,data,headers)
        response = urllib.request.urlopen(req,timeout=10)
        # randSleep()
    except urllib.error.URLError as err:
        qlog.logger.error("[error] 0")
        qlog.logger.error(err)
        response = urllib.request.urlopen(req,timeout=10)
        continue
    the_page = response.read()
    output = json.loads(the_page.decode('utf-8'))
    for i in output['pageData']['resultData']:
        urls.append((i['projectid'],i['imgurl'],i['name'],i['venue'],i['showtime'],i['categoryname'],i['status'],i['showstatus'],i['isxuanzuo']))

qlog.qPrint("总共%s个演出"%len(urls))
#END OF GETTING URLS

# limit =2
# sum_limit =limit
for i in urls:
    # uncomment to enable the limitation
    # start of the limitation block
    # if limit>0:
    #     limit-=1
    # else:
    #     break
    #end of the limitation block

    #basic data 
    #   e.g.   
    #   (65583, '655', '话剧《钱多多嫁人记》', '上海话剧艺术中心-艺术剧院', '2014.08.12-2014.08.24', '话剧歌剧', 2, '售票中', None)
    base_data = i
    pid=0

    #for counting the act
    index = urls.index(i)+1
    qlog.qPrint("[演出] %d"%index)

    #url of the act page 
    #   e.g.   'http://item.damai.cn/64691.html'
    projectId = i[0]

    #url of the act
    #   e.g.
    url ="http://item.damai.cn/%s.html"%projectId
    qlog.qPrint("%s\n"%url)
    imgId = i[1]

    #url of the img
    actImgUrl = "http://pimg.damai.cn/perform/project/%s/%s_n.jpg"%(imgId,projectId)
    qlog.qPrint("----------------------------------")
    qlog.qPrint("actImgUrl : %s"%actImgUrl)
    qlog.qPrint("----------------------------------")

    #request for the page content
    try:
        f = urllib.request.urlopen(url,timeout=10).read().decode('utf-8')
    except Exception as err:
        qlog.logger.error("[error] qin : %s"%err)
        qlog.logger.error(url)
        urls_error.append(base_data)
        continue
    soup = BeautifulSoup(f)
    #演出名称
    actName = i[2]
    qlog.qPrint("----------------------------------")
    try: 
        qlog.qPrint("actName : %s"%actName)
    except UnicodeEncodeError as err:
        qlog.qPrint(err)
    qlog.qPrint("----------------------------------")
    #场馆名称
    if i[3]:
        veName = i[3]
    else:
        veName = '待定'
    qlog.qPrint("veName : %s"%veName)
    #场馆ID
    veid=getVeid(veName)
    qlog.qPrint(veid)

    #演出页面
    fromUrl = url
    #演出页面
    currentUrl =url
    #演出时间
    actTime =i[4]
    qlog.qPrint("----------------------------------")
    qlog.qPrint("actTime:%s"%actTime)
    qlog.qPrint("----------------------------------")
    #演出类别
    catName = i[5]
    qlog.qPrint("catName:%s"%catName)
    #演出状态
    status = 1

    #默认状态值
    editStatus= 0
    addStatus=0
    hasSeatMap=0
    isSeatable=0
    #销售状态
    saleStatus=getSalestatus(i[6])
    qlog.qPrint("---------------------------------")
    qlog.qPrint("saleStatus：%s"%saleStatus)
    qlog.qPrint("---------------------------------")
    #销售状态名
    saleName=i[7]
    qlog.qPrint("---------------------------------")
    qlog.qPrint("saleName：%s"%saleName)
    qlog.qPrint("---------------------------------")
    #获取sid
    sid=getSid(siteUrl,actName,fromUrl)
    qlog.qPrint('---------------------------')
    qlog.qPrint("sid: %s"%sid)
    qlog.qPrint('---------------------------')
#START mp3_spider_activity
    if sid:
        try:
            conn = pymysql.connect(host='localhost', user='root', passwd='root', db='xishiqu',charset='utf8')
            cur = conn.cursor()
            cur.execute("update mp3_spider_activity set veid=%s,actTime=%s,catName=%s,actImgUrl=%s,currentUrl=%s,status=%s,editStatus=%s,addStatus=%s,hasSeatMap=%s,isSeatable=%s,saleStatus=%s,veName=%s,saleName=%s,createDT=%s where sid=%s;",(veid,actTime,catName,actImgUrl,currentUrl,status,editStatus,addStatus,hasSeatMap,isSeatable,saleStatus,veName,saleName,getNow(),sid))
        except Exception as err:
            qlog.logger.error('[error] 1')
            qlog.logger.error("sql : \t update mp3_spider_activity set veid=%d,actTime=%s,catName='%s',actImgUrl='%s',currentUrl='%s',status=%d,editStatus=%d,addStatus=%d,hasSeatMap=%d,isSeatable=%d,saleStatus=%d,veName='%s',saleName='%s',createDT='%s' where sid=%d;"%(veid,actTime,catName,actImgUrl,currentUrl,status,editStatus,addStatus,hasSeatMap,isSeatable,saleStatus,veName,saleName,getNow(),sid))
            qlog.logger.error(err)
            urls_error.append(base_data)
            continue
        finally:
            cur.close()
            conn.commit()
            conn.close()            
    else:
        try:
            conn = pymysql.connect(host='localhost', user='root', passwd='root', db='xishiqu',charset='utf8')
            cur = conn.cursor()
            cur.execute("insert into mp3_spider_activity(siteUrl,veid,actTime,actName,catName,actImgUrl,fromUrl,currentUrl,status,editStatus,addStatus,hasSeatMap,isSeatable,saleStatus,veName,saleName,createDT) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);",(siteUrl,veid,actTime,actName,catName,actImgUrl,fromUrl,currentUrl,status,editStatus,addStatus,hasSeatMap,isSeatable,saleStatus,veName,saleName,getNow()))
            
        except Exception as err:
            qlog.logger.error('[error] 2')
            qlog.logger.error("sql : \t insert into mp3_spider_activity(siteUrl,veid,actTime,actName,catName,actImgUrl,fromUrl,currentUrl,status,editStatus,addStatus,hasSeatMap,isSeatable,saleStatus,veName,saleName,createDT) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"%(siteUrl,veid,actTime,actName,catName,actImgUrl,fromUrl,currentUrl,status,editStatus,addStatus,hasSeatMap,isSeatable,saleStatus,veName,saleName,getNow()))
            qlog.logger.error(err)
            urls_error.append(base_data)
            continue
        finally:
            cur.close()
            conn.commit()
            conn.close()
            #在插入活动以后更新当前sid的值
            sid = getSid(siteUrl,actName,fromUrl)

    qlog.qPrint('---------------------------')
    qlog.qPrint("new sid: %s"%sid)
    qlog.qPrint('---------------------------')
#END mp3_spider_activity
    if soup.find(id="perform"):
        perfs = soup.find(id="perform").find_all('a')
    else:
        qlog.qPrint("这场演出[待定]")
        continue

    for i in perfs:        
        #价格区域 
        #    e.g. 80-150-200-
        priceRegion=''
        index= perfs.index(i)
        #时间序列号(1,2,3,4,5)
        eventSeries = index+1
        #用空格替换\xa0
        eventTitle = i.text.replace(u'\xa0',u' ')
        #事件事件
        #e.g.   2014-7-11 19:30:00
        eventDatetime = i.get('time')
        qlog.qPrint("eventTitle:%s"%eventTitle)
        qlog.qPrint("No: %s"%eventSeries)

        #是否售完
        isSaleOut = 0

        #获取演出描述
        remark =i.get("pfdes")

        #是否通票
        isPass = isPassEvent(eventTitle)

        #通票|套票  默认值
        tckType = 0
        pkgTcks =0
        fromTime = '0000-00-00 00:00:00'
        toTime = '0000-00-00 00:00:00'
        qlog.qPrint("演出编号[pid] : %s 时间[time] %s \n"%(i.get('pid'),i.get('time')))

#START OF THE XHR
        values = {"type":33,"performID":i.get('pid'),"business":soup.find(id="Business").get('value'),"IsBuyFlow":soup.find(id="IsBuyFlow").get('value'),"sitestaus":soup.find(id="hidSiteStatus").get('value')}
        the_page=getAjax("http://item.damai.cn/ajax.aspx",values)
        if the_page == None:
            urls_error.append(base_data)
            continue;
#END OF THE XHR

#parsing the returned data to get the prices under the current event 
        if len(the_page.split('^'))>=2 and len(BeautifulSoup(the_page.split('^')[1]).find_all('a'))>1:
            price = BeautifulSoup(the_page.split('^')[1]).find_all('a')
        else:
            price = soup.find(id="price").find_all('a')

        qlog.qPrint("价格:")
        #count the prices for recording the sale status
        cnt = 0
        #the saleStatuses for each price
        #   e.g     1 =>    销售中
        #           2 =>    已售完
        statuses =[]
        #remarks for special tickets LIKE package tcks | pass tcks | 100(东门) not pck not pass but with description
        price_status = []
        tck_type =[]
        for i in price:
            if('grey' in i['class']):
                qlog.qPrint("%s \t %s"%(i.get('price'),'已卖完'))
                cnt+=1
                statuses.append(2)
            else:
                qlog.qPrint("%s \t %s"%(i.get('price'),'销售中'))
                statuses.append(1)
            # 't'=1                     => 套票
            # 't'=0                     => 普通
            # 't'=0 and 'n' != 'price'  => 非通票非套票，但存在描述性语句
            if i.get("t")=="1":
                price_status.append(i.get("n"))
                tck_type.append(1)
            elif i.get("n") !=i.get("price"):
                price_status.append(i.get("n"))
                tck_type.append(0)
            else:
                price_status.append("")
                tck_type.append(0)
            #making the priceRegion variable
            priceRegion+=(i.get('price')+'-')
        qlog.qPrint("价格区域:%s"%priceRegion)

        # if cnt = the len of the prices
        # set the isSaleOut => 1 
        if cnt == len(price):
            isSaleOut = 1

        if isSaleOut:
            qlog.qPrint("[已售空]")
        else:
            qlog.qPrint("[销售中]")

#START mp3_spider_event 
        #获取seid
        #存在         => 更新
        #不存在       => 插入
        seid = getSeidByTitle(sid,eventTitle)
        qlog.qPrint('-----------------------------')
        qlog.qPrint("seid: %s"%seid)
        qlog.qPrint('-----------------------------')
        if seid:
            try:
                conn = pymysql.connect(host='localhost', user='root', passwd='root', db='xishiqu',charset='utf8')
                cur = conn.cursor()
                cur.execute("update mp3_spider_event set eventSeries=%s,priceRegion=%s,eventTitle=%s,isSaleOut=%s where seid=%s",(eventSeries,priceRegion,eventTitle,isSaleOut,seid))
            except Exception as err:
                qlog.logger.error('[error] 6') 
                qlog.logger.error("sql : \t update mp3_spider_event set eventSeries=%s,priceRegion=%s,eventTitle=%s,isSaleOut=%s where seid=%s"%(eventSeries,priceRegion,eventTitle,isSaleOut,seid))               
                qlog.logger.error(err)
                urls_error.append(base_data)
                continue
            finally:
                cur.close()
                conn.commit()
                conn.close()                
        else:
            try:
                conn = pymysql.connect(host='localhost', user='root', passwd='root', db='xishiqu',charset='utf8')
                cur = conn.cursor()
                cur.execute("insert into mp3_spider_event(sid,eventSeries,priceRegion,eventTitle,eventDatetime,isSaleOut) values(%s,%s,%s,%s,%s,%s)",(sid,eventSeries,priceRegion,eventTitle,eventDatetime,isSaleOut))
            except Exception as err:
                qlog.logger.error('[error] 7')
                qlog.logger.error("sql : \t insert into mp3_spider_event(sid,eventSeries,priceRegion,eventTitle,eventDatetime,isSaleOut) values(%s,%s,%s,%s,%s,%s)"%(sid,eventSeries,priceRegion,eventTitle,eventDatetime,isSaleOut))
                qlog.logger.error(err)
                urls_error.append(base_data)
                continue
            finally:
                cur.close()
                conn.commit()
                conn.close()
        qlog.qPrint('-----------------------------')
        qlog.qPrint("new seid: %s"%seid)
        qlog.qPrint('-----------------------------')
#END mp3_spider_event 

#START mp3_price_section
        #获取价格区域pid
        #存在         => 更新
        #不存在       => 插入
        pid =getPid(sid,priceRegion)
        qlog.qPrint("pid 1:%s"%pid)
        if pid:
            try:
                conn = pymysql.connect(host='localhost', user='root', passwd='root', db='xishiqu',charset='utf8')
                cur = conn.cursor()
                cur.execute("update mp3_price_section set priceRegion=%s where pid=%s",(priceRegion,pid))
                cur.execute("update mp3_spider_event set pid=%s where sid=%s and priceRegion=%s",(pid,sid,priceRegion))
            except Exception as err:
                qlog.logger.error('[error] 9')
                qlog.logger.error("sql : \t update mp3_price_section set priceRegion=%s where pid=%s"%(priceRegion,pid))
                qlog.logger.error("sql : \t update mp3_spider_event set pid=%s where sid=%s and priceRegion=%s"%(pid,sid,priceRegion))
                qlog.logger.error(err)
                urls_error.append(base_data)
                continue
            finally:
                cur.close()
                conn.commit()
                conn.close()
        else:
            try:
                conn = pymysql.connect(host='localhost', user='root', passwd='root', db='xishiqu',charset='utf8')
                cur = conn.cursor()
                cur.execute("insert into mp3_price_section(sid,priceRegion) values(%s,%s)",(sid,priceRegion))
                pid =getPid(sid,priceRegion)
                qlog.qPrint("pid 2:%s"%pid)
                cur.execute("update mp3_spider_event set pid=%s where sid=%s and priceRegion=%s",(pid,sid,priceRegion))
            except Exception as err:
                qlog.logger.error('[error] 10')
                qlog.logger.error("sql : \t insert into mp3_price_section(sid,priceRegion) values(%s,%s)"%(sid,priceRegion))
                qlog.logger.error("sql : \t update mp3_spider_event set pid=%s where sid=%s and priceRegion=%s"%(pid,sid,priceRegion))
                qlog.logger.error(err)
                urls_error.append(base_data)
                continue
            finally:
                cur.close()
                conn.commit()
                conn.close()
        #格式转换
        #100-180-280-380-  ==> 100.00-180.00-280.00-380.00
        newPriceR = priceRegion.rstrip('-').split('-')
        for i in list(range(0,len(newPriceR))):
            newPriceR[i]=newPriceR[i] + '.00'
        qlog.qPrint('-'.join(newPriceR))
#END mp3_price_section

#START mp3_price_section_detail
        if seid:
            # try:
            #     conn = pymysql.connect(host='localhost', user='root', passwd='root', db='xishiqu',charset='utf8')
            #     cur = conn.cursor()
            #     for facePrice in newPriceR:
            #         cur.execute("update mp3_price_section_detail set regionName=%s,facePrice=%s,seatNums=%s,remark=%s where pid = %s",('',facePrice,0,'',seid))
            # except Exception as err:
            #     qlog.qPrint('[error] 12')
            #     qlog.qPrint(err)
            # finally:
            #     cur.close()
            #     conn.commit()
            #     conn.close()
            #如果存在
            pass
        else:
            #如果不存在，插入到[mp3_price_section_detail]
            try:
                conn = pymysql.connect(host='localhost', user='root', passwd='root', db='xishiqu',charset='utf8')
                cur = conn.cursor()
                for i in list(range(0,len(newPriceR))):
                    facePrice = newPriceR[i]
                    tckType =tck_type[i]
                    if not isPass:
                        remark = price_status[i]
                        pkgTcks = getVolume(remark)
                # for facePrice in newPriceR:
                    cur.execute("insert into mp3_price_section_detail(regionName,pid,facePrice,seatNums,remark,isPass,fromTime,toTime,tckType,pkgTcks) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",('',getSeidByTitle(sid,eventTitle),facePrice,0,remark,isPass,fromTime,toTime,tckType,pkgTcks))
            except Exception as err:
                qlog.logger.error('[error] 11')
                qlog.logger.error("insert into mp3_price_section_detail(regionName,pid,facePrice,seatNums,remark,isPass,fromTime,toTime,tckType,pkgTcks) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"%('',getSeidByTitle(sid,eventTitle),facePrice,0,remark,isPass,fromTime,toTime,tckType,pkgTcks))
                qlog.logger.error(err)
                urls_error.append(base_data)
                continue
            finally:
                cur.close()
                conn.commit()
                conn.close()
#END mp3_price_section_detail

#START mp3_spider_tickets
        qlog.qPrint("debug seid : %s "%seid)
        if seid:
            try:
                conn = pymysql.connect(host='localhost', user='root', passwd='root', db='xishiqu',charset='utf8')
                cur = conn.cursor()
                for i in list(range(0,len(statuses))):
                    cur.execute("update mp3_spider_tickets set fromUrl=%s,updateTime=%s,saleStatus=%s where seid=%s and facePrice=%s",(fromUrl,getNow(),statuses[i],seid,newPriceR[i]))
            except Exception as err:
                qlog.logger.error('[error] 12')
                qlog.logger.error("sql : \t update mp3_spider_tickets set fromUrl=%s,updateTime=%s,saleStatus=%s where seid=%s and facePrice=%s"%(fromUrl,getNow(),statuses[i],seid,newPriceR[i]))
                qlog.logger.error(err)
                urls_error.append(base_data)
                continue
            finally:
                cur.close()
                conn.commit()
                conn.close()
        else:
            try:
                conn = pymysql.connect(host='localhost', user='root', passwd='root', db='xishiqu',charset='utf8')
                cur = conn.cursor()
                for i in list(range(0,len(statuses))):
                    cur.execute("insert into mp3_spider_tickets(seid,fromUrl,facePrice,updateTime,saleStatus) values(%s,%s,%s,%s,%s)",(getSeidByTitle(sid,eventTitle),fromUrl,newPriceR[i],getNow(),statuses[i]))
            except Exception as err:
                qlog.logger.error('[error] 13')
                qlog.logger.error("sql : \t insert into mp3_spider_tickets(seid,fromUrl,facePrice,updateTime,saleStatus) values(%s,%s,%s,%s,%s)"%(getSeidByTitle(sid,eventTitle),fromUrl,newPriceR[i],getNow(),statuses[i]))
                qlog.logger.error(err)
                urls_error.append(base_data)
                continue
            finally:
                cur.close()
                conn.commit()
                conn.close()
#END mp3_spider_tickets
    qlog.qPrint("\n")
if 'sum_limit' in vars() and len(urls_error)==0:
    qlog.qPrint("总共抓取%s个页面"%sum_limit)
else:
    qlog.qPrint("总共抓取%s个页面"%len(urls))
qlog.qPrint("错误页面%s个"%len(urls_error))
if len(urls_error)>0:
    urls=urls_error
    urls_error=[]
    for i in urls:
        # uncomment to enable the limitation
        # start of the limitation block
        # if limit>0:
        #     limit-=1
        # else:
        #     break
        #end of the limitation block

        #basic data 
        #   e.g.   
        #   (65583, '655', '话剧《钱多多嫁人记》', '上海话剧艺术中心-艺术剧院', '2014.08.12-2014.08.24', '话剧歌剧', 2, '售票中', None)
        base_data = i
        pid=0

        #for counting the act
        index = urls.index(i)+1
        qlog.qPrint("[演出] %d"%index)

        #url of the act page 
        #   e.g.   'http://item.damai.cn/64691.html'
        projectId = i[0]

        #url of the act
        #   e.g.
        url ="http://item.damai.cn/%s.html"%projectId
        qlog.qPrint("%s\n"%url)
        imgId = i[1]

        #url of the img
        actImgUrl = "http://pimg.damai.cn/perform/project/%s/%s_n.jpg"%(imgId,projectId)
        qlog.qPrint("----------------------------------")
        qlog.qPrint("actImgUrl : %s"%actImgUrl)
        qlog.qPrint("----------------------------------")

        #request for the page content
        try:
            f = urllib.request.urlopen(url,timeout=10).read().decode('utf-8')
        except Exception as err:
            qlog.logger.error("[error] qin : %s"%err)
            qlog.logger.error(url)
            urls_error.append(base_data)
            continue
        soup = BeautifulSoup(f)
        #演出名称
        actName = i[2]
        qlog.qPrint("----------------------------------")
        try: 
            qlog.qPrint("actName : %s"%actName)
        except UnicodeEncodeError as err:
            qlog.qPrint(err)
        qlog.qPrint("----------------------------------")
        #场馆名称
        if i[3]:
            veName = i[3]
        else:
            veName = '待定'
        qlog.qPrint("veName : %s"%veName)
        #场馆ID
        veid=getVeid(veName)
        qlog.qPrint(veid)

        #演出页面
        fromUrl = url
        #演出页面
        currentUrl =url
        #演出时间
        actTime =i[4]
        qlog.qPrint("----------------------------------")
        qlog.qPrint("actTime:%s"%actTime)
        qlog.qPrint("----------------------------------")
        #演出类别
        catName = i[5]
        qlog.qPrint("catName:%s"%catName)
        #演出状态
        status = 1

        #默认状态值
        editStatus= 0
        addStatus=0
        hasSeatMap=0
        isSeatable=0
        #销售状态
        saleStatus=getSalestatus(i[6])
        qlog.qPrint("---------------------------------")
        qlog.qPrint("saleStatus：%s"%saleStatus)
        qlog.qPrint("---------------------------------")
        #销售状态名
        saleName=i[7]
        qlog.qPrint("---------------------------------")
        qlog.qPrint("saleName：%s"%saleName)
        qlog.qPrint("---------------------------------")
        #获取sid
        sid=getSid(siteUrl,actName,fromUrl)
        qlog.qPrint('---------------------------')
        qlog.qPrint("sid: %s"%sid)
        qlog.qPrint('---------------------------')
#START mp3_spider_activity
        if sid:
            try:
                conn = pymysql.connect(host='localhost', user='root', passwd='root', db='xishiqu',charset='utf8')
                cur = conn.cursor()
                cur.execute("update mp3_spider_activity set veid=%s,actTime=%s,catName=%s,actImgUrl=%s,currentUrl=%s,status=%s,editStatus=%s,addStatus=%s,hasSeatMap=%s,isSeatable=%s,saleStatus=%s,veName=%s,saleName=%s,createDT=%s where sid=%s;",(veid,actTime,catName,actImgUrl,currentUrl,status,editStatus,addStatus,hasSeatMap,isSeatable,saleStatus,veName,saleName,getNow(),sid))
            except Exception as err:
                qlog.logger.error('[error] 1')
                qlog.logger.error("sql : \t update mp3_spider_activity set veid=%d,actTime=%s,catName='%s',actImgUrl='%s',currentUrl='%s',status=%d,editStatus=%d,addStatus=%d,hasSeatMap=%d,isSeatable=%d,saleStatus=%d,veName='%s',saleName='%s',createDT='%s' where sid=%d;"%(veid,actTime,catName,actImgUrl,currentUrl,status,editStatus,addStatus,hasSeatMap,isSeatable,saleStatus,veName,saleName,getNow(),sid))
                qlog.logger.error(err)
                urls_error.append(base_data)
                continue
            finally:
                cur.close()
                conn.commit()
                conn.close()            
        else:
            try:
                conn = pymysql.connect(host='localhost', user='root', passwd='root', db='xishiqu',charset='utf8')
                cur = conn.cursor()
                cur.execute("insert into mp3_spider_activity(siteUrl,veid,actTime,actName,catName,actImgUrl,fromUrl,currentUrl,status,editStatus,addStatus,hasSeatMap,isSeatable,saleStatus,veName,saleName,createDT) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);",(siteUrl,veid,actTime,actName,catName,actImgUrl,fromUrl,currentUrl,status,editStatus,addStatus,hasSeatMap,isSeatable,saleStatus,veName,saleName,getNow()))
                
            except Exception as err:
                qlog.logger.error('[error] 2')
                qlog.logger.error("sql : \t insert into mp3_spider_activity(siteUrl,veid,actTime,actName,catName,actImgUrl,fromUrl,currentUrl,status,editStatus,addStatus,hasSeatMap,isSeatable,saleStatus,veName,saleName,createDT) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"%(siteUrl,veid,actTime,actName,catName,actImgUrl,fromUrl,currentUrl,status,editStatus,addStatus,hasSeatMap,isSeatable,saleStatus,veName,saleName,getNow()))
                qlog.logger.error(err)
                urls_error.append(base_data)
                continue
            finally:
                cur.close()
                conn.commit()
                conn.close()
                #在插入活动以后更新当前sid的值
                sid = getSid(siteUrl,actName,fromUrl)

        qlog.qPrint('---------------------------')
        qlog.qPrint("new sid: %s"%sid)
        qlog.qPrint('---------------------------')
#END mp3_spider_activity
        if soup.find(id="perform"):
            perfs = soup.find(id="perform").find_all('a')
        else:
            qlog.qPrint("这场演出[待定]")
            continue

        for i in perfs:        
            #价格区域 
            #    e.g. 80-150-200-
            priceRegion=''
            index= perfs.index(i)
            #时间序列号(1,2,3,4,5)
            eventSeries = index+1
            #用空格替换\xa0
            eventTitle = i.text.replace(u'\xa0',u' ')
            #事件事件
            #e.g.   2014-7-11 19:30:00
            eventDatetime = i.get('time')
            qlog.qPrint("eventTitle:%s"%eventTitle)
            qlog.qPrint("No: %s"%eventSeries)

            #是否售完
            isSaleOut = 0

            #获取演出描述
            remark =i.get("pfdes")

            #是否通票
            isPass = isPassEvent(eventTitle)

            #通票|套票  默认值
            tckType = 0
            pkgTcks =0
            fromTime = '0000-00-00 00:00:00'
            toTime = '0000-00-00 00:00:00'
            qlog.qPrint("演出编号[pid] : %s 时间[time] %s \n"%(i.get('pid'),i.get('time')))

#START OF THE XHR
            values = {"type":33,"performID":i.get('pid'),"business":soup.find(id="Business").get('value'),"IsBuyFlow":soup.find(id="IsBuyFlow").get('value'),"sitestaus":soup.find(id="hidSiteStatus").get('value')}
            the_page=getAjax("http://item.damai.cn/ajax.aspx",values)
            if the_page == None:
                urls_error.append(base_data)
                continue;
#END OF THE XHR

    #parsing the returned data to get the prices under the current event 
            if len(the_page.split('^'))>=2 and len(BeautifulSoup(the_page.split('^')[1]).find_all('a'))>1:
                price = BeautifulSoup(the_page.split('^')[1]).find_all('a')
            else:
                price = soup.find(id="price").find_all('a')

            qlog.qPrint("价格:")
            #count the prices for recording the sale status
            cnt = 0
            #the saleStatuses for each price
            #   e.g     1 =>    销售中
            #           2 =>    已售完
            statuses =[]
            #remarks for special tickets LIKE package tcks | pass tcks | 100(东门) not pck not pass but with description
            price_status = []
            tck_type =[]
            for i in price:
                if('grey' in i['class']):
                    qlog.qPrint("%s \t %s"%(i.get('price'),'已卖完'))
                    cnt+=1
                    statuses.append(2)
                else:
                    qlog.qPrint("%s \t %s"%(i.get('price'),'销售中'))
                    statuses.append(1)
                # 't'=1                     => 套票
                # 't'=0                     => 普通
                # 't'=0 and 'n' != 'price'  => 非通票非套票，但存在描述性语句
                if i.get("t")=="1":
                    price_status.append(i.get("n"))
                    tck_type.append(1)
                elif i.get("n") !=i.get("price"):
                    price_status.append(i.get("n"))
                    tck_type.append(0)
                else:
                    price_status.append("")
                    tck_type.append(0)
                #making the priceRegion variable
                priceRegion+=(i.get('price')+'-')
            qlog.qPrint("价格区域:%s"%priceRegion)

            # if cnt = the len of the prices
            # set the isSaleOut => 1 
            if cnt == len(price):
                isSaleOut = 1

            if isSaleOut:
                qlog.qPrint("[已售空]")
            else:
                qlog.qPrint("[销售中]")

#START mp3_spider_event 
            #获取seid
            #存在         => 更新
            #不存在       => 插入
            seid = getSeidByTitle(sid,eventTitle)
            qlog.qPrint('-----------------------------')
            qlog.qPrint("seid: %s"%seid)
            qlog.qPrint('-----------------------------')
            if seid:
                try:
                    conn = pymysql.connect(host='localhost', user='root', passwd='root', db='xishiqu',charset='utf8')
                    cur = conn.cursor()
                    cur.execute("update mp3_spider_event set eventSeries=%s,priceRegion=%s,eventTitle=%s,isSaleOut=%s where seid=%s",(eventSeries,priceRegion,eventTitle,isSaleOut,seid))
                except Exception as err:
                    qlog.logger.error('[error] 6') 
                    qlog.logger.error("sql : \t update mp3_spider_event set eventSeries=%s,priceRegion=%s,eventTitle=%s,isSaleOut=%s where seid=%s"%(eventSeries,priceRegion,eventTitle,isSaleOut,seid))               
                    qlog.logger.error(err)
                    urls_error.append(base_data)
                    continue
                finally:
                    cur.close()
                    conn.commit()
                    conn.close()                
            else:
                try:
                    conn = pymysql.connect(host='localhost', user='root', passwd='root', db='xishiqu',charset='utf8')
                    cur = conn.cursor()
                    cur.execute("insert into mp3_spider_event(sid,eventSeries,priceRegion,eventTitle,eventDatetime,isSaleOut) values(%s,%s,%s,%s,%s,%s)",(sid,eventSeries,priceRegion,eventTitle,eventDatetime,isSaleOut))
                except Exception as err:
                    qlog.logger.error('[error] 7')
                    qlog.logger.error("sql : \t insert into mp3_spider_event(sid,eventSeries,priceRegion,eventTitle,eventDatetime,isSaleOut) values(%s,%s,%s,%s,%s,%s)"%(sid,eventSeries,priceRegion,eventTitle,eventDatetime,isSaleOut))
                    qlog.logger.error(err)
                    urls_error.append(base_data)
                    continue
                finally:
                    cur.close()
                    conn.commit()
                    conn.close()
            qlog.qPrint('-----------------------------')
            qlog.qPrint("new seid: %s"%seid)
            qlog.qPrint('-----------------------------')
#END mp3_spider_event 

#START mp3_price_section
            #获取价格区域pid
            #存在         => 更新
            #不存在       => 插入
            pid =getPid(sid,priceRegion)
            qlog.qPrint("pid 1:%s"%pid)
            if pid:
                try:
                    conn = pymysql.connect(host='localhost', user='root', passwd='root', db='xishiqu',charset='utf8')
                    cur = conn.cursor()
                    cur.execute("update mp3_price_section set priceRegion=%s where pid=%s",(priceRegion,pid))
                    cur.execute("update mp3_spider_event set pid=%s where sid=%s and priceRegion=%s",(pid,sid,priceRegion))
                except Exception as err:
                    qlog.logger.error('[error] 9')
                    qlog.logger.error("sql : \t update mp3_price_section set priceRegion=%s where pid=%s"%(priceRegion,pid))
                    qlog.logger.error("sql : \t update mp3_spider_event set pid=%s where sid=%s and priceRegion=%s"%(pid,sid,priceRegion))
                    qlog.logger.error(err)
                    urls_error.append(base_data)
                    continue
                finally:
                    cur.close()
                    conn.commit()
                    conn.close()
            else:
                try:
                    conn = pymysql.connect(host='localhost', user='root', passwd='root', db='xishiqu',charset='utf8')
                    cur = conn.cursor()
                    cur.execute("insert into mp3_price_section(sid,priceRegion) values(%s,%s)",(sid,priceRegion))
                    pid =getPid(sid,priceRegion)
                    qlog.qPrint("pid 2:%s"%pid)
                    cur.execute("update mp3_spider_event set pid=%s where sid=%s and priceRegion=%s",(pid,sid,priceRegion))
                except Exception as err:
                    qlog.logger.error('[error] 10')
                    qlog.logger.error("sql : \t insert into mp3_price_section(sid,priceRegion) values(%s,%s)"%(sid,priceRegion))
                    qlog.logger.error("sql : \t update mp3_spider_event set pid=%s where sid=%s and priceRegion=%s"%(pid,sid,priceRegion))
                    qlog.logger.error(err)
                    urls_error.append(base_data)
                    continue
                finally:
                    cur.close()
                    conn.commit()
                    conn.close()
            #格式转换
            #100-180-280-380-  ==> 100.00-180.00-280.00-380.00
            newPriceR = priceRegion.rstrip('-').split('-')
            for i in list(range(0,len(newPriceR))):
                newPriceR[i]=newPriceR[i] + '.00'
            qlog.qPrint('-'.join(newPriceR))
#END mp3_price_section

#START mp3_price_section_detail
            if seid:
                # try:
                #     conn = pymysql.connect(host='localhost', user='root', passwd='root', db='xishiqu',charset='utf8')
                #     cur = conn.cursor()
                #     for facePrice in newPriceR:
                #         cur.execute("update mp3_price_section_detail set regionName=%s,facePrice=%s,seatNums=%s,remark=%s where pid = %s",('',facePrice,0,'',seid))
                # except Exception as err:
                #     qlog.qPrint('[error] 12')
                #     qlog.qPrint(err)
                # finally:
                #     cur.close()
                #     conn.commit()
                #     conn.close()
                #如果存在
                pass
            else:
                #如果不存在，插入到[mp3_price_section_detail]
                try:
                    conn = pymysql.connect(host='localhost', user='root', passwd='root', db='xishiqu',charset='utf8')
                    cur = conn.cursor()
                    for i in list(range(0,len(newPriceR))):
                        facePrice = newPriceR[i]
                        tckType =tck_type[i]
                        if not isPass:
                            remark = price_status[i]
                            pkgTcks = getVolume(remark)
                    # for facePrice in newPriceR:
                        cur.execute("insert into mp3_price_section_detail(regionName,pid,facePrice,seatNums,remark,isPass,fromTime,toTime,tckType,pkgTcks) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",('',getSeidByTitle(sid,eventTitle),facePrice,0,remark,isPass,fromTime,toTime,tckType,pkgTcks))
                except Exception as err:
                    qlog.logger.error('[error] 11')
                    qlog.logger.error("insert into mp3_price_section_detail(regionName,pid,facePrice,seatNums,remark,isPass,fromTime,toTime,tckType,pkgTcks) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"%('',getSeidByTitle(sid,eventTitle),facePrice,0,remark,isPass,fromTime,toTime,tckType,pkgTcks))
                    qlog.logger.error(err)
                    urls_error.append(base_data)
                    continue
                finally:
                    cur.close()
                    conn.commit()
                    conn.close()
#END mp3_price_section_detail

#START mp3_spider_tickets
            qlog.qPrint("debug seid : %s "%seid)
            if seid:
                try:
                    conn = pymysql.connect(host='localhost', user='root', passwd='root', db='xishiqu',charset='utf8')
                    cur = conn.cursor()
                    for i in list(range(0,len(statuses))):
                        cur.execute("update mp3_spider_tickets set fromUrl=%s,updateTime=%s,saleStatus=%s where seid=%s and facePrice=%s",(fromUrl,getNow(),statuses[i],seid,newPriceR[i]))
                except Exception as err:
                    qlog.logger.error('[error] 12')
                    qlog.logger.error("sql : \t update mp3_spider_tickets set fromUrl=%s,updateTime=%s,saleStatus=%s where seid=%s and facePrice=%s"%(fromUrl,getNow(),statuses[i],seid,newPriceR[i]))
                    qlog.logger.error(err)
                    urls_error.append(base_data)
                    continue
                finally:
                    cur.close()
                    conn.commit()
                    conn.close()
            else:
                try:
                    conn = pymysql.connect(host='localhost', user='root', passwd='root', db='xishiqu',charset='utf8')
                    cur = conn.cursor()
                    for i in list(range(0,len(statuses))):
                        cur.execute("insert into mp3_spider_tickets(seid,fromUrl,facePrice,updateTime,saleStatus) values(%s,%s,%s,%s,%s)",(getSeidByTitle(sid,eventTitle),fromUrl,newPriceR[i],getNow(),statuses[i]))
                except Exception as err:
                    qlog.logger.error('[error] 13')
                    qlog.logger.error("sql : \t insert into mp3_spider_tickets(seid,fromUrl,facePrice,updateTime,saleStatus) values(%s,%s,%s,%s,%s)"%(getSeidByTitle(sid,eventTitle),fromUrl,newPriceR[i],getNow(),statuses[i]))
                    qlog.logger.error(err)
                    urls_error.append(base_data)
                    continue
                finally:
                    cur.close()
                    conn.commit()
                    conn.close()
#END mp3_spider_tickets
        qlog.qPrint("\n")
    qlog.qPrint("总共抓取%s个页面"%len(urls))
    qlog.qPrint("错误页面%s个"%len(urls_error))
    for i in urls_error:
        qlog.qPrint(i)
qlog.qPrint("---------------------------------")
qlog.qPrint("END time : %s "%getNow())
qlog.qPrint("---------------------------------")
#END OF THE SPIDER