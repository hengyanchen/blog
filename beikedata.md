# 贝壳找房成都小区、租房、二手房信息爬取及数据分析



## 1. scrapy爬取数据写入MySQL



* 爬虫文件

```python
# -*- coding:utf-8 -*-
import scrapy
import parsel
from beikespider.items import BeikespiderItem
import re
import ast


buildtim = re.compile('([0-9]{4})年建成')
findyear = re.compile('单价(.*)元/平米')

class BeikeSpider(scrapy.Spider):
    name = 'beike'
    allowed_domains = ['cd.ke.com','cd.zu.ke.com']
    start_urls = ['http://cd.ke.com/xiaoqu/']

    def parse(self, response):
        '''
        解析得到大成都24个区包含的小区链接并且将地区信息传入后续解析函数
        '''

        items = BeikespiderItem()
        area_href = response.xpath('//div[@data-role="ershoufang"]/div/a')
        for i in area_href:
            area_list = ''.join(i.xpath('./text()').extract())
            url = response.urljoin(''.join(i.xpath('./@href').extract()))
            yield scrapy.Request(url,callback=self.parsexiaoqu,meta={'area_list':area_list})



    def parsexiaoqu(self,response):
        '''
        解析得到小区列表页并将每个小区的所属区域继续传入后续解析函数
        '''
        area_list = response.meta['area_list']
        totalpage = ''.join(response.xpath('//div[@class="page-box house-lst-page-box"]/@page-data').extract())
        u = ''.join(response.xpath('//div[@class="page-box house-lst-page-box"]//@page-url').extract())
        tpage = ast.literal_eval(totalpage)["totalPage"]
        for i in range(1,tpage+1):
            yield scrapy.Request(response.urljoin(u.format(page=i)),callback=self.parsexiaoqu2,meta={'area_list':area_list})


    def parsexiaoqu2(self,response):
        '''
        解析得到二手房小区部分基本信息、每条二手房房源信息链接、每条租房房源信息
        '''

        items = BeikespiderItem()
        infolist = parsel.Selector(response.text).xpath(
            '//li[@class="clear xiaoquListItem CLICKDATA"]')

        for i in infolist:

            district = response.meta['area_list']
            xiaoqu_name = ''.join(i.xpath('.//div[@class=\'title\']/a/text()').extract())
            bizcircle = ''.join(i.xpath('.//div[@class="positionInfo"]/a[@class="bizcircle"]/text()').extract())
            close_subway = ''.join(i.xpath('.//div[@class="tagList"]/span/text()').extract())
            b = ''.join(i.xpath('.//div[@class="positionInfo"]/text()').extract())
            buildtime = ''.join(re.findall(buildtim, b))

            houseurl = ''.join(i.xpath('.//div[@class ="xiaoquListItemSellCount"]/a/@ href').extract())

            xiaoquurl = ''.join(i.xpath('.//div[@class=\'title\']/a/@href').extract())
            yield scrapy.Request(xiaoquurl,callback=self.parseershoufang,meta={'xiaoqu_name':xiaoqu_name,'district':district,'bizcircle':bizcircle,'close_subway':close_subway,'buildtime': buildtime,'houseurl':houseurl})

            zfurl =''.join(i.xpath('.//div[@class=\'houseInfo\']/a/@href').extract())
            if zfurl:
                yield scrapy.Request(zfurl,callback=self.getzufangnextpage,meta={'xiaoqu_name':xiaoqu_name})


            x = int(''.join(i.xpath('.//div[@class ="xiaoquListItemSellCount"]/a/span/text()').extract()))
            if x != 0:
                yield  scrapy.Request(houseurl,callback=self.gethousenetpage,meta={'xiaoqu_name':xiaoqu_name})



    def parseershoufang(self,response):
        '''
        解析小区基本信息
        '''

        items = BeikespiderItem()
        xiaoquelse = parsel.Selector(response.text).xpath('//div[@class="detailHeader VIEWDATA"]')
        for i in xiaoquelse:
            items['xq_info'] = ''.join(response.xpath('//span[@class="xiaoquInfoContent"]/text()').extract()).replace(' ','').replace('\n', '').replace('\r', '')
            items['xiaoqu_name'] = response.meta['xiaoqu_name']
            items['district'] = response.meta['district']
            items['bizcircle'] = response.meta['bizcircle']
            items['close_subway'] = response.meta['close_subway']
            items['buildtime'] = response.meta['buildtime']
            items['houseurl'] = response.meta['houseurl']
            items['positioninfo'] = ''.join(i.xpath('.//div[@class="sub"]/text()').extract()).replace(' ','').replace('\n','').replace('\r','')
            items['attention'] = int(''.join(i.xpath('.//div[@class="action"]/span[@id="favCount"]/text()').extract()))
            yield items

    def getzufangnextpage(self,response):
        xiaoqu_name= response.meta['xiaoqu_name']
        urlhref = ''.join(response.xpath('//div[@class="content__pg"]/@data-url').extract())
        totalpage = ''.join(response.xpath('//div[@class="content__pg"]/@data-totalpage').extract())


        for i in range(1,int(totalpage)+1):
            url = response.urljoin(urlhref.format(page=i))
            yield scrapy.Request(url,callback=self.parsezufang,meta={'xiaoqu_name':xiaoqu_name})

    def parsezufang(self,response):
        '''
        解析租房信息
        '''
        print('租房列表')
        items = BeikespiderItem()
        zufanglist = response.xpath('//div[@class="content__list"][1]/div[@class="content__list--item"]')
        for i in zufanglist:
            items['xiaoqu_name'] = response.meta['xiaoqu_name']
            items['zufang_price'] = ''.join(i.xpath('.//span[@class="content__list--item-price"]/em/text()').extract())
            items['zufang_info'] = ''.join(i.xpath('.//p[@class="content__list--item--des"]/text()').extract()).replace('\n','').replace(' ','').replace('-','').replace(',','')
            items['zhongjie'] = ''.join(i.xpath('.//span[@class="brand"]/text()').extract()).replace(' ','').replace('\n','')
            yield items


    def gethousenetpage(self,response):
        xiaoqu_name = response.meta['xiaoqu_name']

        href = ''.join(response.xpath('//div[@class ="page-box house-lst-page-box"]/@page-url').extract())
        totalpage = ''.join(response.xpath('//div[@class ="page-box house-lst-page-box"]/@page-data').extract())
        totalpage = ast.literal_eval(totalpage)["totalPage"]
        for i in range(1,totalpage+1):

            url = response.urljoin(href.format(page=i))
            yield scrapy.Request(url, callback=self.parsehouse,meta={'xiaoqu_name':xiaoqu_name})

    def parsehouse(self, response):
        '''
        解析每一个二手房信息
        '''
        # 每个小区二手房房源列表页的小区信息，均价、季度成交量、月带看量；混杂在一起还需要正则处理

        # 每个小区二手房房源列表页
        houseinfo = response.xpath('//div[@data-component="list"]//li[@class="clear"]')
        items = BeikespiderItem()
        for i in houseinfo:
            items['xiaoqu_name'] = response.meta['xiaoqu_name']
            totalprice =  ''.join(i.xpath('.//div[@class="totalPrice"]/span/text()').extract()).replace(' ','').replace('\n','')
            if type(totalprice)== int:
                items['totalprice'] = int(totalprice)
            else:
                items['totalprice'] = float(totalprice)
            unitprice = ''.join(i.xpath('.//div[@class="unitPrice"]/span/text()').extract())
            items['unitprice'] = ''.join(re.findall(findyear,unitprice))
            items['hs_info'] = ''.join(i.xpath('.//div[@class="houseInfo"]/text()').extract()).replace(' ','').replace('\n','')
            items['follow_info'] = ''.join(i.xpath('.//div[@class="followInfo"]/text()').extract()).replace(' ','').replace('\n','')
            x = ''.join(i.xpath('.//span[@class="taxfree"]/text()').extract())
            items['trasfree'] = 1 if x == '满五年' else x==0
            yield items

```

* pipelines部分

  ```python
  # Define your item pipelines here
  #
  # Don't forget to add your pipeline to the ITEM_PIPELINES setting
  # See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
  
  
  # useful for handling different item types with a single interface
  from itemadapter import ItemAdapter
  from pymysql import *
  
  class BeikespiderPipeline:
      def process_item(self, item, spider):
  
          conn = connect(host='localhost',port=3306,user='root',password='root',database='spiderdata',charset='utf8')
          cursor = conn.cursor()
          if 'bizcircle' in item:
              try:
                  cursor.execute('insert into beikedata_community1(id,name,subway,buildtime,position,attention,area,info,bizcircle,houseurl) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',(0,item['xiaoqu_name'],item['close_subway'],item['buildtime'],item['positioninfo'],item['attention'],item['district'],item[ 'xq_info'],item['bizcircle'],item['houseurl']))
  
              except Exception as e:
                  print(e)
                  conn.rollback()
              else:
                  print(222222222222222222222222222222222)
                  conn.commit()
          if 'zhongjie' in item:
              try:
                  cursor.execute(
                      'insert into beikedata_zufang1(id,community,intermediary,unit_type,price) values(%s,%s,%s,%s,%s)',
                      (0, item['xiaoqu_name'], item['zhongjie'], item['zufang_info'], int(item['zufang_price'])))
  
              except Exception as e:
                  print(e)
                  conn.rollback()
              else:
                  print(222222222222222222222222222222222)
                  conn.commit()
  
          if 'follow_info' in item:
              try:
  
                  cursor.execute(
                      'insert into beikedata_secondhandhousing1(id,follow,house_info,totalprice,trasfree,unitprice,community) values(%s,%s,%s,%s,%s,%s,%s)',
                      (0, item['follow_info'], item['hs_info'], (item['totalprice']), item['trasfree'], item['unitprice'],
                       item['xiaoqu_name']))
              except Exception as e:
                  print(e)
                  conn.rollback()
              else:
                  print(222222222222222222222222222222222)
                  conn.commit()
  
          cursor.close()
          conn.close()
          return item
  ```

  ### 1.1 目标数据及异常处理

  #### 1.11 目标数据

  大成都24个区域的小区基本信息、租房信息、二手房信息

  三个板块存入MySQL三张表并且要带有关联查询需要的字段：小区名字

  #### 1.12异常处理及技术总结

  一、租房板块的链接被scrapy屏蔽；

  检查后发现租房板块的链接不属于cd.ke.com这个网址子网址，将cd.zu.ke.com加入scrapy限定的网址列表

  ```
  allowed_domains = ['cd.ke.com','cd.zu.ke.com']
  ```

  二、不同页面的数据使用了不同层次的解析函数，但是有一部分我要存入同一个表中，并且还要保证它们的同步性；scrapy.Request方法里有一个参数meta可以来做这事，meta是一个字典类型，可以在scrapy.Request方法中将需要传入下一个解析函数的数据封装到meta中以此达到目的。

  三、有很多小区0条二手房信息，但是指向一个100页二手房信息页面的链接，如果不做判断将会爬取很多重复数据。

  四、scrapy返回到pipelines的是一个数据流，做一个简单的判断就能正确的把不同的items存入不同的mysql表里。

  五、经过多次调试，这个代码可以达到爬取目的，数据正常，数量基本符合贝壳网的数目，存入16812条小区信息、39672条租房信息和129124条二手房数据。

  

  

  


## 2. 数据基本处理

### 2.1 正则提取把租房信息表朝向、面积、户型重写

```python
# -*- coding:utf-8 -*-
from pymysql import *
import json
import re
# df1 = pd.DataFrame([[1,2,3],[4,5,6],[7,8,9]],index=['row1','row2','row3'],columns=['col1','col2','col'])
conn = connect(host='localhost',port=3306,user='root',password='root',database='spiderdata',charset='utf8')

try:
    with conn.cursor() as cur:
        for i in range(327,39999):
            cur.execute('select * from beikedata_zufang1 where id=%s',i)
            res = cur.fetchall()

            findarea = re.findall('(\d*)㎡', res[0][4])
            findtowards = re.findall('\d*㎡(.*?)\d{1}', res[0][4])
            findunittype = re.findall('\d*㎡.*?(\d{1}.*)', res[0][4])

            cur.execute('update beikedata_zufang1 set towards=%s, area=%s, unit_type=%s where id=%s',(''.join(findtowards),findarea[0],''.join( findunittype),i))
except Exception as e:
    print(e)
    conn.rollback()
finally:
    conn.commit()
    conn.close()
```



### 2.2 正则提取把小区信息物业费、建筑类型、户数数据提取结构化



### 2.3 正则提取把二手房数据结构化



##  3. 数据分析

### 3.1 数据概览

##  4. 数据可视化

[成都热点楼盘词云图（基于楼盘关注人数）](https://hengyanchen.github.io/blog/ciyun)

