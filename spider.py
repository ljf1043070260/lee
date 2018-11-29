from urllib import request
from urllib import error
import re
import my_sql

class Spider():
    page = 0
    count = 1
    url = "https://book.douban.com/top250?start=0"

    root_pattern = '<td valign="top">([\s\S]*?)</td>'
    
    book_pattern = r'<a href="(.*?)".*?>'
    book_root_pattern = r'<div id="info".*?>(.*?)</div>'
    book_title_pattern = r'<div id="wrapper">.*?<span property="v:itemreviewed">(.*?)</span>'
    book_name_pattern = r'作者:?</span>.*?<a .*?>(.*?)</a>'
    book_publishing_company_pattern = r'出版社:?</span>(.*?)<br/>'
    book_produces_pattern = r'出品方:?</span>.*?<a .*?>(.*?)</a>'
    book_original_name_pattern = r'原作名:?</span>(.*?)<br/>'
    book_translator_pattern = r'译者:?</span>.*?<a .*?>(.*?)</a>'
    book_year_pattern = r'出版年:</span>(.*?)<br/>'
    book_pages_pattern = r'页数:</span>(.*?)<br/>'
    book_money_pattern = r'定价:</span>(.*?)<br/>'
    book_bindings_pattern = r'装帧:</span>(.*?)<br/>' 
    book_series_pattern = r'丛书:</span>.*?<a .*?>(.*?)</a>'
    book_isbn_pattern = r'ISBN:</span>(.*?)<br/>'
    book_intro_pattern = r'<div class="intro">(.*?)</div>'


    
    @classmethod
    def change(self):
        url = "https://book.douban.com/top250"
        text = '?start='+str(Spider.page*25)
        url += text
        Spider.url = url
        Spider.page += 1

    def __fetch_contect(self,url):
        try:
            r = request.urlopen(url)
            htmls = r.read()
            htmls = str(htmls,encoding="utf-8")
            return htmls
        except error.URLError as e:
            print(e)
            
        
        
    
    def __analysis_books_url(self,htmls):
        root_html = re.findall(Spider.root_pattern,htmls)
        books_pattern = []
        for html in root_html:
            book_pattern = re.findall(Spider.book_pattern,html,re.S)[0]
            books_pattern.append(book_pattern)
        #print(books_pattern)
        return books_pattern
    
    def judge(self,book_info,book_info_pattern,book_html):
        if len(book_info) == 0:
            book_info = "无"
        else:
            book_info = re.findall(book_info_pattern,book_html[0],re.S)[0].strip()
        return book_info    

    def __analysis_bookinfo(self,htmls):
        book_html = re.findall(Spider.book_root_pattern,htmls,re.S)
        book_title = re.findall(Spider.book_title_pattern,htmls,re.S)[0]
        book_intro = re.findall(Spider.book_intro_pattern,htmls,re.S)[0]
        book_intro = re.sub('[(<p>),(</p>),(href="javascrit:void0" class="j a_show_full),(展开全部)]','', book_intro)
        book_info = []
        book_name = re.findall(Spider.book_name_pattern,book_html[0],re.S)[0]
        book_name = re.sub('[\s]','', book_name)
        book_publishing_company = re.findall(Spider.book_publishing_company_pattern,book_html[0],re.S)[0]
        book_publishing_company = re.sub('[\s]','', book_publishing_company)
        
        book_produces = re.findall(Spider.book_produces_pattern,book_html[0],re.S)
        book_produces = self.judge(book_produces,Spider.book_produces_pattern,book_html)

        book_original_name = re.findall(Spider.book_original_name_pattern,book_html[0],re.S)
        book_original_name = self.judge(book_original_name,Spider.book_original_name_pattern,book_html)

        book_translator = re.findall(Spider.book_translator_pattern,book_html[0],re.S)
        book_translator = self.judge(book_translator,Spider.book_translator_pattern,book_html)
        
        book_year = re.findall(Spider.book_year_pattern,book_html[0],re.S)[0].strip()
        book_pages = re.findall(Spider.book_pages_pattern,book_html[0],re.S)[0].strip()
        book_money = re.findall(Spider.book_money_pattern,book_html[0],re.S)[0].strip()
        book_bindings = re.findall(Spider.book_bindings_pattern,book_html[0],re.S)[0].strip()
        book_series = re.findall(Spider.book_series_pattern,book_html[0],re.S)
        book_series = self.judge(book_series,Spider.book_series_pattern,book_html)
        book_isbn = re.findall(Spider.book_isbn_pattern,book_html[0],re.S)
        book_isbn = self.judge(book_isbn,Spider.book_isbn_pattern,book_html)


        book = (book_name,book_publishing_company,book_produces,book_original_name,book_translator,book_year,book_pages,
        book_money,book_bindings,book_series,book_isbn,book_title,book_intro)
        book_info.append(book)
        #print(book_info)
        return(book_info)



    def __show(self,book_info):
        name = book_info[0][0]
        publishing_company = book_info[0][1]
        produces = book_info[0][2]
        original_name = book_info[0][3]
        translator = book_info[0][4]
        year = book_info[0][5]
        pages = book_info[0][6]
        money = book_info[0][7]
        bindings = book_info[0][8]
        series = book_info[0][9]
        isbn = book_info[0][10]
        title = book_info[0][11]
        intro = book_info[0][12]
        print("----------------------------------------------------------------------------")
        print("第" + str(Spider.count) + "名")
        print("书名："+title)
        print("作者：" + name)
        print("出版社：" + publishing_company)
        print("出品方：" + produces)
        print("原作名：" + str(original_name))
        print("译者：" + translator)
        print("出版年：" + year) 
        print("页数：" + pages)
        print("定价：" + money) 
        print("装帧：" + bindings)
        print("丛书：" + series)
        print("ISBN：" + isbn)
        print("简介：" + intro)    
        Spider.count += 1   
        
        
    def go(self):
        my_sql.create_database()
        for i in range(0,10):
            Spider.change()
            root_html = self.__fetch_contect(Spider.url)
            books_url = self.__analysis_books_url(root_html)
            for j in range(0,25):
                book_html = self.__fetch_contect(books_url[j])
                if book_html == None:
                    continue
                book_info = self.__analysis_bookinfo(book_html)
                my_sql.insert_database(Spider.count,book_info)
                Spider.count += 1
s = Spider()

s.go()
my_sql.close_databse()






        
    
