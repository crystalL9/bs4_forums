import bs4
from datetime import datetime
from queue import Queue
from Browser import GET_HTML_REQUEST
import time
import re
import pytz
import datetime as dt
class BS4_OTOFUN :
    def __init__(self):
        self.link_queue = Queue()
        self.domain = 'https://www.otofun.net'

    def get_link(self, url):
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        midnight_timestamp = int(today.timestamp())
        link_crawled = []
        next_link = 'true'
        while next_link != None:
            req = GET_HTML_REQUEST(url =  url)
            response_html = req.get_html()
            soup = bs4.BeautifulSoup(response_html, 'html.parser')
            try:
                next_link_element = soup.find('a', class_='pageNav-jump pageNav-jump--next')
                next_link = next_link_element.get('href')
            except:
                next_link = None
                pass
            thread_list_div = soup.find('div', class_='structItemContainer-group js-threadList')
            if thread_list_div:
                level1_divs = thread_list_div.find_all('div', recursive=False)
                for article in level1_divs : 
                    try:
    
                        date_element = article.find('time')
                        if not date_element:
                            continue
                        article_time = int(date_element['data-time'])
                        if article_time < midnight_timestamp:
                            return

                        title_div = article.select_one('div.structItem-title')
                        link_element = title_div.select('a')
                        href = link_element[-1]['href']
                        if not href.startswith('/'):
                            href = '/' + href  
                        try: 
                            view_count_element = article.find('div', class_='structItem-cell structItem-cell--meta')
                            view_count_text = view_count_element.text.strip().split('\n')
                            view_count = self.convert_unit_to_num(view_count_text[1])
                            comment_count = self.convert_unit_to_num(view_count_text[5])
                        except:
                            view_count = 0
                            comment_count = 0
                            pass
                        if href not in link_crawled :
                            print(f'---------->>>>>>>>> Put {href} to Queue')
                            self.link_queue.put(f'{self.domain + href}|{view_count}|{comment_count}')
                    except:
                        continue

    def get_reactions(self,url):
        list_like = []
        req = GET_HTML_REQUEST(url =  url)
        response_html = req.get_html()
        soup = bs4.BeautifulSoup(response_html, 'html.parser')
        reaction_row = soup.select('li.block-row.block-row--separated')
        for r in reaction_row:
            data = {}
            # Lấy avatar, link, id, name
            author_information = r.select_one('a.avatar.avatar--s')
            id_user = author_information['data-user-id']
            author_link = self.domain + author_information['href']
            try:
                avatar = self.domain + author_information.select_one('img')['src']
            except:
                avatar = ''
                pass
            author = r.select_one('a.username').get_text()
            # Lấy role và thông tin location
            role = r.select_one('span.userTitle').get_text()
            try:
                presentation = r.select_one('a.u-concealed')
            except:
                presentation = ''
                pass
            try:
                location_link = self.domain +  presentation['href']
            except:
                location_link = ''
                pass
            try:
                location = presentation.get_text()
            except:
                location = ''
                pass
            # Lấy [messages, reaction score, points]
            minor = r.select('dd')
            arr_num=[]
            for m in minor:
                arr_num.append(int(m.get_text().replace(',','')))
            if len(arr_num) < 3:
                arr_num.append(0)
            # Lấy thời gian thả cảm xúc
            reacted_time = int(r.select_one('time.u-dt')['data-time'])
            
            # DATA
            data['id_user'] = id_user
            data['author'] = author
            data['author_link'] = author_link
            data['avatar'] = avatar
            data['role'] = role
            data['location'] = location
            data['link_location'] = location_link
            data['messages'] = arr_num[0]
            data['reactions_points'] = arr_num[1]
            data['points'] = arr_num[2]
            data['reacted_time'] = reacted_time
            list_like.append(data)
        return list_like

    def crawler(self):
        while not self.link_queue.empty():
            try:
                combine_link = self.link_queue.get()
                split_link = str(combine_link).split('|')
                link = split_link[0]
                orginal_link = link.replace(self.domain,'')
                comments = split_link[1]
                views = split_link[2]
                next_page = 'true'
                stt = 0
                while next_page != None:
                    req = GET_HTML_REQUEST(url =  link)
                    response_html = req.get_html()
                    soup = bs4.BeautifulSoup(response_html, 'html.parser')
                    
                    title = soup.select(".p-title-value")[0].get_text()
                    articles = soup.find_all('article', class_=['message', 'message--post', 'js-post', 'js-inlineModContainer'])
                    source_post_id = articles[0]['id'] 
                    try:
                        next_page = self.domain + soup.select_one('a.pageNav-jump.pageNav-jump--next')['href']
                        link = next_page
                    except:
                        next_page = None
                    for a in articles:
                        try:
                            post = self.extract_data(article= a ,stt=stt, comments=comments, source_post_id= source_post_id, title=title, views=views)
                            print(post)
                            # push_kafka([PostForumz(**post)])  
                        except Exception as e:
                            print(e)
                            pass
                        stt += 1

                with open('link.txt','a+',encoding='utf-8') as file:
                                file.write(f'{orginal_link}\n')
                                file.close()
            except:
                continue
                

       

    def extract_data(self, article, stt, title , source_post_id , comments, views):
        now = datetime.now()
        time_crawl = int(now.timestamp())
        data = {}
        list_like = []
        share_link = []
        image_list = []
        # lấy id bài viết , author
        id = article['id']
        author = article['data-author']
        if stt == 0 :
            type_ = 'otofun post'
            source_id = ''
            comments = comments 
            views = views
        else: 
            type_ = 'otofun comment'
            source_id = source_post_id
            comments = 0
            views = 0
            title = ''
        # Lấy created_time , link
        message_divs = article.find_all('div', class_='message-attribution-main')
        for message_div in message_divs:
            links = message_div.find_all('a')
            for link in links:
                link_post = self.domain + link.get('href')
                txt_time = link.text.replace('\n','').replace('\t','')
                created_time = self.convert_to_timestamp(date_time_str=txt_time)

        share_link = []
        e_share_link= article.select('div.bbCodeBlock.bbCodeBlock--unfurl.js-unfurl.fauxBlockLink')
        for e_s in e_share_link:
            share_link.append(e_s['data-url'])
            e_s.decompose()

        # thông tin tác giả
        author_infor = article.select_one('a.avatar.avatar--m')
        id_user = author_infor['data-user-id']
        author_link = self.domain + author_infor['href']
        avatar_img_element = author_infor.select_one('img')
        avatar = self.domain + avatar_img_element['src']
        role = (article.select_one('h5.userTitle.message-userTitle')).get_text()
        
        blockquotes_to_remove = article.find_all('blockquote', class_='bbCodeBlock bbCodeBlock--expandable bbCodeBlock--quote js-expandWatch')
        for blockquote in blockquotes_to_remove:
            blockquote.decompose()
        # Nội dung bài viết
        content = article.select_one('div.bbWrapper').get_text()

        # lấy link ảnh có trong bài viết
        img_post_element = article.select_one('div.bbWrapper').select('img')
        for im in img_post_element :
            if '.gif' not in  im['src'] :
                image_list.append(im['src'])
        

        # Lấy link video

        videos = []
        try:
            media_wrapper = article.select_one('bbMediaWrapper')
            iframe = media_wrapper.select('iframe')
            for m in iframe:
                videos.append(m['src'])
            media_wrapper.decompose()
        except:
            pass
        # lấy link cảm xúc (nếu có)
        try:
            reactions_link = self.domain + (article.select_one('a.reactionsBar-link'))['href']
        except:
            reactions_link = None
            pass
        if reactions_link != None :
            list_like = self.get_reactions(url= reactions_link)

        data['id'] = id
        data['time_crawl'] = time_crawl
        data['link'] = link_post
        data['created_time'] = created_time
        data['author'] = author
        data['author_link'] = author_link
        data['id_user'] = id_user
        data['avatar'] = avatar
        data['domain'] =  self.domain
        data['role'] = role
        data['type'] = type_
        data['title'] = title
        data['content'] = content
        data['image_url'] = image_list
        data['out_links'] = share_link
        data['source_id'] = source_id
        data['comments'] = comments
        data['view'] = views
        data['like'] = len(list_like)
        data['list_like'] = list_like
        data['video'] = videos
        data['out_links'] = share_link
        return data

    
    def convert_to_timestamp(self,date_time_str):
        time_str, date_str = date_time_str.split()
        time_components = [int(x) for x in time_str.split(":")]
        time_obj = dt.time(hour=time_components[0], minute=time_components[1])
        date_components = [int(x) for x in date_str.split("/")]
        date_obj = dt.date(year=date_components[2], month=date_components[1], day=date_components[0])
        datetime_obj = dt.datetime.combine(date_obj, time_obj)
        timestamp = int(datetime_obj.timestamp() )
        return timestamp
    

    def convert_unit_to_num (self,txt):
        numbers = (re.findall(r'\d+', txt))[0]
        try:
            words = (re.findall(r'[a-zA-Z]+', txt))[0]
        except:
            words=''
            pass
        if words == 'K':
            return int(float(numbers) * 1000)
        elif words == 'M':
            return int(float(numbers) * 1000000)
        elif words == 'B':
            return int(float(numbers) * 1000000000)
        else:
            return int(numbers)
        

# reaction_link = 'https://www.otofun.net/posts/69132358/reactions'
otofun = BS4_OTOFUN()
otofun.get_link(url= 'https://www.otofun.net/forums/quan-cafe-otofun.77/?last_days=7&order=post_date&direction=desc')
otofun.crawler()