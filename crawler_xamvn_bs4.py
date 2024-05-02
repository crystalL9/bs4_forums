import requests
from bs4 import BeautifulSoup
from datetime import datetime
import time
import re
from Browser import GET_HTML_REQUEST
from queue import Queue
from Post import PostForumz
from kafka_ncs_temp import push_kafka 
class XAMVN_BS4:
    def __init__(self):
        self.link_queue = Queue()
        self.domain = 'https://xamvn.icu'

    def extract_number(self,text):
        match = re.search(r"<dd>(\d+)</dd>", text)
        if match:
            return match.group(1)
        else:
            return '0'

    def get_link(self,url):
        self.link_queue.put('https://xamvn.icu/r/nhan-giong-thanh-cong-loai-ca-cuc-hiem-o-viet-nam-gia-ca-trieu-dong-con.789235/|123|123')
        # today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        # midnight_timestamp = int(today.timestamp())
        # path_crawled = 'link.txt'
        # path_black_list = 'black_list.txt'

        # with open(path_crawled, 'r') as file:
        #     link_crawled = [line.strip() for line in file]

        # with open(path_black_list, 'r') as file:
        #     black_list = [line.strip() for line in file]

        # next_link = ''
        # while next_link is not None:
        #     rq = GET_HTML_REQUEST(url=url)
        #     html_content = rq.get_html()
        #     soup = BeautifulSoup(html_content, 'html.parser')
        #     next_link_tag = soup.select_one('a.pageNav-jump.pageNav-jump--next')

        #     if next_link_tag:
        #         next_link = next_link_tag['href']
        #         url = self.domain + next_link
        #     else:
        #         next_link = None

        #     div_elements = soup.select('div.structItemContainer-group.js-threadList > div')
        #     for div in div_elements:
        #         try:
        #             li = div.select_one('li.structItem-startDate')
        #             a = li.select_one('a')
        #             href = a['href']
        #             time_element = li.select_one('time')
        #             time_ = int(time_element['data-time'])

        #             cmt_view = div.select_one('div.structItem-cell.structItem-cell--meta')
        #             numbers = cmt_view.select("dd")
        #             arr_num = []
        #             for n in numbers: 
        #                 arr_num.append(self.convert_unit_to_num(self.extract_number(n.text)))
        #             if href in link_crawled:
        #                 return
        #             else:
        #                 if len(numbers) > 1 and href not in link_crawled and href not in black_list and time_ >= midnight_timestamp:
        #                     print(f'---------->>>>>>>>> Put {href} to Queue')
        #                     self.link_queue.put(f'{self.domain + href}|{arr_num[0]}|{arr_num[1]}')
        #                 elif time_ < midnight_timestamp:
        #                     print("Đã lấy hết link bài mới")
        #                     return
        #         except Exception as e:
        #             print(f'An exception occurred: {e}')
        #             continue
        #     time.sleep(2)

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

    def get_reactions(self,url,domain):
            gt1 = GET_HTML_REQUEST(url=url)
            html_content = gt1.get_html()
            list_like = []
            list_angry = []
            list_wow = []
            list_haha = []
            list_love = []
            soup = BeautifulSoup(html_content, 'html.parser')
            reaction_row = soup.select('li.block-row.block-row--separated')
            for r in reaction_row:
                data = {}
                list_like = []
                list_angry = []
                # Lấy avatar, link, id, name
                author_information = r.select_one('a.avatar.avatar--s')
                id_user = author_information['data-user-id']
                author_link = domain + author_information['href']
                try:
                    avatar = domain + author_information.select_one('img')['src']
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
                    location_link = domain +  presentation['href']
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
                # Lấy id cảm xúc (1-ưng,2-phẫn nộ)
                id_reactions = r.select_one('div.contentRow-extra').select_one('span')['data-reaction-id']
                if id_reactions=='1':
                    list_like.append(data)
                elif id_reactions=='2':
                    list_love.append(data)
                elif id_reactions=='3':
                    list_haha.append(data)
                elif id_reactions=='4':
                    list_wow.append(data)
                elif id_reactions=='6':
                    list_angry.append(data)
            return list_like, list_angry, list_love, list_haha, list_wow

    def extract_spotlight_post(self,a,soup,source_id_post,title,comments,views,domain):
        now = datetime.now()
        time_crawl = int(now.timestamp())
        data = {}
        list_like = []
        list_angry = []
        list_love = []
        list_haha = []
        list_wow  = []
        id = source_id_post
        share_link = []
        # Lấy link share nếu có sau đó xóa
        try:
            share_div = a.select('div.bbCodeBlock.bbCodeBlock--unfurl.js-unfurl.fauxBlockLink')
            for s in share_div:
                share_link.append(s['data-url'])
                s.decompose()
        except:
            pass
        
        # Lấy ảnh

        arr_image_link = []
        try:
            bbwrapper_div = soup.select_one('div.bbWrapper')

            # Find all img tags within the bbWrapper div
            img_tags = bbwrapper_div.select('img')
            for e in img_tags:
                image_link = e['data-url']
                arr_image_link.append(image_link)
                e.decompose()

        except:
            pass 

        # Lấy link và id tác giả
        author_information = a.select('a.avatar.avatar--m')
        author_link = domain + author_information[0]['href']
        author_id = str(author_information[0]['data-user-id'])
        author = a.select_one('a.username').get_text()

        # Lấy thời gian đăng bài
        created_time = int(soup.select_one('time.u-dt')['data-time'])
        
        # Lấy link bài post
        li = a.select_one('li.u-concealed')
        link_post = domain + li.select_one('a')['href']
        # Lấy link avatar tác giả
        try:
            author_avatar = domain + author_information[0].select_one('img')['src']
        except:
            author_avatar = ''
            pass
        # Lấy video
        videos = []
        try:
            media_wrapper = a.select_one('div.bbWrapper')
            iframe = media_wrapper.select('iframe')
            for m in iframe:
                videos.append(m['src'])
            media_wrapper.decompose()
        except:
            pass
        # Lấy content bài viết
        content = a.select_one('div.message-content.js-messageContent').get_text().replace('\n\n\n\n','\n')
        # Lấy link cảm xúc (nếu có)
        try:
            reactions_link = domain + a.select_one('a.reactionsBar-link')['href']
        except:
            reactions_link = None
            pass
        if reactions_link is not None :
            (list_like, list_angry, list_love, list_haha, list_wow) = self.get_reactions (url=reactions_link,domain=domain)
        data['id'] = id
        data['time_crawl'] = time_crawl
        data['link'] = link_post
        data['created_time'] = created_time
        data['author'] = author
        data['author_link'] = author_link
        data['id_user'] = author_id
        data['avatar'] = author_avatar
        data['type'] = 'xamvn post'
        data['title'] = title
        data['content'] = content
        data['image_url'] = arr_image_link
        data['out_links'] = share_link
        data['source_id'] = ''
        data['comment'] = comments
        data['view'] = views
        data['like'] = len(list_like)
        data['list_like'] = list_like
        data['angry'] = len(list_angry)
        data['list_angry'] = list_angry
        data['love'] = len(list_love)
        data['list_love'] = list_love
        data['haha'] = len(list_haha)
        data['list_haha'] = list_haha
        data['wow'] = len(list_wow)
        data['list_wow'] = list_wow
        data['video'] = videos

        return data

    def extract_comment(self,a,soup,source_id_post,title,comments,views,domain):
        now = datetime.now()
        time_crawl = int(now.timestamp())
        data = {}
        list_like = []
        list_angry = []
        list_love = []
        list_haha = []
        list_wow  = []
        id = a['id']
        author = a['data-author']
        try:
            itemtype = a['itemtype']
        except:
            itemtype = None
            pass
        if itemtype:
            object_type = 'xamvn comment'
            title = ''
            source_id = source_id_post
            comments = 0
            views = 0
            try:
                blockquote = a.select('blockquote.bbCodeBlock.bbCodeBlock--expandable.bbCodeBlock--quote.js-expandWatch')
                for b in blockquote:
                    id = 'js-post-'+str(b['data-source']).split('post: ')[-1] + '.' + a['id'] 
                    b.decompose()
                
            except:
                pass        

            # Xóa nút mở rộng
            expandLink_button = a.select('div.bbCodeBlock-expandLink.js-expandLink')
            for ex in expandLink_button :
                ex.decompose()
            share_link = []
            e_share_link= a.select('div.bbCodeBlock.bbCodeBlock--unfurl.js-unfurl.fauxBlockLink')
            for e_s in e_share_link:
                share_link.append(e_s['data-url'])
                e_s.decompose()

            # Lấy link ảnh sau đó xóa thẻ ảnh ( loại bỏ chú thích ảnh )
            arr_image_link = []
            try:
                e_image_link=a.select('img.bbImage')
                for e in e_image_link:
                    image_link = e['data-url']
                    arr_image_link.append(image_link)
                    e.decompose()
            except:
                pass    
            # Lấy thời gian đăng bài
            created_time = int(soup.select_one('time.u-dt')['data-time'])
            
            # Lấy link bài post
            li = a.select_one('li.u-concealed')
            link_post = domain + li.select_one('a')['href']
            
            
            # Lấy nội dung post
            content= a.select("div.bbWrapper")[0].get_text().replace('\n\n\n\n','')
            
            # Lấy link và id tác giả
            author_information = a.select('a.avatar.avatar--m')
            author_link = domain + author_information[0]['href']
            author_id = str(author_information[0]['data-user-id'])
            # Lấy link avatar tác giả
            try:
                author_avatar = domain + author_information[0].select_one('img')['src']
            except:
                author_avatar = ''
                pass
            # Lấy video
            videos = []
            try:
                media_wrapper = a.select_one('bbMediaWrapper')
                iframe = media_wrapper.select('iframe')
                for m in iframe:
                    videos.append(m['src'])
                media_wrapper.decompose()
            except:
                pass

            # Lấy link cảm xúc (nếu có)
            try:
                reactions_link = domain + a.select_one('a.reactionsBar-link')['href']
            except:
                reactions_link = None
                pass
            if reactions_link is not None :
                (list_like, list_angry, list_love, list_haha, list_wow) = self.get_reactions (url=reactions_link,domain=domain)
            # POST
            data['id'] = id
            data['time_crawl'] = time_crawl
            data['link'] = link_post
            data['created_time'] = created_time
            data['author'] = author
            data['author_link'] = author_link
            data['id_user'] = author_id
            data['avatar'] = author_avatar
            data['type'] = object_type
            data['title'] = title
            data['content'] = content
            data['image_url'] = arr_image_link
            data['out_links'] = share_link
            data['source_id'] = source_id
            data['comments'] = comments
            data['view'] = views
            data['like'] = len(list_like)
            data['list_like'] = list_like
            data['angry'] = len(list_angry)
            data['list_angry'] = list_angry
            data['love'] = len(list_love)
            data['list_love'] = list_love
            data['haha'] = len(list_haha)
            data['list_haha'] = list_haha
            data['wow'] = len(list_wow)
            data['list_wow'] = list_wow
            data['video'] = videos
            return data

    def get_full_articles(self):
        while not self.link_queue.empty():
            try:
                str_link = self.link_queue.get()
                split_link = str_link.split('|')
                url = split_link[0]
                comments = split_link[1]
                views = split_link[2]
                orignal_link = url
                next_page = 'true'
                page = 0 
                print(f"Crawl all data of all pages from {url}")
                while next_page != None:
                    page = page + 1
                    domain = 'https://xamvn.icu'
                    _request = GET_HTML_REQUEST(url = url)
                    html_content = _request.get_html()
                    soup = BeautifulSoup(html_content, 'html.parser')
                    next_link_elm = soup.find('a', class_='pageNav-jump pageNav-jump--next')
                    next_link = next_link_elm['href'] if next_link_elm else None
                    if next_link == None :
                        next_page = None
                    else :
                        url = next_link
                    
                    try:
                        article_post = soup.find('article')
                        source_id = article_post.get('id') if article_post else None
                        title_element = soup.find('div', class_='p-title')
                        for span in title_element.find_all('span'):
                            span.extract()
                        title = title_element.get_text(strip=True) if title_element else ''
                        
                        if page <= 1:
                            try:
                                post= self.extract_spotlight_post(a=article_post,soup=soup,source_id_post=source_id,title=title,comments=comments,views=views,domain=domain)
                                push_kafka([PostForumz(**post)])
                                with open('xamvn.txt','a+',encoding='utf-8') as file:
                                    file.write(f'{post}\n')
                                    file.close()
                            except Exception as e:
                                print(e)
                                pass
                        articles = soup.find_all('article', class_='message message--post js-post js-inlineModContainer')
                        for a in articles:
                            try:
                            # lấy content bài viết
                                comment = self.extract_comment(a=a,soup=soup,source_id_post=source_id,title=title,comments=0,views=0,domain=domain)
                                if comment != None :
                                    # with open('xamvn.txt','a+',encoding='utf-8') as file:
                                    #     file.write(f'{comment}\n')
                                    #     file.close()
                                    push_kafka([PostForumz(**comment)])
                            except Exception as e:
                                print(e)
                                pass
                    except :
                        pass
                with open('link.txt','a+',encoding='utf-8') as file :
                    file.write(f'{str(orignal_link).replace(self.domain,"")}\n')
                    file.close()

            except:
                continue