
from bs4 import BeautifulSoup
from datetime import datetime
from Browser import GET_HTML
from queue import Queue
from Post import PostForumz
from kafka_ncs_temp import push_kafka
import re

class BS4_Crawler :
    def __init__(self):
        self.link_queue = Queue()

    def extract_link(self, url):
        link_crawled = []
        with open('link.txt', 'r') as f:
            for line in f:
                link_crawled.append(line.rstrip())  # Loại bỏ ký tự dòng mới
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        midnight_timestamp = int(today.timestamp())
        black_list = []
        next_page = 'true' 
        while next_page != None:
            gt = GET_HTML(domain='voz.vn',url=url)
            html_content= gt.get_html()
            soup = BeautifulSoup(html_content, 'html.parser')
            div_elements = soup.select('div.structItemContainer-group.js-threadList > div')
            try:
                next_page_element = soup.select('a.pageNavSimple-el.pageNavSimple-el--next')[0]
                next_page = 'https://voz.vn' + next_page_element['href']
                url = next_page
            except:
                next_page = None
            for div in div_elements:
                try:
                    reaction_txt = str(div.select_one("div.structItem-cell.structItem-cell--meta").text).split('\n')
                    comment = self.convert_unit_to_num(str(reaction_txt[3]))
                    views = self.convert_unit_to_num(str(reaction_txt[7]))

                    a_element = div.select('div.structItem-title a')[-1]
                    li_element = div.select_one('li.structItem-startDate')
                    time_element = li_element.select_one('time')
                    time_ = int(time_element['data-time'])
                    href = a_element['href'].replace('/unread', '')
                    last_char = href[-1]
                    if last_char != '/':
                        href = href+'/'
                    if href in link_crawled:
                        return 
                    elif href not in link_crawled and href not in black_list and time_ >= midnight_timestamp:
                        href = 'https://voz.vn' + str(href)
                        link = f'{href}|{comment}|{views}'
                        print(f'---------->>>>>>>>> Put {link} to Queue')
                        
                        self.link_queue.put(link)
                except Exception as e:
                    pass
                    print(f'Error processing div: {e}')

                if time_ < midnight_timestamp:
                    return

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
        gt1 = GET_HTML(url=url,domain=domain)
        html_content = gt1.get_html()
        list_like = []
        list_angry = []
        list_haha = []
        list_love = []
        list_wow = []
        soup = BeautifulSoup(html_content, 'html.parser')
        reaction_row = soup.select('li.block-row.block-row--separated')
        for r in reaction_row:
            data = {}
            list_like = []
            list_angry = []
            # Lấy avatar, link, id, name
            author_information = r.select_one('a.avatar.avatar--s')
            id_user = author_information['data-user-id']
            author_link = 'https://voz.vn' + author_information['href']
            try:
                avatar = author_information.select_one('img')['src']
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
                location_link = 'https://voz.vn/' +  presentation['href']
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

    def extract_post(self,a,soup,timestamp,source_id_post,title,comments,views,domain):
        
        data = {}
        list_like = []
        list_angry = []
        id = a['id']
        author = a['data-author']
        try:
            itemtype = a['itemtype']
        except:
            itemtype = None
            pass
        if itemtype:
            object_type = 'voz comment'
            title = ''
            source_id = source_id_post
            comments = 0
            views = 0
            try:
                blockquote = a.select('blockquote.bbCodeBlock.bbCodeBlock--expandable.bbCodeBlock--quote.js-expandWatch')
                for b in blockquote:
                    b.decompose()
            except:
                pass
        else:
            object_type = 'voz post'
            source_id = ''            

        # Xóa nút mở rộng
        expandLink_button = soup.select('div.bbCodeBlock-expandLink.js-expandLink')
        for ex in expandLink_button :
            ex.decompose()
        share_link = []
        e_share_link= soup.select('div.bbCodeBlock.bbCodeBlock--unfurl.js-unfurl.fauxBlockLink')
        for e_s in e_share_link:
            share_link.append(e_s['data-url'])
            e_s.decompose()

        # Lấy link ảnh sau đó xóa thẻ ảnh ( loại bỏ chú thích ảnh )
        arr_image_link = []
        # try:
        #     e_image_link=a.select('img.bbImage')
        #     for e in e_image_link:
        #         image_link = e['data-url']
        #         arr_image_link.append(image_link)
        #         e.decompose()
        # except:
        #     pass 
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
        # Lấy thời gian đăng bài
        created_time = int(soup.select_one('time.u-dt')['data-time'])
        
        # Lấy link bài post
        li = a.select_one('li.u-concealed')
        link_post = 'https://voz.vn'+ li.select_one('a')['href']
        
        
        # Lấy nội dung post
        content= a.select("div.bbWrapper")[0].get_text().replace('\n\n\n\n','')
        
        # Lấy link và id tác giả
        author_information = a.select('a.avatar.avatar--m')
        author_link = 'https://voz.vn'+ author_information[0]['href']
        author_id = str(author_information[0]['data-user-id'])
        # Lấy link avatar tác giả
        try:
            author_avatar = author_information[0].select_one('img')['src']
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
            reactions_link = 'https://voz.vn/' + a.select_one('a.reactionsBar-link')['href']
        except:
            reactions_link = None
            pass
        if reactions_link is not None :
            (list_like, list_angry) = self.get_reactions (url=reactions_link,domain=domain)
        # POST
        data['id'] = id
        data['time_crawl'] = timestamp
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
        data['comment'] = comments
        data['view'] = views
        data['like'] = len(list_like)
        data['list_like'] = list_like
        data['angry'] = len(list_angry)
        data['list_angry'] = list_angry
        data['video'] = videos

        return data


    def crawl_post(self):
            # Timestamp lấy bài
            now = datetime.now()
            timestamp = int(now.timestamp())
            while not self.link_queue.empty():
                try:
                    combine_link = self.link_queue.get()
                    split_link = str(combine_link).split('|')
                    link = split_link[0].replace('https://voz.vn','')
                    orginal_link = link
                    comments = split_link[1]
                    views = split_link[2]
                    next_page = 'true'
                    while next_page != None:
                        get_html = GET_HTML(domain='voz.vn',url=link)
                        html_content = get_html.get_html()
                        soup = BeautifulSoup(html_content, 'html.parser')
                        try:
                            next_page = 'https://voz.vn' + soup.select_one('a.pageNav-jump.pageNav-jump--next')['href']
                            link = next_page
                        except:
                            next_page = None
                        title = soup.select(".p-title-value")[0].get_text()
                        articles = soup.select('article.message.message--post.js-post.js-inlineModContainer')
                        source_id_post = articles[0]['id'] 
                        for a in articles:
                            try:
                                post= self.extract_post(a=a,soup=soup,source_id_post=source_id_post,timestamp=timestamp,title=title,comments=comments,views=views)
                                print(post)
                                push_kafka([PostForumz(**post)])  
                            except Exception as e:
                                print(e)
                                continue
                    with open('link.txt','a+',encoding='utf-8') as file:
                                    file.write(f'{orginal_link}\n')
                                    file.close()
                except:
                    continue
                