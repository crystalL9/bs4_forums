from playwright.sync_api import sync_playwright
import time
import http.client
import requests

class ChromiumBrowser:
    def __init__(self, proxy=None,reset=0,fake=0,cookie=None):
        self.browser = None
        self.context = None
        self.page = None
        self.cookie=cookie
        self.proxy = proxy
        self.reset = reset
        self.fake= fake
        self.playwright = sync_playwright().start()
        self.init_browser()

    def init_browser(self):
        browser_args = [
            '--disable-software-rasterizer',
            '--disable-background-networking',
            '--disable-default-apps',
            '--disable-extensions',
            '--disable-sync',
            '--disable-translate',
            '--disable-setuid-sandbox',
            '--disable-gpu',
            '--single-process',
            '--no-sandbox',
            '--disable-application-cache',
            '--disable-offline-load-stale-cache',
            '--disk-cache-size=0',
            '--media-cache-size=0',
            '--no-zygote',
            '--start-maximized',
            '--no-first-run',
            '--disable-renderer-backgrounding',
            '--disable-backgrounding-occluded-windows',
            '--disable-background-timer-throttling',
            '--enable-fast-unload',
            '--disable-blink-features=AutomationControlled',
            '--blink-settings=imagesEnabled=false'
        ]
        
        launch_options = {
            'headless': False,
            'args': browser_args
        }
        if self.proxy:
            if self.reset==1:
                self.change_ip_proxy()
            status=self.check_status_proxy()
            if status == True:
                print(f"Proxy {self.proxy} is ready")
                launch_options['proxy'] = {'server': f'http://{self.proxy}'}
            else:
                pass

        self.browser = self.playwright.chromium.launch(**launch_options)
        self.context = self.browser.new_context(no_viewport=True)
        if self.fake==1:
            cookies = self.cookie
            self.context.add_cookies(cookies)
            self.page = self.context.new_page()
        else:
            self.page = self.context.new_page()
        # if self.fake==1:
        #     if len(pages) > 1:
        #         pages[0].close()
    def change_ip_proxy(self):
        pass
        # print(f"Change IP Public of Proxy: {self.proxy}")
        # port=str(self.proxy).split(':')[-1]
        # url = f'http://192.168.143.101:6868/reset?proxy={port}'
        # response = requests.post(url)
        # time.sleep(10)
    def check_status_proxy(self):
        return True
        # print(f"Check status proxy {self.proxy}")
        # url=f'http://192.168.143.101:6868/status?proxy={self.proxy}'
        # response = requests.post(url)
        # json_res=json.loads(response.text)
        # if json_res['status'] is True:
        #     return True
        # else:
        #     return False  
    def close(self):
        if self.context:
            self.context.close()
        if self.playwright:
            self.playwright.stop()

    def open_new_tab(self, url='about:blank'):
        new_page = self.context.new_page()
        new_page.goto(url)
        return new_page

    def close_tab(self, page):
        if page:
            page.close()
            print('Tab has been closed.')
        else:
            print('The page does not exist.')

class GET_HTML:
    def __init__(self, url, domain):
        self.url = url
        self.domain = domain
        
    def get_html(self):
        start_time = time.time()
        proxy_host = '192.168.143.102'
        proxy_port = 4013
        conn = http.client.HTTPSConnection(proxy_host, proxy_port)
        target_host = self.domain
        target_path = str(self.url).replace(f'https://{self.domain}','')
        cookies = [
            {
                "domain": "voz.vn",
                "expirationDate": 8841849156,
                "name": "xf_bcc",
                "path": "/",
                "value": "cacbbbbc"
            },
            {
                "domain": "voz.vn",
                "name": "xf_session",
                "path": "/",
                "value": "dVrl08kOWl-sW6sLYhaXS7Ghy-U9wcfL"
            },
            {
                "domain": "voz.vn",
                "expirationDate": 8841849156.843996,
                "name": "xf_user",
                "path": "/",
                "value": "1972472%2CQtj5lBKbMYGTIakLE9SPZwRWLWLaoVFF6mzs-3t8"
            }]
        cookie_string = '; '.join(f"{cookie['name']}={cookie['value']}" for cookie in cookies)
        headers = {
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.0.0',
            'sec-ch-ua': '"Microsoft Edge";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'Cookie': cookie_string
        }
        conn.set_tunnel(target_host)
        conn.request(
            'GET',
            f'{target_path}',
            headers=headers

        )
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"Time get reponse: {elapsed_time} giây")
        response = conn.getresponse()
        data = response.read()
        html = data.decode('utf-8')
        return html

class GET_HTML_REQUEST:
    def __init__(self, url):
        self.url = url
    def get_html(self):
        start_time = time.time()
        response = requests.get(self.url)
        html = response.text
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"Time get reponse: {elapsed_time} giây")
        return html