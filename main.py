import time
from crawler_voz_bs4 import BS4_Crawler
import threading
from crawler_xamvn_bs4 import XAMVN_BS4

def run_voz():
    url = '/f/diem-bao.33/?last_days=7&order=post_date&direction=desc'
    crawler = BS4_Crawler()
    crawler.extract_link(url=url)
    crawler.crawl_post()

def run_xamvn():
    bs4_xamvn = XAMVN_BS4() 
    bs4_xamvn.get_link(url='https://xamvn.icu/box/leu-bao-nguoi-toi-co.145/?order=bumped_threads&direction=desc')
    bs4_xamvn.get_full_articles()
if __name__ == "__main__":
    while True:
        voz_thread = threading.Thread(target=run_voz)
        xamvn_thread = threading.Thread(target=run_xamvn)
        # voz_thread.start()
        xamvn_thread.start()
        # voz_thread.join()
        xamvn_thread.join()
        time.sleep(1800)
    