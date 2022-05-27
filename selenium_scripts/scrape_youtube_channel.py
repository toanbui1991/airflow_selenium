from http.client import CannotSendRequest
import time, re
import pymysql
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from datetime import datetime
from selenium_scripts.config import config_local
from selenium_scripts.utils.mysql_utils import generate_upsert_statement


def scrape_youtube_channel(driver, channel_id):

    start = time.time()
    target_link = 'https://www.youtube.com/channel/{}/about'.format(channel_id)

    wait = WebDriverWait(driver,15)
    driver.get(target_link)

    #wait for page to load with tag body and roll down to load comments counts
    for _ in range(2):
        wait.until(EC.visibility_of_element_located((By.TAG_NAME, "body"))).send_keys(Keys.END)
        time.sleep(2)
    #wait for statistic element to show up
    elements = driver.find_elements_by_css_selector('div.tab-content.style-scope.tp-yt-paper-tab')
    about_element = elements[-1]
    driver.execute_script("arguments[0].click();", about_element)

    for _ in range(2):
        wait.until(EC.visibility_of_element_located((By.TAG_NAME, "body"))).send_keys(Keys.END)
        time.sleep(2)
    wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'div#right-column')))
    page_source = driver.page_source

    #parse the data with beautiful soup
    soup = BeautifulSoup(page_source, 'html.parser')
    channel_title =  soup.select_one('div#meta yt-formatted-string#text').get_text()
    channel_description = soup.select_one('div#description-container yt-formatted-string#description').get_text()
    country_id = soup.select('td.style-scope.ytd-channel-about-metadata-renderer yt-formatted-string.style-scope.ytd-channel-about-metadata-renderer')
    country_id = country_id[-1].get_text()
    # likes = soup.select_one('div#description-container yt-formatted-string#description').get_text() #do not have
    view_count = soup.select('div#right-column yt-formatted-string.style-scope.ytd-channel-about-metadata-renderer')
    view_count = view_count[2].get_text()
    view_count = view_count.replace('views', '')
    view_count = view_count.replace(r'[,|\s]', '')
    view_count = re.sub(r'[,|\s]', '', view_count)
    view_count = int(view_count)
    # comment_count = #do not have
    subscriber_count = soup.select_one('yt-formatted-string#subscriber-count').get_text()
    subscriber_count = subscriber_count.replace('subscribers', '')
    subscriber_count = re.sub(r'[,|\s]', '', subscriber_count)
    #reference to convert string to int
    number_dict = {'K': 1000, 'M': 1000000, 'B': 1000000000, 'T': 1000000000000}
    subscriber_count = float(subscriber_count[:-1]) * number_dict.get(subscriber_count[-1])
    subscriber_count = int(subscriber_count)
    # video_count = #do not have
    thumbnails_url = soup.select_one('div#channel-header-container img').get('src')
    thumbnails_width = soup.select_one('div#channel-header-container img').get('width')
    thumbnails_height = thumbnails_width
    # official_fl = #do not understand
    # display_fl = #do not understand
    published_ts = soup.select('div#right-column yt-formatted-string.style-scope.ytd-channel-about-metadata-renderer span')
    published_ts = published_ts[1].get_text()
    #convert str to datetime object
    published_ts = datetime.strptime(published_ts, '%b %d, %Y')
    item = {'channel_id': channel_id, 'channel_title': channel_title, 'channel_description': channel_description,
    'country_id': country_id, 'view_count': view_count, 'subscriber_count': subscriber_count, 'thumbnails_url': thumbnails_url,
    'thumbnails_width': thumbnails_width, 'thumbnails_height': thumbnails_height, 'published_ts': published_ts}
    print('item: ')
    print(item)

    #insert data into mysql
    connection = pymysql.connect(**config_local.get('mysql_dev_remote'))
    #pymysql use context manager so it will close connection and cursor when it finished
    with connection:
        with connection.cursor() as cursor:
            # Create a new record
            try:
                target_table = 'tb_youtube_chn_info_stg'
                sql = generate_upsert_statement(target_table, item)
                params = tuple(item.values())
                cursor.execute(sql, params)
                connection.commit()
            except Exception as e:
                connection.rollback()
                raise e
                
    end = time.time()
    period = end - start
    print('time to finished: {}'.format(period))       

