import time, re, os
import pandas as pd
import numpy as np
import MySQLdb
import sshtunnel
from selenium.webdriver import Chrome, ChromeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from datetime import datetime
from utils.pd_mysql_utils import df_to_mysql
from selenium_scripts.config import config

def scrape_video(video_id):
    """ parse youtube video information given video_id

    Args:
        video_id (str): youtube video id

    Returns:
        dictionary: video information in dictionary
    """

    # driver_path = './chromedriver.exe' # do not specify chromedriver path
    # driver_path = os.getcwd() + 'chromedriver.exe'
    target_link = 'https://www.youtube.com/watch?v={}'.format(video_id)
    options = ChromeOptions()
    options.add_argument("--lang=en-US")
    options.add_argument("--no-sandbox")
    # options.add_argument("--headless")

    #start browser with context manager
    with Chrome(chrome_options=options) as driver:
        wait = WebDriverWait(driver,15)
        try: 
            driver.get(target_link)

            #wait for page to load with tag body and roll down to load comments counts
            for _ in range(2):
                wait.until(EC.visibility_of_element_located((By.TAG_NAME, "body"))).send_keys(Keys.END)
                time.sleep(2)
            #find number of comments in the video
            wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, 'h2#count yt-formatted-string span')))
            page_source = driver.page_source
        except Exception as e:
            #do not raise Exception here. it will break the loop
            print('Exception: ', e)
            item = {'video_id': video_id, 'video_title': None, 'video_description': None, 'channel_id': None, 'channel_title': None,
            'like_count': None, 'view_count': None, 'dislike_count': None, 'comment_count': None, 'published_ts': None}
            return item
        else:
            #if can access video continue process
            soup = BeautifulSoup(page_source, 'html.parser')
            video_title =  soup.select_one('h1.title.style-scope.ytd-video-primary-info-renderer yt-formatted-string').get_text()
            video_description = soup.select_one('div#description').get_text()
            channel_id = soup.select_one('div#upload-info a').get('href')
            channel_id = channel_id.replace('/channel/', '')
            channel_title = soup.select_one('div#upload-info a').get_text()
            # video_tags = soup.select('yt-formatted-string#text') #only have if you sign in
            # tags = video_tags[-1]
            # print(tags)
            # tags = tags.select('div#scroll-container')
            # tags = [tag.get_text() for tag in tags]

            # print(tags)
            # video_tags = [tag.get_text() for tag in video_tags]
            # video_tags = ', '.join(video_tags)
            # category_id, do not know
            # default_language
            # default_audio_language
            # live_broadcast_content
            number_dict = {'K': 1000, 'M': 1000000, 'B': 1000000000, 'T': 1000000000000}
            try:
                like_count = soup.select_one('yt-formatted-string.style-scope.ytd-toggle-button-renderer.style-text').get_text()
                like_count = float(like_count[:-1]) * number_dict.get(like_count[-1])
                like_count = int(like_count)
            except Exception as e:
                print('Exception: ', e)
                like_count = None
            try:
                view_count = soup.select_one('ytd-video-view-count-renderer.style-scope.ytd-video-primary-info-renderer span').get_text()
                view_count = view_count.replace('views', '')
                # view_count = view_count.replace(r'[,|\s]', '') # for regex replace substring use re package instead
                view_count = re.sub(r'[,|\s]', '', view_count)
                view_count = int(view_count)
            except Exception as e:
                print('Exception: ', e)
            dislike_count = None
            comment_count = soup.select_one('h2#count yt-formatted-string span').get_text()
            comment_count = re.sub(r'[,|\s]', '', comment_count)
            # thumbnails_url
            # thumbnails_width
            # thumbnails_height
            published_ts = soup.select_one('div#info-strings yt-formatted-string').get_text()
            try:
                published_ts = datetime.strptime(published_ts, '%b %d, %Y')
            except Exception as e:
                print('Exception: ', e)
            item = {'video_id': video_id, 'video_title': video_title, 'video_description': video_description, 'channel_id': channel_id, 'channel_title': channel_title,
            'like_count': like_count, 'view_count': view_count, 'dislike_count': dislike_count, 'comment_count': comment_count, 'published_ts': published_ts}
            return item #type dict

def scrape_channel_basic(channel_id):
    """parse channel information with given channel_id

    Args:
        channel_id (str): id of the youtube channel

    Returns:
        dictionary: dictionary contain youtube channel information
    """

    # driver_path = './chromedriver.exe'
    driver_path = '/usr/bin/chromedriver'
    target_link = 'https://www.youtube.com/channel/{}/about'.format(channel_id)
    options = ChromeOptions()
    # options.add_argument("--lang=ko-KR")
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument("--headless")
    options.add_argument("--lang=en-US")
    # options.add_argument('--headless') 
    options.add_argument('--disable-gpu')

    #start browser with context manager
    with Chrome(executable_path=driver_path, chrome_options=options) as driver:
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
    return item

def scrape_channel(channel_id):
    """scrape youtube channel page to get information about the channel

    Args:
        channel_id (str): channel id of the target channel

    Returns:
        pandas dataframe: channel data type pandas dataframe
    """
    driver_path = './chromedriver.exe'
    target_link = 'https://www.youtube.com/channel/{}'.format(channel_id)
    options = ChromeOptions()
    # options.add_argument("--lang=ko-KR")
    options.add_argument("--lang=en-US")

    #start browser with context manager
    with Chrome(executable_path=driver_path, chrome_options=options) as driver:
        wait = WebDriverWait(driver,15)
        try:
            driver.get(target_link)
        except Exception as e:
            print('Exception: ', e)
        else:
            wait.until(EC.visibility_of_element_located((By.TAG_NAME, "body"))).send_keys(Keys.END)
            #wait for statistic element to show up
            elements = driver.find_elements_by_css_selector('div.tab-content.style-scope.tp-yt-paper-tab')
            print('elements len: {}'.format(len(elements)))
            vidoes = elements[1]
            driver.execute_script("arguments[0].click();", vidoes)
            #warit for videos tab load
            wait.until(EC.visibility_of_element_located((By.TAG_NAME, "body")))
            wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "div#items.style-scope.ytd-grid-renderer div#dismissible")))
            #starting setting for looping
            flag = True
            videos_len = 0
            while flag:
                wait.until(EC.visibility_of_element_located((By.TAG_NAME, "body"))).send_keys(Keys.END)
                time.sleep(2)
                videos = driver.find_elements_by_css_selector('div#items.style-scope.ytd-grid-renderer div#dismissible')
                # print('size of videos: {}'.format(videos.size))
                print('len of videos: {}'.format(len(videos)))
                # # videos = videos.find_elements_by_css_selector('div#dismissible')
                # print('videos: ')
                # print(vidoes)
                # print('fifth video: ')
                # print(videos[4]) 
                if videos_len != len(videos):
                    videos_len = len(videos)
                else:
                    print('videos_len: {}'.format(videos_len))
                    flag = False
            #get information about all videos in target_channel
            page_source = driver.page_source 

    #parse channel videos
    soup = BeautifulSoup(page_source)
    video_titles = soup.select('div#details a#video-title')
    video_titles = [title.get_text() for title in video_titles]
    video_ids = soup.select('div#details a#video-title')
    video_ids = [id.get('href') for id in video_ids]
    view_counts = soup.select('div#details div#metadata-line')
    view_counts = [count.get_text() for count in view_counts]
    data = {'video_title': video_titles, 'video_id': video_ids, 'view_count': view_counts}
    data = pd.DataFrame(data)
    #clean data
    data['video_id'] = data['video_id'].str.replace(r'\/watch\?v=', '') #this is not working with normal string
    data['video_id'] = data['video_id'].str.replace(r'\/shorts\/', '') #have to use regex in this replace method
    number_dict = {'K': 1000, 'M': 1000000, 'B': 1000000000, 'T': 1000000000000}
    data['view_count'] = data['view_count'].str.split(r'\s')
    data['view_count'] = data['view_count'].apply(lambda x: x[0])
    data['view_count'] = data['view_count'].apply(lambda x: float(x[:-1]) * number_dict.get(x[-1])).astype('int')
    data['channel_id'] = channel_id
    return data

