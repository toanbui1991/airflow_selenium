import asyncio
import time
import pandas as pd
import re
from arsenic import get_session, keys, browsers, services
from datetime import datetime
from aiohttp import ClientTimeout
import MySQLdb
import sshtunnel
from config import config

#single task
async def scraper(video_id, limit):
    service = services.Chromedriver(binary='./chromedriver.exe')
    browser = browsers.Chrome()
    # browser.capabilities = {
    #     "goog:chromeOptions": {"args": ["--headless", "--disable-gpu", "--no-sandbox", "--disable-dev-shm-usage", "--lang=en-US"]}
    # }
    browser.capabilities = {
        "goog:chromeOptions": {"args": ["--lang=en-US"]}
    }
    timeout = ClientTimeout(
        total=None, # default value is 5 minutes, set to `None` for unlimited timeout
        connect=30, # how long to wait for new connection from conneciton pool
        sock_connect=30, # How long to wait before an open socket allowed to connect
        sock_read=30 # How long to wait with no data being read before timing out
    )
    async with limit:
        try:
            async with get_session(service, browser) as session:
                url = 'https://www.youtube.com/watch?v={}'.format(video_id)
                await session.get(url, timeout= timeout)
                body = await session.wait_for_element(5, 'body')
                # expand_button = await session.wait_for_element(5, "div#description tp-yt-paper-button#expand")
                # await expand_button.click()
                for _ in range(2):
                    await body.send_keys(keys.END)
                    time.sleep(2)
                #wait for number of comment element appear
                await session.wait_for_element(5, "h2#count yt-formatted-string span")
                #star parsing from here
                #have to await element first, and then wait to get text or attribute
                video_title =  await session.wait_for_element(5,'h1.title.style-scope.ytd-video-primary-info-renderer')
                video_title = await video_title.get_text()
                print('video_title: ', video_title)
                video_description = await session.wait_for_element(5, 'div#description')
                video_description = await video_description.get_text()
                print('video_descriptio: ', video_description)
                channel_id = await session.wait_for_element(5, 'div#upload-info a')
                channel_id = await channel_id.get_attribute('href')
                channel_id = channel_id.replace('/channel/', '')
                channel_title = await session.wait_for_element(5, 'div#upload-info a')
                channel_title = await channel_title.get_text()
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
                like_count = await session.wait_for_element(5, 'yt-formatted-string.style-scope.ytd-toggle-button-renderer.style-text')
                like_count = await like_count.get_text()
                number_dict = {'K': 1000, 'M': 1000000, 'B': 1000000000, 'T': 1000000000000}
                try:
                    like_count = float(like_count[:-1]) * number_dict.get(like_count[-1])
                    like_count = int(like_count)
                except Exception as e:
                    print('Exception: ', e)
                    like_count = None

                view_count = await session.wait_for_element(5, 'ytd-video-view-count-renderer.style-scope.ytd-video-primary-info-renderer span')
                view_count = await view_count.get_text()
                print('view_count: ', view_count)
                view_count = view_count.replace('views', '')
                view_count = re.sub(r'[,|\s]', '', view_count)
                print('view_count: ', view_count)
                # dislike_count = await session.wait_for_element(5, 'yt-formatted-string.style-scope.ytd-toggle-button-renderer.style-text')
                # dislike_count = await dislike_count[1].get_text()
                # dislike_count = dislike_count.replace('Dislike', '')
                # dislike_count = None if dislike_count else dislike_count
                dislike_count = None
                comment_count = await session.wait_for_element(5, 'h2#count yt-formatted-string span')
                comment_count = await comment_count.get_text()
                comment_count = re.sub(r'[,|\s]', '', comment_count)
                # thumbnails_url
                # thumbnails_width
                # thumbnails_height
                published_ts = await session.wait_for_element(5, 'div#info-strings yt-formatted-string')
                published_ts = await published_ts.get_text()
                print('published_ts: ')
                print(published_ts)
                try: 
                    published_ts = datetime.strptime(published_ts, '%b %d, %Y')
                except Exception as e:
                    print('Exception: ', e)
                    published_ts = None
                item = {'video_id': video_id, 'video_title': video_title, 'video_description': video_description, 'channel_id': channel_id, 'channel_title': channel_title,
                'like_count': like_count, 'view_count': view_count, 'dislike_count': dislike_count, 'comment_count': comment_count, 'published_ts': published_ts}
                # print('item: ')
                # print(item)

        except Exception as e:
            print('Exception: ', e)
        else:

            #update data to mysql target_table if can parse video
            target_table = 'tb_youtube_vdo_info_stg'
            values = list(item.keys())
            values = ["{} = %s".format(value) for value in values ]
            values_str = ", ".join(values)
            update_key = 'video_id'
            condition_str = "{} = %s".format(update_key)
            query = """UPDATE {} SET {} WHERE {};""".format(target_table, values_str, condition_str)
            params = list(item.values())
            params.append(video_id)
            with sshtunnel.SSHTunnelForwarder(
                config['remote_machine']['hostname'],
                ssh_username=config['remote_machine']['ssh_username'], 
                ssh_password=config['remote_machine']['ssh_password'],
                remote_bind_address=(config['remote_machine']['msql_hostname'], 3306)
            ) as tunnel:
                con = MySQLdb.connect(
                    user=config['mysql_dev']['user'],
                    passwd=config['mysql_dev']['passwd'],
                    host='127.0.0.1', #have to set this exact value
                    port=tunnel.local_bind_port,
                    db=config['mysql_dev']['db'],
                )
                #truncate table before insert so that do not violate unique constraint
                try:
                    cursor = con.cursor()
                    print('item: ')
                    print(item)
                    print('query: ')
                    print(query)
                    print('params: ')
                    print(params)
                    cursor.execute(query, params)
                    con.commit()
                except Exception as e:
                    con.rollback()
                    raise Exception(e)
                finally:
                    cursor.close()



async def scraper_all(video_ids, limit):

    tasks = []
    for url in video_ids:
        task = asyncio.create_task(scraper(url, limit))
        tasks.append(task)
    #return_exceptions=False exception treat as successful
    response = await asyncio.gather(*tasks, return_exceptions=False)
    return response       
    

#run multiple task as one
async def main(video_ids, num_per_batch=10):
    limit = asyncio.Semaphore(num_per_batch)
    await scraper_all(video_ids, limit)
