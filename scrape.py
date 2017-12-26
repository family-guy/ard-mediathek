import json
import os
import re
import shutil
import time
import urllib.parse

from bs4 import BeautifulSoup
import requests

BASE_URL = 'http://www.ardmediathek.de'
DOWNLOAD_DIR = '/home/guy/Videos'
FILE_EXTENSIONS = ('mp4', 'ts', )
LAST_DAY = 7


def parse_item(item):
    video_urls = {}
    for teaser in item.select('.teaser'):
        if teaser.select('.subtitle'):
            parts = teaser.select('.subtitle')[0].text.split('|')
            if len(parts) >= 2 and re.search('UT', parts[1]):
                url = teaser.select('.mediaCon .media a')[0].attrs['href']
                video_urls[url] = {
                    'title': teaser.select('a h4')[0].text,
                    'document_id': urllib.parse.parse_qs(
                        url)['documentId'][0],
                }
    return video_urls


def get_films(channels):
    print('Extracting subtitled films...')
    films = {}
    for channel, value in channels.items():
        print('Searching channel {}'.format(channel))
        channel_dir = os.path.join(DOWNLOAD_DIR, channel)
        if not os.path.isdir(channel_dir):
            os.mkdir(channel_dir)
        for day in range(LAST_DAY):
            params = {
                'tag': day,
                'kanal': value['id'],
            }
            r = requests.get(
                '{}/tv/sendungVerpasst'.format(BASE_URL), params=params)
            if 200 <= r.status_code < 300:
                programme_list_soup = BeautifulSoup(r.text, 'html5lib')
                items = programme_list_soup.select('.entries .teaserbox')
                for item in items:
                    video_urls = parse_item(item)
                    for url, metadata in video_urls.items():
                        metadata['channel'] = channel
                        category = url.split('/')[2]
                        if re.search('film', category, re.IGNORECASE):
                            film_url = '{}{}'.format(BASE_URL, url)
                            print('Extracted url {}'.format(film_url))
                            document_id = metadata['document_id']
                            downloaded = False
                            for file_extension in FILE_EXTENSIONS:
                                if os.path.isfile(
                                        os.path.join(
                                            DOWNLOAD_DIR,
                                            channel,
                                            document_id,
                                            '{}-video.{}'.format(
                                                document_id, file_extension)
                                        )
                                ):
                                    print(
                                        'Film {} already downloaded'
                                        .format(document_id))
                                    downloaded = True
                                    break
                            metadata['downloaded'] = downloaded
                            films[film_url] = metadata
            else:
                continue
    print('{} films found'.format(len(films)))
    return films


def download_video_in_chunks(url, file_path):
    r = requests.get(url)
    lines = r.text.split('\n')
    video_data = bytearray()
    for line in lines:
        if line.startswith('http') and 'segment' in line:
            fragment_url = line
            print('Processing fragment url {}'.format(fragment_url))
            s = requests.get(fragment_url)
            video_data.extend(s.content)
    with open(file_path, 'wb') as f:
        f.write(video_data)


def download_video(url, file_path):
    r = requests.get(url, stream=True)
    if 200 <= r.status_code < 300:
        with open(file_path, 'wb') as f:
            shutil.copyfileobj(r.raw, f)
        print('Video successfully downloaded from {}'.format(url))
        return True
    print('Unable to download video from {}'.format(url))
    return False


def download_subtitles(url, file_path, title, description):
    r = requests.get(url)
    soup = BeautifulSoup(r.text, 'xml')
    subtitles = '{}\n\n{}\n\n'.format(title, description)
    for tag in soup.find_all('tt:span'):
        subtitles += tag.text + '\n'
    with open(file_path, 'w') as f:
        f.write(subtitles)
    print('Subtitles successfully downloaded from {}'.format(url))


def take_break():
    print('#' * 50)
    print('#' * 50)
    print('Taking a break...')
    time.sleep(3)
    print('Break over')
    print('#' * 50)
    print('#' * 50)


def get_film_description(url):
    r = requests.get(url)
    soup = BeautifulSoup(r.text, 'html5lib')
    return soup.select('.teaser p[itemprop="description"]')[0].text


def process_videos(films, chunks=False):
    print('Processing videos...')
    print('{} urls received'.format(len(films)))
    downloaded = len(
        [url for url, metadata in films.items() if metadata['downloaded']])
    to_download = len(films) - downloaded
    print('{} urls already downloaded'.format(downloaded))
    print('Attempting to download {} urls'.format(to_download))
    i = 0
    for url, metadata in films.items():
        if not metadata['downloaded']:
            i += 1
            print('{}/{}'.format(i, to_download))
            media_file_path = os.path.join(
                DOWNLOAD_DIR,
                metadata['channel'],
                metadata['document_id'],
            )
            if not os.path.isdir(media_file_path):
                os.mkdir(media_file_path)
            print('Processing video for url {}'.format(url))
            document_id = metadata['document_id']
            metadata['description'] = get_film_description(url)
            if not chunks:
                r = requests.get(
                    'http://www.ardmediathek.de/play/media/{}'.format(
                        document_id),
                    params={
                        'devicetype': 'pc',
                        'features': '',
                    }
                )
                if 200 <= r.status_code < 300:
                    media = r.json()
                    if media['_geoblocked']:
                        print(
                            'Geoblocked, may have problems downloading media')
                    subtitles_url = media['_subtitleUrl']
                    if subtitles_url:
                        print(
                            'Downloading subtitles from {} for url {}'
                            .format(subtitles_url, url))
                        subtitles_file_path = os.path.join(
                            media_file_path,
                            '{}-subtitles.txt'.format(document_id)
                        )
                        download_subtitles(
                            subtitles_url,
                            subtitles_file_path,
                            metadata['title'],
                            metadata['description']
                        )
                    else:
                        print('Subtitles unavailable for url {}'.format(url))
                    media_array = media['_mediaArray']
                    if media_array:
                        streams = [stream for x in media_array for stream in
                                   x['_mediaStreamArray']]
                        if streams:
                            best_quality = -1
                            best_stream = ''
                            for stream in streams:
                                if stream['_quality'] == 'auto':
                                    continue
                                if stream['_quality'] > best_quality:
                                    best_quality = stream['_quality']
                                    if isinstance(stream['_stream'], list):
                                        best_stream = stream['_stream'][-1]
                                    else:
                                        best_stream = stream['_stream']
                            if not urllib.parse.urlparse(best_stream).scheme:
                                best_stream = 'http:{}'.format(best_stream)
                            video_file_path = os.path.join(
                                media_file_path,
                                '{}-video.mp4'.format(document_id)
                            )
                            print(
                                'Downloading video from {} for url {}'.format(
                                    best_stream, url))
                            if download_video(best_stream, video_file_path):
                                take_break()
                                continue
            print('Unable to download video in one file or chunks set to true.'
                  ' Downloading video in chunks without subtitles')
            s = requests.get(
                'http://www.ardmediathek.de/play/config/{}'.format(
                    document_id),
                params={'devicetype': 'pc'}
            )
            asset_id = s.json()['_pixelConfig'][0]['agfMetaDataSDK']['assetid']
            if not urllib.parse.urlparse(asset_id).scheme:
                asset_id = 'http:{}'.format(asset_id)
            t = requests.get(asset_id)
            master_m3u8 = t.text
            index_urls = [line for line in master_m3u8.split('\n')
                          if line.startswith('http') and 'index' in line
                          and 'av.m3u8' in line]
            if index_urls:
                index_url = index_urls[0]  # need to pick resolution
                video_file_path = os.path.join(
                                    media_file_path,
                                    '{}-video.ts'.format(document_id)
                                )
                print('Downloading video from {} for {}'.format(
                    index_url, url))
                download_video_in_chunks(index_url, video_file_path)
                take_break()
            else:
                print('Unable to download video in chunks (possible that'
                      ' streaming is geo-blocked) for {}'.format(url))


with open('channels.json') as f:
    channels = json.load(f)

for channel, value in channels.items():
    value['video_urls'] = []

films = get_films(channels)
process_videos(films)
