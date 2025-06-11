import json
import requests
import pandas as pd
from time import sleep

# 初始化存储所有数据的列表
all_episodes = []
all_transcripts = []


for page in range(1, 100):
    start = (page-1)*20

    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'zh-CN,zh;q=0.9',
        'authorization': 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2FwaS5wb2RjaGFzZXIuY29tL2F1dGgvcmVmcmVzaCIsImlhdCI6MTczOTQxMjg3NSwiZXhwIjoxNzM5ODQ4OTE0LCJuYmYiOjE3Mzk4NDUzMTQsImp0aSI6IncyYjdwaUh5RXR3RXRwdnMiLCJzdWIiOiI2MDUwMTMiLCJwcnYiOiI2YzAzYjQ2Y2NiYWRkNDViZjAwNDMwN2VkYmZkZmYzZjM5MTVhOGU0In0.oKpO0ilnub71njK_oY26g5Kt9dSea8YMj-iM5UertMg',
        'cache-control': 'no-cache',
        'content-type': 'application/json;charset=UTF-8',
        'origin': 'https://www.podchaser.com',
        'pragma': 'no-cache',
        'priority': 'u=1, i',
        'referer': 'https://www.podchaser.com/',
        'sec-ch-ua': '"Not A(Brand";v="8", "Chromium";v="132", "Google Chrome";v="132"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36',
    }

    json_data = {
        'start': start,
        'count': 20,
        'sort_order': 'SORT_ORDER_RECENT',
        'sort_direction': 'desc',
        'filters': {
            'podcast_id': 72263,
        },
        'options': {},
        'omit_results': False,
        'total_hits': False,
    }
    print(f'开始请求第 {page} 页,从{start}开始')
    #response = requests.post('https://api.podchaser.com/list/episode', headers=headers, json=json_data)
        # 请求剧集列表
    #json_data = {
            #'start': start,
           # 'count': 20,
           # 'sort_order': 'SORT_ORDER_RECENT',
           # 'filters': {'podcast_id': 72263},
       #

    try:
            response = requests.post(
                'https://api.podchaser.com/list/episode',
                headers=headers,
                json=json_data
            )
            response.raise_for_status()   # 检查HTTP错误
    except requests.exceptions.RequestException as e:
        print(f"请求剧集列表失败: {e}")

    try:
            data = response.json()
            entities = data['entities']
            print(data)
    except (json.JSONDecodeError, KeyError) as e:
            print(f"解析剧集数据失败: {e}")
    

        # 处理每个剧集
    for idx, episode in enumerate(entities, 1):
        episode_id = episode['id']
            
            # 存储主信息
        episode_info = {
                'episode_id': episode_id,
                'release_date': episode.get('air_date'),
                'duration': episode.get('length'),
                'title': episode.get('title'),
                'description': episode.get('description'),
            }
        all_episodes.append(episode_info)
            # 请求转录文本
        print(f'开始爬取分页页面: {idx} {episode_id}')
        transcript_url = f'https://api.podchaser.com/episodes/{episode_id}/transcript'
        try:
            transcript_res = requests.get(
                        transcript_url,
                        headers={'User-Agent': headers['user-agent']}
                    )
            transcript_res.raise_for_status()
            transcript_data = transcript_res.json()
        except Exception as e:
                    print(f"{episode_id}无transcript内容")
                    transcript_data = []

                # 处理转录内容
        for utterance in transcript_data:
                    all_transcripts.append({
                        'episode_id': episode_id,
                        'utterance': utterance.get('utterance'),
                        'start_time': utterance['timestamp'][0],
                        'end_time': utterance['timestamp'][1]
                    })
                
                # 礼貌性延迟
        sleep(1)

# 创建DataFrame
df_episodes = pd.DataFrame(all_episodes)
df_transcripts = pd.DataFrame(all_transcripts)

print("剧集数据：")
print(df_episodes.head())
print("\n转录数据：")
print(df_transcripts.head())

print(all_episodes)
# 保存到CSV
df_episodes.to_excel('episodes.xlsx', index=False)
df_transcripts.to_excel('transcripts.xlsx', index=False) 