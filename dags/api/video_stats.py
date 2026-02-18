import requests 
import json
from datetime import date
# import os
# from dotenv import load_dotenv

from airflow.decorators import task
from airflow.models import Variable

# load_dotenv(dotenv_path="./.env")

API_KEY = Variable.get("API_KEY")
CHANNEL_HANDLE = Variable.get("CHANNEL_HANDLE")
maxResults = 50

@task
def get_playlist_id():
    try:
        handle = CHANNEL_HANDLE.lstrip("@")

        # Try forHandle first
        url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={handle}&key={API_KEY}"
        r = requests.get(url)
        r.raise_for_status()
        data = r.json()

        items = data.get("items", [])
        if items:
            return items[0]["contentDetails"]["relatedPlaylists"]["uploads"]

        # Fallback: resolve channelId via search
        s_url = f"https://youtube.googleapis.com/youtube/v3/search?part=snippet&type=channel&maxResults=1&q={handle}&key={API_KEY}"
        s = requests.get(s_url)
        s.raise_for_status()
        sdata = s.json()

        sitems = sdata.get("items", [])
        if not sitems:
            raise Exception(
                f"Could not resolve channel from handle={handle}. "
                f"channels(forHandle) items=0; search items=0. "
                f"channels response: {json.dumps(data)[:1200]} "
                f"search response: {json.dumps(sdata)[:1200]}"
            )

        channel_id = sitems[0]["snippet"]["channelId"]

        # Get uploads playlist by channel ID (most reliable)
        c_url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&id={channel_id}&key={API_KEY}"
        c = requests.get(c_url)
        c.raise_for_status()
        cdata = c.json()

        citems = cdata.get("items", [])
        if not citems:
            raise Exception(f"Resolved channelId={channel_id} but channels?id returned 0 items. Response: {json.dumps(cdata)[:1200]}")

        return citems[0]["contentDetails"]["relatedPlaylists"]["uploads"]

    except requests.exceptions.RequestException as e:
        raise e

# def get_playlist_id():
#     try:

#         url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}"
#         response = requests.get(url)
#         response.raise_for_status()
#         data = response.json()
#         # print(json.dumps(data,indent=4))

#         channel_items = data['items'][0]
#         channel_playlistId = channel_items['contentDetails']['relatedPlaylists']['uploads']
#         # print(channel_playlistId)
#         return channel_playlistId
        
#     except requests.exceptions.RequestException as e:
#         raise e
    
@task
def get_video_ids(playlistId):
     base_url = f"https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults={maxResults}&playlistId={playlistId}&key={API_KEY}"
     video_ids = []
     pageToken = None  # ✅ MUST exist before "if pageToken:"


     try:
          while True:
               url = base_url
               if pageToken:
                    url += f"&pageToken={pageToken}"
               response = requests.get(url)
               response.raise_for_status()
               data = response.json()

               for item in data.get('items', []):
                    video_id = item['contentDetails']['videoId']
                    video_ids.append(video_id)

               pageToken = data.get('nextPageToken')

               if not pageToken:
                    break
          return video_ids

     except requests.exceptions.RequestException as e:
          raise e

@task
def extract_video_data(video_ids):
     extracted_data = []

     def batch_list(video_id_list, batch_size):
        for video_id in range(0, len(video_id_list), batch_size):
          yield video_id_list[video_id: video_id + batch_size]

     try:
        for batch in batch_list(video_ids, maxResults):
            video_ids_str = ",".join(batch)
            url = f"https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails&part=snippet&part=statistics&id={video_ids_str}&key={API_KEY}"
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            for item in data.get('items',[]):
                video_id = item['id']
                snippet = item['snippet']
                contentDetails = item['contentDetails']
                statistics = item['statistics']

                video_data = {
                    "video_id": video_id,
                    "title": snippet["title"],
                    "publishedAt": snippet["publishedAt"],
                    "duration":contentDetails["duration"],
                    "viewCount": statistics.get("viewCount", None),
                    "likeCount":statistics.get("likeCount", None),
                    "commentCount": statistics.get("commentCount", None)
                }
                extracted_data.append(video_data)
        return extracted_data

     except requests.exceptions.RequestException as e:
         raise e 

@task
def save_to_json(extracted_data):
    file_path = f"./data/Youtube_data_{date.today()}.json"

    with open(file_path,"w",encoding="utf-8") as json_outfile:
        json.dump(extracted_data, json_outfile, indent=4, ensure_ascii=False)


if __name__ == "__main__":
    playlistId = get_playlist_id()
    video_ids = get_video_ids(playlistId)
    video_data = extract_video_data(video_ids)
    save_to_json(video_data)
    # print(get_video_ids(playlistId))
