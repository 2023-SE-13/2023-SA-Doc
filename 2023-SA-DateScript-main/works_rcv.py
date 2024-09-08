import os
import time
import json
import gzip
import requests
from datetime import datetime
from elasticsearch import Elasticsearch, helpers
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Elasticsearch 配置
ES_HOST = "localhost"
ES_PORT = 9200
ES_USERNAME = "elastic"  # 替换为您的 Elasticsearch 用户名
ES_PASSWORD = "yXC0ZTAbjmhmyLHb7fBv"  # 替换为您的 Elasticsearch 密码
INDEX_NAME = "new_works"         # 根据您的需求更改
DOC_TYPE = "_doc"

CORP_ID = "ww139c8045e371e29e"
CORP_SECRET = "CjNHTG1-4cozBQpCo2WWnLb7weyCT0Bc0pzdGNp9Lfk"
ACCESS_TOKEN = None
AGENT_ID = "1000003"


# 初始化 Elasticsearch 客户端
es = Elasticsearch(
    [{
        'scheme': 'http',
        'host': ES_HOST, 
        'port': ES_PORT
    }],
    basic_auth=(ES_USERNAME, ES_PASSWORD)
)


def get_access_token():
    """
    向微信企业API发送GET请求以获取access_token。
    """
    url = f"https://qyapi.weixin.qq.com/cgi-bin/gettoken?corpid={CORP_ID}&corpsecret={CORP_SECRET}"
    response = requests.get(url)
    if response.status_code == 200:
        print("access_token get success")
        return response.json().get('access_token')
    else:
        raise Exception("Failed to get access token")


def send_message(message):
    """
    向微信企业API发送POST请求以发送消息。
    """
    global ACCESS_TOKEN
    if not ACCESS_TOKEN:
        ACCESS_TOKEN = get_access_token()

    message_data = {
        "touser": "ZhouXingDa",
        "msgtype": "text",
        "agentid": AGENT_ID,
        "text": {
            "content": message
        },
        "safe": 0
    }

    url = f"https://qyapi.weixin.qq.com/cgi-bin/message/send?access_token={ACCESS_TOKEN}"
    response = requests.post(url, json=message_data)

    if response.status_code == 200 and response.json().get("errcode") == 0:
        print("send success")
        return response.json()
    else:
        # 如果发送失败，尝试重新获取access_token并重发
        print(response.json())
        ACCESS_TOKEN = get_access_token()
        url = f"https://qyapi.weixin.qq.com/cgi-bin/message/send?access_token={ACCESS_TOKEN}"
        response = requests.post(url, json=message_data)
        return response.json()


def decompress_gz(gz_path, output_path):
    with gzip.open(gz_path, 'rb') as f_in:
        with open(output_path, 'wb') as f_out:
            f_out.write(f_in.read())


# 准备批量上传的函数
def bulk_index(file_path):
    json_data = {}
    try:
        with open(file_path, 'r') as file:
            actions = []
            count = 0
            
            for line in file:
                json_data = json.loads(line)  # 解析每一行为 JSON
                work_id = json_data.get('wid')
                action = {
                    "_index": INDEX_NAME,
                    "_id": work_id,
                    "_source": json_data
                }
                actions.append(action)
                count += 1

                # 每读取100个数据就进行一次批量上传
                if count % 1000 == 0:
                    helpers.bulk(es, actions)
                    actions = []  # 清空列表以便下一批数据

            # 处理剩余的数据（如果有）
            if actions:
                helpers.bulk(es, actions)

        return True
    except Exception as e:
        print(f"Error indexing file {file_path}: {e}")
        print(f"{json.dumps(json_data)}")
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        text_message = f"Error indexing file {file_path} at {current_time}: {e}"
        send_message(text_message)
        return False


# Elasticsearch 配置和初始化，以及 bulk_index 函数保持不变

class Watcher:
    DIRECTORY_TO_WATCH = "./works"

    def __init__(self):
        self.observer = Observer()

    def count_files(self, extensions):
        """统计目录中特定扩展名文件的数量"""
        files = [f for f in os.listdir(self.DIRECTORY_TO_WATCH) if any(f.endswith(ext) for ext in extensions)]
        return len(files)

    def run(self):
        event_handler = Handler()
        self.observer.schedule(event_handler, self.DIRECTORY_TO_WATCH, recursive=True)
        self.observer.start()
        try:
            while True:
                time.sleep(5)
        except:
            self.observer.stop()
            print("Observer stopped")
        self.observer.join()


class Handler(FileSystemEventHandler):
    @staticmethod
    def process_json_file(json_file_path):
        """处理JSON文件的通用逻辑"""
        print(f"Processing JSON file: {json_file_path}")
        if bulk_index(json_file_path):
            os.remove(json_file_path)
            print(f"Finished indexing and deleted file: {json_file_path}")
            send_message(f"Finished indexing and deleted file: {json_file_path}")
        else:
            print(f"Failed to index file: {json_file_path}")


    @staticmethod
    def on_created(event):
        if event.is_directory:
            return None

        if event.event_type == 'created':
            if event.src_path.endswith('.done'):
                # 处理.done文件
                json_file_path = event.src_path[:-5]  # 获取JSON文件路径
                print(f"Detected .done file for {json_file_path}")

                if os.path.exists(json_file_path):
                    Handler.process_json_file(json_file_path)
                else:
                    print("JSON file not found")
                # 删除处理过的.done或.gzdone文件
                os.remove(event.src_path)

            elif event.src_path.endswith('.gzdone'):
                # 处理.gzdone文件
                gzip_file_path = event.src_path[:-7]  # 获取gzip文件路径
                json_file_path = gzip_file_path[:-3]  # 获取JSON文件路径
                print(f"Detected .gzdone file for {gzip_file_path}")

                if os.path.exists(gzip_file_path):
                    decompress_gz(gzip_file_path, json_file_path)
                    print(f"Decompressed file: {json_file_path}")
                    Handler.process_json_file(json_file_path)
                    os.remove(gzip_file_path)
                else:
                    print("Gzip file not found")
                # 删除处理过的.done或.gzdone文件
                os.remove(event.src_path)

            


if __name__ == '__main__':
    send_message("Server IS Ready to receive and index works!!!")
    w = Watcher()
    w.run()