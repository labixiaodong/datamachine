import requests


def data_msg(content, subject):
    json_data = {
            "msgtype": "text",
            "text": {
                "content": '报警：\n '+content,  # 发送内容
            },
            "at": {
                "atMobiles": [
                ],
                "isAtAll": False  # 是否要@某位用户
            }
        }

    ding_url = subject
    requests.post(url=ding_url, json=json_data)
    print('钉钉信息发送成功。')


if __name__ == '__main__':
    data_msg()