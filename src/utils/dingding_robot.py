import requests


def data_msg(content):
    json_data = {
            "msgtype": "text",
            "text": {
                "content": '报警： '+content,  # 发送内容
            },
            "at": {
                "atMobiles": [
                ],
                "isAtAll": False  # 是否要@某位用户
            }
        }

    ding_url = 'https://oapi.dingtalk.com/robot/send?access_token=f33d8d962bf5d7118cb88030f8dba9fe7e6a42b14746e7d644361a987dc0ccf3'
    requests.post(url=ding_url, json=json_data)
    print('信息发送成功。')


def error_msg():
    try:
        a = [1, 3, 5]
        x = a[7]
        print(x)
    except IndexError as e:
        error_content = '错误信息是： ' + str(e)
        data_msg(error_content)


if __name__ == '__main__':
    error_msg()