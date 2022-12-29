import json
import sys
import random
import requests
if __name__ == '__main__':
    url = "https://hooks.slack.com/services/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"
    try:
        # url = "https://hooks.slack.com/services/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"
        message = ("A Sample Message")
        title = (f"New Incoming Message :zap:")
        slack_data = {
            "username": "Ajay P",
            "icon_emoji": ":satellite:",
            "text": "hi team",
            # "channel" : "#somerandomcahnnel",
            "attachments": [
                {
                    "color": "#9733EE",
                    "fields": [
                        {
                            "title": title,
                            "value": message,
                            "short": "false",
                        }
                    ]
                }
            ]
        }

        byte_length = str(sys.getsizeof(slack_data))
        headers = {'Content-Type': "application/json", 'Content-Length': byte_length}
        # headers=100
        r = 1 / 0
        response = requests.post(url, data=json.dumps(slack_data), headers=headers)


    except Exception as exe:

        message = ("A  Message")
        title = (f"Error :zap:")
        slack_data = {
            "username": "Ajay P",
            "icon_emoji": ":satellite:",
            "text":"a",
             "response_metadata": {
        "warnings": [
            exe
        ],
        "messages": [
            'exe'
        ]
    }
            # "warnings":exe
            # "channel" : "#somerandomcahnnel",
            # "attachments": [
            #     {
            #         "color": "#9733EE",
            #         "fields": [
            #             {
            #                 "title": title,
            #                 "value": message,
            #                 "short": "false",
            #             }
            #         ]
            #     }
            # ]
        }

        byte_length = str(sys.getsizeof(slack_data))
        headers = {'Content-Type': "application/json", 'Content-Length': byte_length}
        # headers=100
        response = requests.post(url, data=json.dumps(slack_data), headers=headers)
        # print(exe)
        # print(response.status_code)
        # print(response.text)
        if response.status_code != 200:
            raise Exception(response.status_code, response.text)
