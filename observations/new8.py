import requests
def post_image():
    url="https://slack.com/api/chat.postMessage"
    data = {

        "token": "https://hooks.slack.com/services/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",
        "channels": ['#data'],
        "text":"Message to send",
    }

    response = requests.post(
         url=url, data=data,
         headers={"Content-Type": "application/json"})

    #response = requests.post(url=url, data=payload, params=data, files=file_upload)
    if response.status_code == 200:
        print("successfully completed post_reports_to_slack "
                      "and status code %s" % response.status_code)
    else:
        print("Failed to post report on slack channel "
                      "and status code %s" % response.status_code)