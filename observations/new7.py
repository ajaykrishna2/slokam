import requests

# slack access bot token
slack_token = "https://hooks.slack.com/services/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"

data = {
    'token': slack_token,
    'channel': 'U03CR6B0T9Q',    # User ID.
    'as_user': True,
    'text': "GO Home!"
}

requests.post(url='https://slack.com/api/chat.postMessage',
              data=data)