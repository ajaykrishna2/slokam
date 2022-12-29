import requests
## 1st api to get post ids
# params = {
#     'period': 'day',
#     'since': '1609439400',
#     'until': '1649183399',
#     'limit': '50',
#     'access_token': 'EAANiAUmOkqEBAOgnWKgM2SzGTis43OKY5FXsV7sZCcXDNBSRYbn46FZBUTXedhfWuKnlTItFe0Y3O2Gg7V1cHivpuuBRgLv5Fezu3vBTYiG2o6TxzVRmTWEe4QMrk6I8O6cpkGQOzb0eZCAJFAkHC6tymy2KPhPnjXRwD8ua5Mvzaf3nU0a',
# }
#
# response = requests.get('https://graph.facebook.com/v13.0/102663271454005/posts', params=params)

## 2nd api to get post metrics

params = {

    'metric': 'post_impressions,post_clicks_unique,post_impressions_unique,post_engaged_users',
    'access_token': 'EAANiAUmOkqEBAOgnWKgM2SzGTis43OKY5FXsV7sZCcXDNBSRYbn46FZBUTXedhfWuKnlTItFe0Y3O2Gg7V1cHivpuuBRgLv5Fezu3vBTYiG2o6TxzVRmTWEe4QMrk6I8O6cpkGQOzb0eZCAJFAkHC6tymy2KPhPnjXRwD8ua5Mvzaf3nU0a',
}

response = requests.get('https://graph.facebook.com/v13.0/102663271454005_461473148906347/102663271454005_223974049322926/insights', params=params)
print(response.json())