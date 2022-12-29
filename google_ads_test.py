import sys

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

#from main.models.gads_models import Gads
#from main.resource.db_helper import bulk_insert


config_dict = {
    "developer_token": "ek3hduE3C0h3dFEIMyyUew",
    "use_proto_plus": False,
    "client_id": "441559264124-7mmgsi8colhshqj8v6e48jsap8g8isgn.apps.googleusercontent.com",
    "client_secret": "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",
    "refresh_token": "1//0g136OSDDxfLuCgYIARAAGBASNwF-L9IrLhfyH-UJiV-u7-Xh-ueMvGSsIbT_HpASu1vkUuJao5jkCoIrNtVyBp0ehppXLhDPJXc",
    "login_customer_id": "2133678556"
}

#"login_customer_id": ""

client = GoogleAdsClient.load_from_dict(config_dict=config_dict, version="v9")

def get_google_ads_data(customer_id):

    print(type(customer_id))
    try:
        ga_service = client.get_service("GoogleAdsService")

        query = """
            SELECT campaign.name, metrics.clicks, campaign.id, campaign.labels,
             campaign.end_date, campaign.status, campaign.start_date,
             campaign.serving_status, campaign.target_cpm, metrics.impressions,
             campaign.campaign_budget, metrics.active_view_ctr, metrics.active_view_cpm,
             metrics.active_view_impressions, metrics.ctr, metrics.average_cpc, campaign_budget.status,
             campaign.base_campaign, campaign.bidding_strategy, campaign.bidding_strategy_type,
             campaign_budget.name,campaign_budget.period,campaign_budget.total_amount_micros,segments.date,segments.month FROM campaign
             WHERE segments.date BETWEEN '2022-01-01' AND '2022-02-28' ORDER BY campaign.id"""

        # Issues a search request using streaming.
        stream = ga_service.search_stream(customer_id=str(customer_id), query=query)

        # id = 0
        # data = []
        for batch in stream:
            for row in batch.results:
                 print(
                     f"Campaign with ID {row.campaign.id} and name "
                     f'"{row.campaign.name}" was found.'
                     f"With Clicks :- {row.metrics.clicks} and impressions:- "
                     f'"{row.metrics.impressions}" was found.'
                     f'"{(row.metrics.average_cpc)/1000000}" with average cpc'
                     f'"{row.metrics.ctr}" and with ctr'
                     f'"{row.segments.date}" and with date'
                     f'"{row.segments.month}" and with month'
                 )
                # ad_entity = Gads()
                # ad_entity.id = id
                # ad_entity.camp_id = row.campaign.id
                # ad_entity.camp_name = row.campaign.name
                # ad_entity.budget = int(row.campaign.campaign_budget.split("/")[-1])
                # ad_entity.clicks = row.metrics.clicks
                # ad_entity.impressions = row.metrics.impressions
                # ad_entity.ctr = row.metrics.ctr
                # ad_entity.avg_cpc = row.metrics.average_cpc
                # ad_entity.cost = row.metrics.cost_micros

                # data.append(ad_entity)
                # print(str(row))
                # id += 1

        #print("Total elements : " + str(len(data)))
        #bulk_insert(data)

    except GoogleAdsException as ex:
        print(
            f'Request with ID "{ex.request_id}" failed with status '
            f'"{ex.error.code().name}" and includes the following errors:'
        )
        for error in ex.failure.errors:
            print(f'\tError with message "{error.message}".')
            if error.location:
                for field_path_element in error.location.field_path_elements:
                    print(f"\t\tOn field: {field_path_element.field_name}")
        sys.exit(1)

if __name__ == "__main__":
    get_google_ads_data("3568082135")