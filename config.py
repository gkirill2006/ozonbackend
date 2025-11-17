import os
API_KEY = os.getenv("API_KEY")



# submit_campaign_reports_for_day(date_str="2025-10-02", store_id=None, batch_size=10, retry_interval_sec=10, campaign_kind="manual")
# submit_campaign_reports_for_day(date_str="2025-09-30", store_id=None, batch_size=10, retry_interval_sec=10, campaign_kind="manual")

# docker logs -f --tail 1000 markets_celery | grep "submit_auto_reports_for_yesterday"