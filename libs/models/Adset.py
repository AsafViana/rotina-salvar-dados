class Adset:
    def __init__(self, id=None, name=None, adset_name=None, adset_id=None, campaign_id=None, campaign_name=None, account_id=None,
                 account_name=None, clicks=None,
                 cpp=None, ctr=None, cpc=None, date_start=None, date_stop=None, frequency=None, impressions=None,
                 inline_link_clicks=None,
                 inline_link_click_ctr=None, cost_per_inline_link_click=None, cost_per_unique_inline_link_click=None,
                 inline_post_engagement=None, cost_per_inline_post_engagement=None, objective=None, reach=None, spend=None,
                 full_view_impressions=None, purchase_roas=None, video_p25_watched_actions=None, video_p50_watched_actions=None,
                 video_p75_watched_actions=None, video_p100_watched_actions=None):
        self.id = id
        self.name = name
        self.campaign_id = campaign_id
        self.campaign_name = campaign_name
        self.account_id = account_id
        self.account_name = account_name
        self.clicks = clicks
        self.cpp = cpp
        self.ctr = ctr
        self.cpc = cpc
        self.date_start = date_start
        self.date_stop = date_stop
        self.frequency = frequency
        self.impressions = impressions
        self.inline_link_clicks = inline_link_clicks
        self.inline_link_click_ctr = inline_link_click_ctr
        self.cost_per_inline_link_click = cost_per_inline_link_click
        self.cost_per_unique_inline_link_click = cost_per_unique_inline_link_click
        self.inline_post_engagement = inline_post_engagement
        self.cost_per_inline_post_engagement = cost_per_inline_post_engagement
        self.objective = objective
        self.reach = reach
        self.spend = spend
        self.full_view_impressions = full_view_impressions
        self.purchase_roas = purchase_roas
        self.video_p25_watched_actions = video_p25_watched_actions
        self.video_p50_watched_actions = video_p50_watched_actions
        self.video_p75_watched_actions = video_p75_watched_actions
        self.video_p100_watched_actions = video_p100_watched_actions

    @classmethod
    def from_dict(cls, data_dict):
        return cls(**data_dict)

    @classmethod
    def to_dict(self):
        return self.__dict__
