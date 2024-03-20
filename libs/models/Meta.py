from typing import List
from libs.models.Campanha import Campanha
from libs.models.Adset import Adset


class Meta:
    def __init__(self, campaigns: List[Campanha], adsets: List[Adset], id: str = None, name: str = None):
        self.id = id
        self.name = name
        self.campaigns = campaigns
        self.adsets = adsets

    @classmethod
    def from_dict(cls, data_dict):
        return cls(**data_dict)

    def to_dict(self):
        print(self.__dict__)
        return self.__dict__
