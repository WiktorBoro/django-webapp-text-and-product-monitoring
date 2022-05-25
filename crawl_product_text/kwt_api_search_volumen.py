from ..models import URL
from json import loads
from requests import post, get
from time import sleep


class KwtApi:
    def __init__(self,
                 token: str):
        self.token = token
        # main kw:search volume dict
        self.kw_serch_volumen_dict = dict()
        self.url_list = URL.objects.filter(token=self.token)

    def save_search_volumen_to_db(self):
        for key_word in self.url_list:
            if key_word.h1:
                URL.objects.filter(token=self.token, h1=key_word.h1, crawl_date=key_word.crawl_date).update(
                    h1_search_volumen=self.kw_serch_volumen_dict[f"{key_word.h1}"])

    def check_kwt_api_remaining(self):
        url_num = len(self.url_list)

        api_key_list = ["<YOUR API KEY>"]
        api_key = ""

        for apikey in api_key_list:
            url = "https://api.keywordtool.io/v2/quota"

            data = {
                "apikey": apikey,
            }

            response = get(url, json=data)
            try:
                # loop to check api key availability
                for period in ["minute", "daily", "monthly"]:
                    # api can send max 799 url in one call
                    if loads(response.text)["limits"][period]['remaining'] * 799 < url_num:
                        # if minute limits it's over but daily limits not we need wait 60 sec and we can use api key
                        if period == "minute" and loads(response.text)["limits"]["daily"]['remaining'] * 799 > url_num:
                            sleep(61)
                            raise TimeoutError
                        else:
                            # if this is last api key and its' finite
                            # we need to complete main dict with "end API" comment
                            if apikey == api_key_list[-1]:
                                for kw in self.url_list:
                                    dic = {f"{kw}": f"Koniec API w skali {period}"}
                                    self.kw_serch_volumen_dict.update(dic)
                                raise self.save_search_volumen_to_db()
                            else:
                                break
                    # if first "if" statement it's False we can use api key and don't check another period
                    raise TimeoutError
            except TimeoutError:
                api_key = apikey
                break
        return api_key

    def get_search_volumen_kwt_api(self, kw_list, api_key):
        url = "https://api.keywordtool.io/v2/search/volume/google"

        data = {
            "apikey": api_key,
            "keyword": kw_list,
            "metrics_location": [
                2616
            ],
            "metrics_language": [
                "pl"
            ],
            "metrics_network": "googlesearch",
            "metrics_currency": "PLN",
            "output": "json"
        }
        response = post(url, json=data)

        # iteration response dict
        for kw in kw_list:
            try:
                kw_volumen = loads(response.text)["results"][f"{kw.lower()}"]["volume"]
                dic = {f"{kw}": f"{kw_volumen}"}
            except:
                dic = {f"{kw}": "Error KWT API"}
            self.kw_serch_volumen_dict.update(dic)

    def get_kw_list(self):
        # list to stored 799 keyword of each iteration
        kw_list = []

        api_key = self.check_kwt_api_remaining()

        # filters H1 list from to long keywords
        for key_word in self.url_list:
            
            # 80 is max length for one word in API KWP
            if len(key_word.h1) > 79:
                dic = {f"{key_word.h1}": "Fraza zbyt długa"}
                self.kw_serch_volumen_dict.update(dic)
                continue

            # maximum word length is 9
            if len([space for space in key_word.h1 if space == " "]) > 9:
                dic = {f"{key_word.h1}": "Max kw len is 10"}
                self.kw_serch_volumen_dict.update(dic)
                continue

            # maximum word in one requests is 800
            if len(kw_list) == 799:
                self.get_search_volumen_kwt_api(kw_list=kw_list, api_key=api_key)
                kw_list.clear()
            kw_list.append(key_word.h1)

        # if len list is less than 799 and loop over
        if kw_list:
            self.get_search_volumen_kwt_api(kw_list=kw_list, api_key=api_key)
            kw_list.clear()

        # save result to db
        if self.kw_serch_volumen_dict:
            self.save_search_volumen_to_db()
