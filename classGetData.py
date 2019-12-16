import requests


class APIFootball:

    apikey = "8bd895bff24885bf8a8121199638a50604231fb8016c2f0824d19fca7a3f804e"
    site = "https://apiv2.apifootball.com/"
    countrydata = None
    competionsdata = None
    eventsdata = None
    predictiondata = None

    TestMode = True

    def getCountries(self):

        response = requests.get(f"{self.site}?action=get_countries&APIkey={self.apikey}")
        if response.status_code != 200:
            print("!!! Ошибка: Данные по странам")
            return False

        self.countrydata = response.json()
        print("Данные по странам получены")

        return True


    def getCompetitions(self, data):

        c_id = data["country_id"]
        c_name = data["country_name"]

        reqstr = f"{self.site}?action=get_leagues&country_id={c_id}&APIkey={self.apikey}"
        response = requests.get(reqstr)
        if response.status_code != 200:
            print(f"!!! Ошибка: Данные по турнирам {c_name}")
            return False

        self.competionsdata = response.json()
        print(f"Данные по турнирам получены {c_name}")

        return True


    def getEvents(self, c_id, c_name):

        reqstr = f"{self.site}?action=get_events&from=1900-01-01&to=2999-01-01&league_id={c_id}&APIkey={self.apikey}"
        response = requests.get(reqstr)
        if response.status_code != 200:
            print(f"!!! Ошибка: Данные по турнирам по {c_name}")
            return False

        rezvalue = response.json()

        if type(rezvalue) == dict and rezvalue.get("error") == 404:
            self.predictiondata = None
            print(f"!!! Ошибка получения данных по событию {c_id} {c_name}")
            return False

        self.eventsdata = rezvalue
        print(f"Данные по событиям получены {c_name}")

        return True

    def getPredictions(self, c_id, c_name):

        reqstr = f"{self.site}?action=get_predictions&from=1900-01-01&to=2999-01-01&league_id={c_id}&APIkey={self.apikey}"
        response = requests.get(reqstr)
        if response.status_code != 200:
            print(f"!!! Ошибка: Данные по прогнозам {c_name}")
            return False

        rezvalue = response.json()

        if type(rezvalue) == dict and rezvalue.get("error") == 404:
            print(f"!!! Ошибка получения прогноза по событию {c_id} {c_name}")
            self.predictiondata = None
            return False

        self.predictiondata = rezvalue
        print(f"Данные по прогнозам получены {c_name}")

        return True