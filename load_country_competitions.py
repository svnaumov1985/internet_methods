from classDB import FootballDB
from classGetData import APIFootball

cl = FootballDB()
cl.openConnection()
cl.createDataBase()

data = APIFootball()

data.getCountries()

cl.save_countries(data.countrydata)

for c_d in data.countrydata:
    data.getCompetitions(c_d)
    cl.save_league(data.competionsdata)

cl.closeConnection()