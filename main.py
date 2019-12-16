from classDB import FootballDB
from classGetData import APIFootball

cl = FootballDB()
cl.openConnection()

data = APIFootball()

values = cl.get_league_to_load()

while values is not None:

    league_id = values[0]
    country_id = values[1]
    league_name = values[2] + " " + values[3]

    if data.getEvents(league_id, league_name):
        cl.save_event(country_id, data.eventsdata)

    if data.getPredictions(league_id, league_name):
        cl.save_predictions(data.predictiondata)

    cl.set_league_is_loaded(league_id)

    values = cl.get_league_to_load()


cl.closeConnection()