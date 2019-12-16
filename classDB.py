import pymysql
import warnings

class FootballDB:

    dbcon = None
    __luser = "sergey"
    __lserver = "192.168.1.129"
    __lpass = "1"
    __dbname = "football"

    def __init__(self):
        pass

    def openConnection(self):

        try:
            self.dbcon = pymysql.connect(host=self.__lserver,
                                         user=self.__luser,
                                         password=self.__lpass)
        except Exception as e:
            print("Ошибка подключения к БД", e)
            return False
        else:
            print("Подключение к БД установлено")

        return True

    def closeConnection(self):

        try:
            self.dbcon.close()
        except Exception as e:
            print("Ошибка закрытия БД", e)
            return False
        else:
            print("Соединение к БД закрыто")

        return True

    def createDataBase(self):

        cur = self.dbcon.cursor()

        warnings.filterwarnings("ignore")

        cur.execute(f"DROP DATABASE IF EXISTS {self.__dbname}")
        cur.execute(f"CREATE DATABASE {self.__dbname} COLLATE 'utf16_unicode_ci'")

        cur.execute(f"USE {self.__dbname}")

        cur.execute("DROP TABLE IF EXISTS `countries`")
        cur.execute("CREATE TABLE `countries`(`country_id` INT NOT NULL, " + \
                    "`country_name` VARCHAR(255) NOT NULL," + \
                    "PRIMARY KEY(`country_id`) )" + \
                    "COLLATE = 'utf16_unicode_ci'" + \
                    "ENGINE = InnoDB")

        cur.execute("DROP TABLE IF EXISTS `league`")
        cur.execute("CREATE TABLE `league`(`league_id` INT NOT NULL, " + \
                    "`country_id` INT NOT NULL, " + \
                    "`league_name` VARCHAR(255) NOT NULL," + \
                    "`league_season` VARCHAR(255) NOT NULL," + \
                    "`data_loaded` TINYINT NOT NULL," + \
                    "PRIMARY KEY(`league_id`) )" + \
                    "COLLATE = 'utf16_unicode_ci'" + \
                    "ENGINE = InnoDB")

        cur.execute("DROP TABLE IF EXISTS `events`")
        cur.execute("CREATE TABLE `events`(`match_id` INT NOT NULL, " + \
                    "`country_id` INT NOT NULL, " + \
                    "`league_id` INT NOT NULL," + \
                    "`match_date` DATETIME NOT NULL," + \
                    "`match_status` VARCHAR(255) NOT NULL," + \
                    "`match_time` VARCHAR(10) NOT NULL," + \
                    "`match_hometeam_name` VARCHAR(255) NOT NULL," + \
                    "`match_awayteam_name` VARCHAR(255) NOT NULL," + \
                    "`match_hometeam_score` INT NOT NULL," + \
                    "`match_awayteam_score` INT NOT NULL," + \
                    "PRIMARY KEY(`match_id`) )" + \
                    "COLLATE = 'utf16_unicode_ci'" + \
                    "ENGINE = InnoDB")

        cur.execute("DROP TABLE IF EXISTS `predictions`")
        cur.execute("CREATE TABLE `predictions`(`match_id` INT NOT NULL, " + \
                    "`prob_HW` FLOAT(5,2) NOT NULL, " + \
                    "`prob_D` FLOAT(5,2) NOT NULL, " + \
                    "`prob_AW` FLOAT(5,2) NOT NULL, " + \
                    "`prob_HW_D` FLOAT(5,2) NOT NULL, " + \
                    "`prob_AW_D` FLOAT(5,2) NOT NULL, " + \
                    "`prob_HW_AW` FLOAT(5,2) NOT NULL, " + \
                    "PRIMARY KEY(`match_id`) )" + \
                    "COLLATE = 'utf16_unicode_ci'" + \
                    "ENGINE = InnoDB")
        cur.close()

    def save_countries(self, countrydata):

        cur = self.dbcon.cursor()
        cur.execute(f"USE {self.__dbname}")

        for data in countrydata:

            c_id = int(data["country_id"])
            c_name = data["country_name"]

            cur.execute(f"DELETE FROM countries WHERE country_id = {c_id}")
            cur.execute(f"INSERT INTO countries VALUES({c_id}, '{c_name}')")

        self.dbcon.commit()
        cur.close()

    def save_league(self, competionsdata):

        cur = self.dbcon.cursor()
        cur.execute(f"USE {self.__dbname}")

        for data in competionsdata:

            l_id = int(data["league_id"])
            c_id = int(data["country_id"])
            l_name = data["league_name"]
            l_ses = data["league_season"]

            cur.execute(f"DELETE FROM league WHERE league_id = {c_id}")
            cur.execute(f"INSERT INTO league VALUES({l_id}, {c_id}, '{l_name}', '{l_ses}', 0)")

        self.dbcon.commit()
        cur.close()

    def _save_event(self, cur, country_id, data):

        m_id = int(data["match_id"])
        c_id = country_id
        l_id = data["league_id"]
        m_date = data["match_date"]
        m_status = data["match_status"]
        m_time = data["match_time"]
        mh_name = data["match_hometeam_name"]
        ma_name = data["match_awayteam_name"]
        try:
            mh_score = int(data["match_hometeam_score"])
        except:
            mh_score = 0
        try:
            ma_score = int(data["match_awayteam_score"])
        except:
            ma_score = 0

        cur.execute(f"DELETE FROM events WHERE match_id = {m_id}")

        select = f"INSERT INTO events VALUES(\
                    {m_id}, {c_id}, {l_id}, '{m_date}', '{m_status}',\
                    '{m_time}', '{mh_name}', '{ma_name}', {mh_score}, {ma_score}\
                    )"
        # print(select)

        cur.execute(select)


    def save_event(self, country_id, eventsdata):

        cur = self.dbcon.cursor()
        cur.execute(f"USE {self.__dbname}")

        for data in eventsdata:
            try:
                self._save_event(cur, country_id, data)
            except Exception as e:
                print(data)
                raise e

        self.dbcon.commit()
        cur.close()

    def save_predictions(self, predictiondata):

        cur = self.dbcon.cursor()
        cur.execute(f"USE {self.__dbname}")

        for data in predictiondata:

            m_id = int(data["match_id"])
            prob_HW = float(data["prob_HW"])
            prob_D = float(data["prob_D"])
            prob_AW = float(data["prob_AW"])
            prob_HW_D = float(data["prob_HW_D"])
            prob_AW_D = float(data["prob_AW_D"])
            prob_HW_AW = float(data["prob_HW_AW"])

            cur.execute(f"DELETE FROM predictions WHERE match_id = {m_id}")

            select = f"INSERT INTO predictions VALUES(\
                        {m_id}, {prob_HW}, {prob_D}, {prob_AW}, {prob_HW_D},\
                        {prob_AW_D}, {prob_HW_AW}\
                        )"
            # print(select)

            cur.execute(select)

        self.dbcon.commit()
        cur.close()

    def get_league_to_load(self):

        cur = self.dbcon.cursor()
        cur.execute(f"USE {self.__dbname}")

        cur.execute("select * from league where data_loaded = 0 limit 1")

        values = cur.fetchone()

        cur.close()

        return values

    def set_league_is_loaded(self, league_id):

        cur = self.dbcon.cursor()
        cur.execute(f"USE {self.__dbname}")

        cur.execute(f"update league set data_loaded = 1 where league_id = {league_id}")

        self.dbcon.commit()

        cur.close()
