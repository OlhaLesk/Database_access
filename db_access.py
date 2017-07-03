import configparser
import psycopg2
from collections import namedtuple


Person = namedtuple("Person", ["id", "name", "surname"])
DatabaseConfig = namedtuple("Config", ["dbname", "user", "password", "host",
                                       "port"])


class MyDatabase(object):
    _INSERT_QUERY_TEMPLATE = "INSERT INTO person (name, surname) VALUES (%s, %s)"
    _SELECT_QUERY_TEMPLATE = "SELECT id, name, surname FROM person"
    _UPDATE_QUERY_TEMPLATE = "UPDATE person SET name = %s WHERE surname = %s"
    _DELETE_QUERY_TEMPLATE = "DELETE FROM person WHERE name = %s AND surname = %s"

    def __init__(self, config):
        self._config = config

    def _GetConnection(self):
        return psycopg2.connect(dbname=self._config.dbname, user=self._config.user,
                                password=self._config.password, host=self._config.host, port=self._config.port)

    def InsertPerson(self, name, surname):
        try:
            with self._GetConnection() as conn:
                cur = conn.cursor()
                query = cur.mogrify(MyDatabase._INSERT_QUERY_TEMPLATE, (name, surname))
                cur.execute(query)
                print "A new person with name %s and surname %s is added." % (name, surname)
        except psycopg2.Error as e:
            print "Failed to insert values:", name, surname
            print e

    def GetAllValues(self):
        try:
            with self._GetConnection() as conn:
                cur = conn.cursor()
                cur.execute(MyDatabase._SELECT_QUERY_TEMPLATE)
                return map(Person._make, cur.fetchall())
        except psycopg2.Error as e:
            print "Failed to get values.", e

    def UpdateNameBySurname(self, name, surname):
        try:
            with self._GetConnection() as conn:
                cur = conn.cursor()
                query = cur.mogrify(MyDatabase._UPDATE_QUERY_TEMPLATE, (name, surname))
                cur.execute(query)
                print "%s's name is changed to %s." % (surname, name)
        except psycopg2.Error as e:
            print "Failed to update values:", name, surname
            print e

    def DeletePersonsByNameAndSurname(self, name, surname):
        try:
            with self._GetConnection() as conn:
                cur = conn.cursor()
                query = cur.mogrify(MyDatabase._DELETE_QUERY_TEMPLATE, (name, surname))
                cur.execute(query)
                print "%s %s's data were deleted from database." % (name, surname)
        except psycopg2.Error as e:
            print "Failed to delete values with name = %s and surname = %s" % (name, surname)
            print e


def printValues(values, message = "\n"):
    print message
    for value in values:
        print value
    print "\n"


def main():
    config = configparser.ConfigParser()
    config.read("config.ini")
    conf = DatabaseConfig(dbname=config["PSQL"]["DbName"], user=config["PSQL"]["User"],
                            password=config["PSQL"]["Password"], host=config["PSQL"]["Host"], port=config["PSQL"]["Port"])
    db = MyDatabase(conf)
    db.InsertPerson(name='John', surname='McRead')
    db.InsertPerson(name='Paul', surname='Qwery')
    printValues(db.GetAllValues(), "Database data")

    db.UpdateNameBySurname(name='Ben', surname='McRead')
    printValues(db.GetAllValues(), "Database data")

    db.DeletePersonsByNameAndSurname('Ben', "McRead")
    printValues(db.GetAllValues(), "Database data")


if __name__ == "__main__":
    main()