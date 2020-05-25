import psycopg2
import datetime
import ast
import pandas as pd

class HomeIoT(object):
    def __init__(self, conn):
        """
        Supply a connection string for connecting to the database. Must be a dictionary. Requires:
            host, database, user & password
        """
        self._validate_conn(conn)
        self.conn = conn

    def create_new_sensor(self, name, units = ''):
        """
        Creates a new sensor entry in the database. name must not be present in the database.
        name - the varchar name. Required
        units - the units in varchar. Not required.
        """
        con = psycopg2.connect(**self.conn)
        cur = con.cursor()

        q = "SELECT * FROM sensors WHERE name = '{0}'".format(name)
        cur.execute(q)
        names = cur.fetchall()

        if len(names) > 0:
            print('Note: Sensor {0} already exists in the database.')
        else:
            q = "INSERT INTO sensors (name, units) VALUES ('{0}', '{1}')".format(name, units)
            cur.execute(q)
            con.commit()

        con.close()

    def insert_data(self, sensorname, value, timestamp = None):
        """
        Inserts a data point. provide the varchar name of the sensor and the value.
        Timestamp is optional. If not Provided the current system time will be used.
        """
        con = psycopg2.connect(**self.conn)
        cur = con.cursor()

        q = "SELECT id FROM sensors WHERE name = '{0}'".format(sensorname)
        cur.execute(q)
        ids = cur.fetchall()

        if len(ids) == 0:
            raise Exception('Sensor {0} does not exist in the database. Add it first using method create_new_sensor'.format(sensorname))

        ids = ids[0][0]

        if timestamp == None:
            q = "INSERT INTO sensordata (sensorid, datatime, value) VALUES ({0}, now(), {1})".format(ids, value)
        else:
            q = "INSERT INTO sensordata (sensorid, datatime, value) VALUES ({0}, {1}, {2})".format(ids, datetime.datetime.now, value)

        cur.execute(q)
        con.commit()

        con.close()

    def get_data(self, sensorname):
        """
        Gets the data in the database from the selected sensor.
        """
        con = psycopg2.connect(**self.conn)
        cur = con.cursor()

        q = "SELECT id FROM sensors WHERE name = '{0}'".format(sensorname)
        cur.execute(q)
        ids = cur.fetchall()

        if len(ids) == 0:
            raise Exception('Sensor {0} does not exist in the database. Add it first using method create_new_sensor'.format(sensorname))

        ids = ids[0][0]

        q = "SELECT datatime, value FROM sensordata WHERE sensorid = {0} ORDER BY datatime".format(ids)

        cur.execute(q)
        data = cur.fetchall()

        data = pd.DataFrame(data, columns = ['datatime', 'value'])

        con.close()

        return data

    def _validate_conn(self, conn):
        req = ['host', 'database', 'user', 'password']

        if type(conn) != dict:
            raise Exception('conn string is not a dictionary. Must be a dictionary')

        for r in req:
            if r not in conn:
                raise Exception('{0} not in supplied database connection string'.format(r))


if __name__ == "__main__":
    #Inport the connectionstring and cast as a dictionary
    file = open("../connstr.txt", "r")
    contents = file.read()
    conn = ast.literal_eval(contents)
    file.close()

    #Instantiate the HomeIoT Object
    ht = HomeIoT(conn)

    #Create a new sensor in the database
    ht.create_new_sensor(name = 'temp_study')

    #insert some data
    ht.insert_data('temp_study', 44.)
    ht.insert_data('temp_study', 56.)

    #get the data back
    data = ht.get_data('temp_study')
    print(data)





















