import sys
from enum import Enum, auto
from datetime import datetime

import dotenv
import logging
import os
import re
import time

import neo4j
from neo4j import GraphDatabase, Query
from neo4j.exceptions import ServiceUnavailable, CypherSyntaxError
from neo4j.graph import Relationship


class App:
    channels = set()
    def __init__(self, uri, user, password):
        # Global thread-safe driver object. It's reused during the entire lifetime of the
        # project then destroyed when closing the application. If the driver object is destroyed
        # prematurely it would close open connections, etc.
        # Here we authenticate with username and password, called "basic authentication"
        # in the documentation.
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        # Don't forget to close the driver connection when you are finished with it

        # Would probably look cleaner to implement with a context manager.
        # but that's a low priority change imo.
        self.driver.close()

    @staticmethod
    def enable_log(level, output_stream):
        handler = logging.StreamHandler(output_stream)
        handler.setLevel(level)
        logging.getLogger("neo4j").addHandler(handler)
        logging.getLogger("neo4j").setLevel(level)

    @staticmethod
    def get_datetime(unix_time):
        return datetime.utcfromtimestamp(unix_time).strftime('%Y-%m-%d %H:%M:%S')

    @staticmethod
    def get_unixtime(date_time):
        dt_split = re.split("[-_:]", date_time)
        year = int(dt_split[0])
        month = int(dt_split[1])
        day = int(dt_split[2])
        hour = int(dt_split[3])
        minute = int(dt_split[4])

        dt = datetime(year, month, day, hour, minute)
        unixtime = time.mktime(dt.timetuple())
        return str(int(unixtime))

    def get_direct_channels(self, contacting, contacted):
        with self.driver.session() as session:
            print("------------------------------------------------------------------------------------------------------------------")
            print("Type         Contacting          Contacted           Start time              End time")
            print("------------------------------------------------------------------------------------------------------------------")
            result = session.read_transaction(
                self._get_direct_channels, contacting, contacted)

            for row in result:
                print("{type}       {start_node}            {end_node}             {start_time}     {end_time}".format(
                    type=row["type"], start_node=row["start_node"], end_node=row["end_node"],
                    start_time=self.get_datetime(row['start_time']),
                    end_time=self.get_datetime(row['end_time'])
                ))

    @staticmethod
    def _get_direct_channels(tx, p1, p2):
        query = (
            "MATCH (p1:POI { name: $p1})-[r]-(p2:POI {name: $p2}) "
            "RETURN r, startNode(r).name as start_node, endNode(r).name as end_node, TYPE(r) as type "
            "ORDER BY r.start_time"
        )
        result = tx.run(query, p1=p1, p2=p2)

        try:
            return [{"type": row["type"],
                     "start_node": row["start_node"],
                     "end_node": row["end_node"],
                     "start_time": row["r"]["start_time"],
                     "end_time": row["r"]["end_time"]}
                    for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception".format(
                query=query, exception=exception))
            raise

    def find_all_outgoing_contacts(self, poi):
        with self.driver.session() as session:
            print("------------------------------------------------------------------------------------------------------------------")
            print("Type         Contacting          Contacted           Start time              End time")
            print("------------------------------------------------------------------------------------------------------------------")
            result = session.read_transaction(
                self._find_all_outgoing_contacts, poi)

            for row in result:
                print("{type}       {start_node}            {end_node}             {start_time}     {end_time}".format(
                    type=row["type"], start_node=row["start_node"], end_node=row["end_node"],
                    start_time=self.get_datetime(row['start_time']),
                    end_time=self.get_datetime(row['end_time'])
                ))

    @staticmethod
    def _find_all_outgoing_contacts(tx, poi):
        query = (
            "MATCH (poi:POI { name: $poi})-[r]->(n)"
            "RETURN r, startNode(r).name as start_node, endNode(r).name as end_node, TYPE(r) as type "
            "ORDER BY r.start_time"
        )
        result = tx.run(query, poi=poi)
        try:
            return [{"type": row["type"],
                     "start_node": row["start_node"],
                     "end_node": row["end_node"],
                     "start_time": row["r"]["start_time"],
                     "end_time": row["r"]["end_time"]}
                    for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception".format(
                query=query, exception=exception))
            raise

    def add_ci(self, relationship, contacting, contacted, start, end):
        start = App.get_unixtime(start)
        end = App.get_unixtime(end)
        rel = relationship.upper()
        with self.driver.session() as session:
            result = session.write_transaction(self._add_ci, rel, contacting, contacted, start, end)
            for row in result:
                print("added ci: ({contacting})-[:{rel} start_time: {rstart}, end_time: {rend}]->({contacted})".format(
                    contacting=row['contacting'],
                    rel=rel,
                    contacted=row['contacted'],
                    rstart=row['ci.start_time'],
                    rend=row['ci.end_time']
                ))

    @staticmethod
    def _add_ci(tx, relationship, contacting, contacted, start, end):
        query = (
            "MATCH (contacting:POI {name: '" + contacting + "' }) "
            "MATCH (contacted:POI {name: '" + contacted + "'}) "
            "MERGE (contacting)-[ci:" + relationship + " {start_time: " + start + ", end_time: " + end + "}]->(contacted) "
            "RETURN contacting, contacted, ci"
        )
        result = tx.run(query, contacting=contacting, contacted=contacted, relationship=relationship, start=start,
                        end=end)

        return [{"contacting": row["contacting"]["name"], "contacted": row["contacted"]["name"],
                 "ci.start_time": row["ci"]["start_time"], "ci.end_time": row["ci"]["end_time"]}
                for row in result]

    def get_channels_date(self, from_date_time, to_date_time):
        from_date_time = App.get_unixtime(from_date_time)
        to_date_time = App.get_unixtime(to_date_time)
        with self.driver.session() as session:
            result = session.read_transaction(self._get_channels_date, from_date_time, to_date_time)
        print("------------------------------------------------------------------------------------------------------------------")
        print("Type         Contacting          Contacted           Start time              End time")
        print("------------------------------------------------------------------------------------------------------------------")
        for row in result:
            print("{type}       {start_node}            {end_node}             {start_time}     {end_time}".format(
                type=row["type"], start_node=row["start_node"], end_node=row["end_node"],
                start_time=self.get_datetime(row['start_time']),
                end_time=self.get_datetime(row['end_time'])
            ))

    @staticmethod
    def _get_channels_date(tx, from_date_time, to_date_time):
        query = (
            "MATCH (n1)-[r]->(n2)"
            "WHERE " + from_date_time + " <= r.start_time <= " + to_date_time + " " 
            "RETURN r, startNode(r).name as start_node, endNode(r).name as end_node, TYPE(r) as type "
            "ORDER BY r.start_time"
        )
        result = tx.run(query, from_date_time=from_date_time, to_date_time=to_date_time)
        try:
            return [{"type": row["type"],
                     "start_node": row["start_node"],
                     "end_node": row["end_node"],
                     "start_time": row["r"]["start_time"],
                     "end_time": row["r"]["end_time"]}
                    for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception".format(
                query=query, exception=exception))
            raise

    def add_poi(self, name):
        with self.driver.session() as session:
            result = session.write_transaction(self._add_poi, name)
            for row in result:
                print("Created POI: {row}".format(row=row))

    @staticmethod
    def _add_poi(tx, name):
        query = (
                "CREATE (p:POI {name: '" + name + "' })"
                "RETURN p.name AS name"
        )
        result = tx.run(query, name=name)
        try:
            return [{"name": row["name"]}
                for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception".format(
                query=query, exception=exception))
            raise

    def communicated_with(self, poi, degrees):
        with self.driver.session() as session:
            result = session.read_transaction(self._communicated_with, poi, degrees)
        marker = "(POI)---"
        print("Length between")
        index = 0
        contacted = set()
        for row in result:
            if row["contacted"] in contacted:
                continue
            else:
                contacted.add(row["contacted"])
            length = int(row["length"])
            print(str(row["length"]) + "          (" + str(poi) + ")---", end="")
            for x in range(length-1):
                print(marker, end="")
            print("(" + row["contacted"] + ")")

    @staticmethod
    def _communicated_with(tx, poi, degrees):
        query = (
            "MATCH p = (poi:POI {name: '" + poi + "'})-[*.." + degrees + "]-(n:POI) "
            "WHERE n.name <> '" + poi + "' "
            "WITH relationships(p) as contacts, length(p) as length, n.name as contacted "
            "RETURN DISTINCT contacts, length, contacted "
            "ORDER BY length"
        )
        result = tx.run(query, poi=poi, degrees=degrees)
        try:
            return [{"contacts": row["contacts"],
                     "length": row["length"],
                     "contacted": row["contacted"]}
                    for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception".format(
                query=query, exception=exception))
            raise

    def communication_between(self, x, y):

        with self.driver.session() as session:
            path = session.read_transaction(self._communication_between, x, y)
        print("the chain of relationships between " + x + " and " + y + ":")
        for x in path:
            for p in x["p"]:
                if isinstance(p, dict):
                    print("(" + p["name"] + ")", end="")
                else:
                    print("---[" + p + "]---", end="")

        print("\n")

    @staticmethod
    def _communication_between(tx, x, y):
        query = (
            "MATCH (x:POI {name: '" + x + "'}), (y:POI {name: '" + y + "'}), "
            "p = SHORTESTPATH((x)-[*]-(y)) "
            "RETURN p"
        )
        result = tx.run(query, x=x, y=y)
        record = result.data()

        try:
            return record
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception".format(
                query=query, exception=exception))
            raise

    def update_poi(self, poi, info_key, info_value):
        with self.driver.session() as session:
            result = session.write_transaction(self._update_poi, poi, info_key, info_value)
        print(f'updated record of {poi}:')
        for x in result:
            for key, value in x["poi"].items():
                print(key + ": " + value)

    @staticmethod
    def _update_poi(tx, poi, info_key, info_value):
        query = (
            "MATCH (poi:POI {name: '" + poi + "'}) "
            "SET poi." + info_key + " = '" + info_value + "' "
            "RETURN poi"
        )
        result = tx.run(query, poi=poi, info_key=info_key, info_value=info_value)
        record = result.data()

        try:
            return record
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception".format(
                query=query, exception=exception))
            raise

    @staticmethod
    def load_channels():
        app.channels.clear()
        f = open('channels', "r")
        for line in f:
            app.channels.add(line.replace("\n", ""))

    @staticmethod
    def add_channel(channel):
        ch = channel.upper()
        if app.channel_exists(channel):
            return print("Channel already exists")
        else:
            f = open("channels", "a")
            f.write(ch + "\n")
            f.close()
            app.load_channels()

    @staticmethod
    def channel_exists(channel):
        if app.channels.__contains__(channel.upper()):
            return True
        else:
            return False

    def run_advanced_mode_query(self, query):
        with self.driver.session() as session:
            res = session.run(Query(query))
            for r in res:
                print(r)


class CLIState(Enum):
    QUIT = auto()
    BASIC = auto()
    ADVANCED = auto()


class CommandLine:
    def __init__(self, app: App):
        self.app = app
        self.state = CLIState.BASIC

    def run(self):
        while self.state != CLIState.QUIT:
            try:
                if self.state == CLIState.BASIC:
                    user_input = input('<basic> ')
                    if user_input == '!q':
                        self.state = CLIState.QUIT
                    elif user_input == '!adv':
                        self.state = CLIState.ADVANCED
                    elif user_input == '!basic':
                        self.state = CLIState.BASIC
                    elif user_input.startswith('!add_poi '):
                        self.add_poi(user_input)
                    elif user_input.startswith('!update_poi '):
                        self.update_poi(user_input)
                    elif user_input.startswith('!add_channel '):
                        self.add_channel(user_input)
                    elif user_input.startswith('!outgoing '):
                        self.outgoing_channels(user_input)
                    elif user_input.startswith('!communicated_with '):
                        self.communicated_with(user_input)
                    elif user_input.startswith('!comm_between '):
                        self.communication_between(user_input)
                    elif user_input.startswith('!add_ci '):
                        self.add_ci(user_input)
                    elif user_input.startswith('!direct_channels '):
                        self.direct_channels(user_input)
                    elif user_input.startswith('!channels_date '):
                        self.channels_date(user_input)
                    else:
                        print(f'unknown command: {user_input}')
                elif self.state == CLIState.ADVANCED:
                    user_input = input('<advanced> ')
                    if user_input == '!q':
                        self.state = CLIState.QUIT
                    elif user_input == '!basic':
                        self.state = CLIState.BASIC
                    elif user_input == '!adv':
                        self.state = CLIState.ADVANCED
                    else:
                        try:
                            app.run_advanced_mode_query(user_input)
                        except CypherSyntaxError:
                            print(f"invalid query: {user_input}")
            except Exception as f:
                print(f)


    @staticmethod
    def update_poi(user_input):
        # 1.2.
        poi, info_key, info_value = user_input.split(' ')[1:]
        print(f'updating {poi} with key {info_key} and value {info_value}')
        app.update_poi(poi, info_key, info_value)

    @staticmethod
    def add_channel(user_input):
        # 1.3 The operator can define a new channel. New means of communication are continuously developed and adopted.
        # The database must be designed such that it can gracefully handle, for example, a new communication app.
        channel, = user_input.split(' ')[1:]
        print(f'adding {channel} to the database')
        app.add_channel(channel)

    @staticmethod
    def outgoing_channels(user_input):
        # 1.4: The operator can enter a query to find all outgoing channels from a POI.
        poi = user_input.split(' ')[1:]
        print(f'find all outgoing channels from {poi[0]}')
        app.find_all_outgoing_contacts(poi[0])

    @staticmethod
    def communicated_with(user_input):
        # 1.5 The operator can find all POIs that a particular POI has communicated with,
        # with a configurable degree of separation.
        poi, degrees = user_input.split(' ')[1:]
        print(f'who has {poi} communicated with? degrees of separation: {degrees}')
        app.communicated_with(poi, degrees)

    @staticmethod
    def communication_between(user_input):
        # 1.6 The operator can enter POI X and POI Y as a query to see if there has been any CIs between them.
        # The query returns the shortest path, including CI and intermediate POI’s.
        poi1, poi2 = user_input.split(' ')[1:]
        print(f'has {poi1} and {poi2} communicated?')
        app.communication_between(poi1, poi2)

    @staticmethod
    def add_ci(user_input):
        # 1.7 The operator can add a new CI between two existing POIs.
        poi1, channel, poi2, start, end = user_input.split(' ')[1:]
        if app.channel_exists(channel):
            print(f'adding {channel} between {poi1} and {poi2} with start time {start} and end time {end}')
            app.add_ci(channel, poi1, poi2, start, end)
        else:
            print("specified channel does not exist, please add channel first (!add_channel).")

    @staticmethod
    def direct_channels(user_input):
        # 1.8 The operator can get all CIs between two directly communicating POIs.
        poi1, poi2 = user_input.split(' ')[1:]
        print(f'finding the direct channels between {poi1} and {poi2}')
        app.get_direct_channels(poi1, poi2)

    @staticmethod
    def channels_date(user_input):
        # 1.9 The operator can get all CIs between two points of datetime.
        from_datetime, to_datetime = user_input.split(' ')[1:]
        print(f'finding channels between {from_datetime} and {to_datetime}')
        app.get_channels_date(from_datetime, to_datetime)

    @staticmethod
    def add_poi(user_input):
        name = user_input.split(' ')[1:]
        print(f'adding poi {name}')
        app.add_poi(name[0])


if __name__ == "__main__":
    # Third-party library that handles the .env file and loads it into
    # the systems environmental variables. You could for example, use
    # the .env file in development, but then load from actual env variables
    # in production. Neat, but the major risk is to commit .env to git.
    dotenv.load_dotenv()

    uri = os.getenv('AURA_URI')
    user = os.getenv('ODIN_USER')
    password = os.getenv('ODIN_PASSWORD')

    App.enable_log(logging.INFO, sys.stdout)
    app = App(uri, user, password)
    app.load_channels()
    # app.create_friendship("Alice", "David")
    # app.find_person("Alice")
    # app.find_outgoing_contacts('Petter Nordblom', 'Stefan Karlsson')
    # app.create_poi('Thorild Sten')
    # app.find_all_outgoing_contacts('Vesslan')
    cli = CommandLine(app)
    cli.run()

    app.close()
