import sys
from enum import Enum, auto
from datetime import datetime

import datetime
import dotenv
import logging
import os
import re
import time

from neo4j import GraphDatabase, Query
from neo4j.exceptions import ServiceUnavailable, CypherSyntaxError


class App:
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

    def create_friendship(self, person1_name, person2_name):
        with self.driver.session() as session:
            # Write transactions allow the driver to handle retries and transient errors
            result = session.write_transaction(
                self._create_and_return_friendship, person1_name, person2_name)
            for row in result:
                print("Created friendship between: {p1}, {p2}".format(p1=row['p1'], p2=row['p2']))

    @staticmethod
    def _create_and_return_friendship(tx, person1_name, person2_name):
        # To learn more about the Cypher syntax, see https://neo4j.com/docs/cypher-manual/current/
        # The Reference Card is also a good resource for keywords https://neo4j.com/docs/cypher-refcard/current/

        # To execute a Cypher Query, the query text is required along with an optional set of named parameters.
        # The text can contain parameter placeholders that are substituted with the corresponding values at
        # runtime. While it is possible to run non-parameterized Cypher Queries, good programming practice
        # is to use parameters in Cypher Queries whenever possible. This allows for the caching of queries
        # within the Cypher Engine, which is beneficial for performance.
        # Parameter values should adhere to Cypher values.
        query = (
            "CREATE (p1:Person { name: $person1_name }) "
            "CREATE (p2:Person { name: $person2_name }) "
            "CREATE (p1)-[:KNOWS]->(p2) "
            "RETURN p1, p2"
        )
        result = tx.run(query, person1_name=person1_name, person2_name=person2_name)
        try:
            return [{"p1": row["p1"]["name"], "p2": row["p2"]["name"]}
                    for row in result]
        # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    @staticmethod
    def get_datetime(unix_time):
        return datetime.utcfromtimestamp(unix_time).strftime('%Y-%m-%d %H:%M:%S')

    @staticmethod
    def get_unixtime(date_time):
        print(date_time)
        dt_split = re.split("[-_:]", date_time)
        print(dt_split)
        year = int(dt_split[0])
        month = int(dt_split[1])
        day = int(dt_split[2])
        hour = int(dt_split[3])
        minute = int(dt_split[4])

        dt = datetime.datetime(year, month, day, hour, minute)
        unixtime = time.mktime(dt.timetuple())
        return str(int(unixtime))

    def find_outgoing_contacts(self, contacting, contacted):
        with self.driver.session() as session:
            # Write transactions allow the driver to handle retries and transient errors
            print("Contacting              Contacted                  Start time                End time")
            print("------------------------------------------------------------------------------------------------")
            result = session.read_transaction(
                self.find_and_return_outgoing_contacts, contacting, contacted)
            for row in result:
                print("{p1}         {p2}            {rstart}       {rend}".format(
                    p1=row['p1'], p2=row['p2'],
                    rstart=self.get_datetime(row['r.start']),
                    rend=self.get_datetime(row['r.end'])
                ))

    @staticmethod
    def find_and_return_outgoing_contacts(tx, contacting, contacted):
        query = (
            "MATCH (p1:POI { name: $contacting})-[r]->(p2:POI {name: $contacted})"
            "RETURN p1, p2, r"
        )
        result = tx.run(query, contacting=contacting, contacted=contacted)
        try:
            return [{"p1": row["p1"]["name"], "p2": row["p2"]["name"], "r.start": row["r"]["start"],
                     "r.end": row["r"]["end"]}
                    for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception".format(
                query=query, exception=exception))
            raise

    def find_person(self, person_name):
        with self.driver.session() as session:
            # Routing Cypher by identifying reads and writes can improve the utilization of available
            # cluster resources. Since read servers are typically more plentiful than write servers,
            # it is beneficial to direct read traffic to read servers instead of the write server.
            # Doing so helps in keeping write servers available for write transactions.
            # The driver does not parse Cypher and therefore cannot automatically determine whether
            # a transaction is intended to carry out read or write operations.
            # As a result, a write transaction tagged as a read will still be sent to a read server,
            # but will fail on execution.
            result = session.read_transaction(self._find_and_return_person, person_name)
            for row in result:
                # Drivers translate between application language types and the Cypher Types.
                # To pass parameters and process results, it is important to know the basics of how Cypher
                # works with types and to understand how the Cypher Types are mapped in the driver.
                # All types can be potentially found in the result, although not all types can be used as parameters.
                # https://neo4j.com/docs/python-manual/current/cypher-workflow/#python-driver-type-mapping
                print("Found person: {row}".format(row=row))

    @staticmethod
    def _find_and_return_person(tx, person_name):
        query = (
            "MATCH (p:Person) "
            "WHERE p.name = $person_name "
            "RETURN p.name AS name"
        )
        result = tx.run(query, person_name=person_name)
        return [row["name"] for row in result]

    def add_ci(self, relationship, contacting, contacted, start, end):
        start = App.get_unixtime(start)
        end = App.get_unixtime(end)
        rel = relationship.upper()
        with self.driver.session() as session:
            result = session.write_transaction(self._add_ci, rel, contacting, contacted, start, end)
            for row in result:
                print("Added relationship: ({contacting})-[:{rel} start: {rstart}, end: {rend}]->({contacted})".format(
                    contacting=row['contacting'],
                    rel=rel,
                    contacted=row['contacted'],
                    rstart=row['ci.start'],
                    rend=row['ci.end']
                ))

    @staticmethod
    def _add_ci(tx, relationship, contacting, contacted, start, end):
        query = (
            "MATCH (contacting:POI {name: '" + contacting + "' }) "
            "MATCH (contacted:POI {name: '" + contacted + "'}) "
            "MERGE (contacting)-[ci:" + relationship + " {start: " + start + ", end: " + end + "}]->(contacted) "
            "RETURN contacting, contacted, ci"
        )
        result = tx.run(query, contacting=contacting, contacted=contacted, relationship=relationship, start=start,
                        end=end)

        return [{"contacting": row["contacting"]["name"], "contacted": row["contacted"]["name"],
                 "ci.start": row["ci"]["start"], "ci.end": row["ci"]["end"]}
                for row in result]

    def create_poi(self, name):
        with self.driver.session() as session:
            result = session.write_transaction(self._create_and_return_poi, name)
            for row in result:
                print("Created POI: {row}".format(row=row))

    @staticmethod
    def _create_and_return_poi(tx, name):
        query = (
                "CREATE (p:POI {name: '" + name + "' })"
                                                  "RETURN p.name AS name"
        )
        result = tx.run(query, name=name)
        return [row["name"] for row in result]

    def run_advanced_mode_query(self, query):
        with self.driver.session() as session:
            # An auto-commit transaction is a basic but limited form of transaction.
            # Such a transaction consists of only one Cypher query and is not automatically
            # retried on failure. Therefore, any error scenarios will need to be handled by
            # the client application itself.
            #
            # Auto-commit transactions serve the following purposes:
            #   simple use cases such as when learning Cypher or writing one-off scripts.
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
                elif user_input.startswith('!add_ci '):
                    self.add_ci(user_input)
                elif user_input.startswith('!outgoing '):
                    self.outgoing_channels(user_input)
                elif user_input.startswith('!communicated_with '):
                    self.communicated_with(user_input)
                elif user_input.startswith('!comm_between '):
                    self.communication_between(user_input)
                elif user_input.startswith('!add_channel '):
                    self.add_channel(user_input)
                elif user_input.startswith('!direct_channels '):
                    self.direct_channels(user_input)
                elif user_input.startswith('!channels_date '):
                    self.channels_date(user_input)
                elif user_input.startswith('!add_poi'):
                    self.add_poi(user_input)
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

    @staticmethod
    def add_poi(user_input):
        name, = user_input.split(' ')[1:]
        print(f'adding {name} to database.')

    @staticmethod
    def update_poi(user_input):
        # 1.2.
        # Don't know what data to extract from user input.
        pass

    @staticmethod
    def add_ci(user_input):
        # 1.3 The operator can enter a CI between operators
        channel, = user_input.split(' ')[1:]
        print(f'adding {channel} to the database')

    @staticmethod
    def outgoing_channels(user_input):
        # 1.4: The operator can enter a query to find all outgoing channels from a POI.
        arguments = user_input.split(' ')[1:]
        print(arguments)

    @staticmethod
    def communicated_with(user_input):
        # 1.5 The operator can find all POIs that a particular POI has communicated with,
        # with a configurable degree of separation.
        poi, degrees = user_input.split(' ')[1:]
        print(f'who has {poi} communicated with? degrees of separation: {degrees}')

    @staticmethod
    def communication_between(user_input):
        # 1.6 The operator can enter POI X and POI Y as a query to see if there has been any CIs between them.
        # The query returns the shortest path, including CI and intermediate POI’s.
        poi1, poi2 = user_input.split(' ')[1:]
        print(f'has {poi1} and {poi2} communicated?')

    @staticmethod
    def add_channel(user_input):
        # 1.7 The operator can add a new CI between two existing POIs.
        poi1, channel, poi2, start, end = user_input.split(' ')[1:]
        print(f'adding {channel} between {poi1} and {poi2} with start time {start} and end time {end}')
        app.add_ci(channel, poi1, poi2, start, end)

    @staticmethod
    def direct_channels(user_input):
        # 1.8 The operator can get all CIs between two directly communicating POIs.
        poi1, poi2 = user_input.split(' ')[1:]
        print(f'finding the direct channels between {poi1} and {poi2}')

    @staticmethod
    def channels_date(user_input):
        # 1.9 The operator can get all CIs between two points of datetime.
        date1, date2 = user_input.split(' ')[1:]
        print(f'finding channels between {date1} and {date2}')

    @staticmethod
    def add_poi(user_input):
        name = user_input.split(' ')[1:]
        print(f'adding poi {name}')
        app.create_poi(name[0])


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
    # app.create_friendship("Alice", "David")
    # app.find_person("Alice")
    # app.find_outgoing_contacts('Petter Nordblom', 'Stefan Karlsson')
    # app.create_poi('Thorild Sten')
    cli = CommandLine(app)
    cli.run()

    app.close()
