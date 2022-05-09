import sys
from enum import Enum, auto

import dotenv
import logging
import os

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
                else:
                    # Handle the "normal mode" commands.
                    print(f'{user_input}')
            elif self.state == CLIState.ADVANCED:
                user_input = input('<advanced> ')
                if user_input == '!q':
                    self.state = CLIState.QUIT
                elif user_input == '!basic':
                    self.state = CLIState.BASIC
                else:
                    try:
                        app.run_advanced_mode_query(user_input)
                    except CypherSyntaxError:
                        print(f"invalid query: {user_input}")


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
    app.find_person("Alice")
    cli = CommandLine(app)
    cli.run()

    app.close()
