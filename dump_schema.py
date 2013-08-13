import sys
import argparse
import re
import datetime
import sqlalchemy
import pickle


def parse_arguments():
    tokens = sys.argv
    arguments = tokens[1:]
    example = "connection_string: <connector>://<username>:<password>@<host>[:<port>]/<dbname>"
    parser = argparse.ArgumentParser(epilog=example)
    parser.add_argument("connection_string", help="sqlalchemy connection string")
    arguments = parser.parse_args(arguments)
    return arguments.connection_string


def parse_connection_string(string):
    pattern = r'(.*)://(.*):(.*)@(.*)(:.*)?/(.*)'
    result = re.match(pattern, string, re.I | re.M)
    connector = result.group(1)
    username = result.group(2)
    password = result.group(3)
    host = result.group(4)
    port = result.group(5)
    dbname = result.group(6)
    return connector, username, password, host, port, dbname


def get_timestamp():
    now = datetime.datetime.now()
    return now.strftime("%Y%m%d%H%M")


def get_filename(connection_string):
    _, _, _, _, _, dbname = parse_connection_string(connection_string)
    timestamp = get_timestamp()
    return "{:s}-{:s}.pickle".format(dbname, timestamp)


def get_metadata(connection_string):
    engine = sqlalchemy.create_engine(connection_string)
    metadata = sqlalchemy.MetaData()
    metadata.reflect(bind=engine)
    return metadata


def get_metadata_and_catch_exceptions(connection_string):
    try:
        return get_metadata(connection_string)
    except Exception:
        print "Invalid connection string"
        exit(1)


def save_metadata(metadata, filename):
    file = open(filename, "w")
    pickle.dump(metadata, file)


if __name__ == "__main__":
    connection_string = parse_arguments()
    metadata = get_metadata_and_catch_exceptions(connection_string)
    filename = get_filename(connection_string)
    save_metadata(metadata, filename)
