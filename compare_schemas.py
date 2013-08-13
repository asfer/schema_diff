import sys
import argparse
import sqlalchemy
import pickle
from migrate.versioning.schemadiff import SchemaDiff


def parse_arguments():
    tokens = sys.argv
    arguments = tokens[1:]
    parser = argparse.ArgumentParser()
    parser.add_argument("first_filename", help="first metadata filename")
    parser.add_argument("second_filename", help="second metadata filename")
    arguments = parser.parse_args(arguments)
    return arguments.first_filename, arguments.second_filename


def load_metadata(filename):
    metadata = load_metadata_from_file_and_catch_exception(filename)
    check_metadata_validity(metadata, filename)
    return metadata


def load_metadata_from_file_and_catch_exception(filename):
    try:
        return load_metadata_from_file(filename)
    except Exception:
        print "Invalid filename: {:s}".format(filename)
        exit(1)


def load_metadata_from_file(filename):
    file = open(filename, "r")
    metadata = pickle.load(file)
    return metadata


def check_metadata_validity(metadata, filename):
    if not isinstance(metadata, sqlalchemy.schema.MetaData):
        print "Invalid file content: {:s}".format(filename)
        exit(1)


if __name__ == '__main__':
    first_filename, second_filename = parse_arguments()
    first_metadata = load_metadata(first_filename)
    second_metadata = load_metadata(second_filename)
    schema_diff = SchemaDiff(first_metadata, second_metadata, labelA='First', labelB='Second', excludeTables=None)
    print schema_diff
