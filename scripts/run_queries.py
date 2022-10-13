"""
This script parses the queries sql file, transforms them into pyspark format, and returns a .txt file that
will then be used as input to run the queries.
"""
from pyspark.sql import SparkSession
from argparse import ArgumentParser


def create_spark_session():
    return SparkSession.builder.master("spark://spark-master:7077").appName("tpcds").getOrCreate()


def parse_args():
    """
    Parses arguments to run the queries. Expects the file path for the queries to run.
    :return:
    """
    parser = ArgumentParser()
    parser.add_argument("-q", "--queries-file-path")
    parser.add_argument("-o", "--output-file-path")
    args = parser.parse_args()
    return args


def parse_file(file_obj) -> list:
    """
    Parses the file object to split into queries. Returns a list with the query strings.
    :param file_obj:
    :return:
    """
    comment_count = 0
    queries = []
    query_lines = []
    for line in file_obj:
        if comment_count == 0 and "--" in line:  # it is a comment and therefore the beginning or end of a query
            comment_count += 1
            query_lines = []
            continue
        elif comment_count == 1 and "--" not in line:  # we are reading part of the query
            query_lines.append(line)
        elif comment_count == 1 and "--" in line:  # it is the second comment indicating this is the end of the query
            query = "".join(query_lines)
            queries.append(query)
            comment_count = 0
    return queries


def main():
    """
    Main function
    :return:
    """
    args = parse_args()
    print("running")
    with open(args.queries_file_path, "r") as queries_file_old:
        with open(args.output_file_path, "w") as queries_file_new:
            queries_new = parse_file(queries_file_old)
            print(len(queries_new))

    # Now do spark things
    spark = create_spark_session()
    spark.sql("use tpcds;")
    spark.sql("SELECT * FROM web_sales LIMIT 5;");


if __name__ == "__main__":
    main()
