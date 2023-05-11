import argparse
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


def log_row(row):
    logging.info(str(row))
    return row

def main(argv=None):

    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)

    input_query = """
    SELECT *
    FROM `bigquery-public-data.san_francisco.sfpd_incidents` 
    order by timestamp ASC
    """

    pipeline =  beam.Pipeline(options=pipeline_options)

    read_data = (
                pipeline
                | 'ReadFromBigQuery' >> beam.io.ReadFromBigQuery(query=input_query,use_standard_sql=True)
                | "logging ginfo" >> beam.Map(log_row)
    )


    result = pipeline.run()
    result.wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    main()
