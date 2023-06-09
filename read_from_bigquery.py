import argparse
import logging
import json
import random

import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.trigger import AccumulationMode, AfterProcessingTime, AfterWatermark
from apache_beam.transforms.trigger import Repeatedly, AfterCount
from apache_beam.transforms.window import FixedWindows

def log_row(row):
    print(row)
    return row

class AddWindowInfo(beam.DoFn):
    def process(self, x, window=beam.DoFn.WindowParam):
        x["window_start"] = window.start.to_utc_datetime()
        x["window_end"] = window.end.to_utc_datetime()
        yield x


def encode_data(data):
    key, value = data
    data_dict = {'pddistrict': key, 'count': value}
    return data_dict


def main(argv=None):

    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)

    input_query = """
    SELECT *
    FROM `bigquery-public-data.san_francisco.sfpd_incidents` 
    order by timestamp ASC
    """

    pipeline = beam.Pipeline(options=pipeline_options)

    read_data = (
                pipeline
                | 'ReadFromBigQuery' >> beam.io.ReadFromBigQuery(query=input_query,use_standard_sql=True)
    )

    pddistrict = (
        read_data
        #| "reservoire sampling" >> beam.ParDo(selectKItems(read_data, len(list(read_data)), 5))
        #| "Window into fixed windows" >> beam.WindowInto(FixedWindows(60 * 60 * 24),
        #                                                   trigger=AfterWatermark(),
        #                                                   allowed_lateness=7 * 24 * 60 * 60,
        #                                                   accumulation_mode=AccumulationMode.DISCARDING)
        | "Key Value Pairs" >> beam.Map(lambda x: (x["pddistrict"],1))
        | "Sum" >> beam.CombinePerKey(sum)
        | beam.Map(encode_data)
        #| beam.ParDo(AddWindowInfo())
        #| "logging info" >> beam.Map(log_row)
    )


    """dayofweek = (
        read_data
        # | "reservoire sampling" >> beam.ParDo(selectKItems(read_data, len(list(read_data)), 5))
        | "Key Value Pairs 2" >> beam.Map(lambda x: (x["dayofweek"], 1))
        | "Sum 2" >> beam.CombinePerKey(sum)
        | "logging info 2" >> beam.Map(log_row)
    )

    category = (
        read_data
        # | "reservoire sampling" >> beam.ParDo(selectKItems(read_data, len(list(read_data)), 5))
        | "Key Value Pairs 3" >> beam.Map(lambda x: (x["category"], 1))
        | "Sum 3" >> beam.CombinePerKey(sum)
        | "logging info 3" >> beam.Map(log_row)
    )"""
    


    result = pipeline.run()
    result.wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    main()
