import argparse
import logging
import json

import random
import apache_beam as beam
from apache_beam.io import ReadFromPubSub
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io.gcp.pubsub import WriteToPubSub
from apache_beam.transforms.trigger import AccumulationMode, AfterProcessingTime, AfterWatermark
from apache_beam.transforms.trigger import Repeatedly, AfterCount
from apache_beam.transforms.window import FixedWindows

from geopandas import points_from_xy,read_file

from apache_beam.io.gcp.internal.clients import bigquery

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
    data_dict = {'borough': key, 'value': value}
    return data_dict



def main(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""

    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    p = beam.Pipeline(options=pipeline_options)

    rides = (
            p
            | 'Read from Pub/Sub' >> ReadFromPubSub(subscription="projects/maxis-projekt-384312/subscriptions/sfpd_incidents-sub",
                                                    timestamp_attribute="Text.timestamp",
                                                    with_attributes=False
                                                    )
    )

    rides_windowed = (
            rides
            | "Parse JSON payload" >> beam.Map(json.loads)
            #| "logging info2" >> beam.Map(log_row)
            | "Window into fixed windows" >> beam.WindowInto(FixedWindows(10),
                                                             trigger=AfterWatermark(
                                                           late=Repeatedly(AfterCount(1))),
                                                           allowed_lateness=60*60*24,
                                                         accumulation_mode=AccumulationMode.DISCARDING)

            | "Key Value Pairs" >> beam.Map(lambda x: (x["pddistrict"], 1))
            | "Sum" >> beam.CombinePerKey(sum)
            | beam.Map(encode_data)
            | beam.ParDo(AddWindowInfo())
            | "logging info" >> beam.Map(log_row)
    )

    result = p.run()
    result.wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    main()
