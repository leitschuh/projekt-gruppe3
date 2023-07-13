import argparse
import logging
import json
import ast

import random
import apache_beam as beam
from apache_beam.io import ReadFromPubSub
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io.gcp.pubsub import WriteToPubSub
from apache_beam.transforms.trigger import AccumulationMode, AfterProcessingTime, AfterWatermark
from apache_beam.transforms.trigger import Repeatedly, AfterCount
from apache_beam.transforms.window import SlidingWindows
from geopy import distance as dst

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
    data_dict = {'pddistrict': key, 'value': value}
    return data_dict


class PartitionData(beam.DoFn):
    def process(self, x):
        if x["value"] != None:
            x["incident1"] = x["value"][0]
            x["incident2"] = x["value"][1]
            x["min_distance"] = x["value"][0]["min_distance"]
            x.pop("value")
        yield x


class SmallestDistance(beam.CombineFn):
    def create_accumulator(self):
        return []

    def add_input(self, accumulator, input):
        accumulator.append(input)
        return accumulator

    def merge_accumulators(self, accumulators):
        merged_accumulator = []
        for acc in accumulators:
            merged_accumulator.extend(acc)
        return merged_accumulator

    def extract_output(self, accumulator):
        smallest_distance = float('inf')
        smallest_events = None

        for i in range(len(accumulator)):
            for j in range(i + 1, len(accumulator)):
                event1 = accumulator[i]
                event2 = accumulator[j]
                event1_location = ast.literal_eval(event1['location'])
                event2_location = ast.literal_eval(event2['location'])
                distance = dst.distance(event1_location, event2_location).km

                if distance < smallest_distance and distance != 0:
                    smallest_distance = distance
                    event1['min_distance'] = smallest_distance
                    event2['min_distance'] = smallest_distance
                    smallest_events = (event1, event2)

        return smallest_events


def main(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""

    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    p = beam.Pipeline(options=pipeline_options)

    incidents = (
            p
            | 'Read from Pub/Sub' >> ReadFromPubSub(subscription="projects/loyal-framework-384312/subscriptions/sfpd-sub",
                                                    timestamp_attribute="timestamp",
                                                    with_attributes=False
                                                    )
    )

    incidents_distance = (
            incidents
            | "Parse JSON payload" >> beam.Map(json.loads)
            | "Window into fixed windows" >> beam.WindowInto(SlidingWindows(60 * 60, 60 * 30),
                                                             trigger=AfterWatermark(
                                                                 late=Repeatedly(AfterCount(10)),
                                                                 early=Repeatedly(AfterProcessingTime(60))
                                                             ),
                                                             allowed_lateness=60 * 60 * 24,
                                                             accumulation_mode=AccumulationMode.ACCUMULATING)

            | "Key Value Pairs" >> beam.Map(lambda x: (x["pddistrict"], x))
            | "Calculate Smallest Distance" >> beam.CombinePerKey(SmallestDistance())
            | beam.Map(encode_data)
            | beam.ParDo(PartitionData())
            | beam.ParDo(AddWindowInfo())
            | "Log Output" >> beam.Map(log_row)
    )

    result = p.run()
    result.wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    main()
