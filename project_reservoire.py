import argparse
import logging
import json
import datetime
import random

from datetime import timedelta

import apache_beam as beam
from apache_beam.io import ReadFromPubSub
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io.gcp.pubsub import WriteToPubSub
from apache_beam.transforms.trigger import AccumulationMode, AfterProcessingTime, AfterWatermark
from apache_beam.transforms.trigger import Repeatedly, AfterCount
from apache_beam.transforms.window import FixedWindows


from apache_beam.io.gcp.internal.clients import bigquery

def log_row(row):
    print(row)
    return row

class ReservoireSampling(beam.CombineFn):
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
        k=3
        message_list = accumulator

        # Initialisiere das Reservoir mit den ersten k Nachrichten
        reservoir = message_list[:k]

        # Iteriere über die verbleibenden Nachrichten im Beam
        for i in range(k, len(message_list)):
            # Wähle ein zufälliges Element aus dem Reservoir
            random_index = random.randint(0, i)

            # Wenn das zufällige Index kleiner als die Beam-Breite ist, ersetze das Element im Reservoir
            if random_index < k:
                reservoir[random_index] = message_list[i]

        return reservoir

class AddWindowInfo(beam.DoFn):
    def process(self, x, window=beam.DoFn.WindowParam,  timestamp=beam.DoFn.TimestampParam):
        x["window_start"] = window.start.to_utc_datetime()
        x["window_end"] = window.end.to_utc_datetime()
        x["watermark"] = timestamp.to_utc_datetime()
        x["late"] = x["watermark"]+ datetime.timedelta(microseconds=1) < x["window_end"]
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

    incidents = (
            p
            | 'Read from Pub/Sub' >> ReadFromPubSub(subscription="projects/maxis-projekt-384312/subscriptions/sfpd_incidents-sub",
                                                    timestamp_attribute="timestamp",
                                                    with_attributes=False
                                                    )
    )

    incidents_category = (
            incidents
            | "Parse JSON payload2" >> beam.Map(json.loads)
            | "Window into fixed windows2" >> beam.WindowInto(FixedWindows(60),
                                                             trigger=AfterWatermark(
                                                                 late=Repeatedly(AfterCount(5))),
                                                             allowed_lateness=60 * 60 * 24,
                                                             accumulation_mode=AccumulationMode.DISCARDING)
            | "Key Value Pairs2" >> beam.Map(lambda x: (x["pddistrict"], x))
            | "Sum2" >> beam.CombinePerKey(ReservoireSampling())
            | "logging info2" >> beam.Map(log_row)

    )

    result = p.run()
    result.wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    main()
