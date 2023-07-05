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


class Reservoir:

    def __init__(self, k):
        self.k = k
        self.items = []  # items in reservoir
        self.t = self.k  # number of items seen so far

    def add(self, x):
        if (len(self.items) < self.k):
            self.items.append(x)
        else:
            r = random.randint(0, self.t - 1)
            if (r < self.k):
                self.items[r] = x
        self.t += 1

    def get(self):
        return self.items


def log_row(row):
    print(row)
    return row


class ReservoireSampling(beam.CombineFn):
    def create_accumulator(self):
        k = 3
        return Reservoir(k)

    def add_input(self, reservoir, input):
        reservoir.add(input)
        return reservoir

    def merge_accumulators(self, reservoirs):
        k1 = 1
        k2 = 1
        S = Reservoir(reservoirs[0].k) # result

        for i in range(0, reservoirs[0].k):
            n1 = reservoirs[0].t
            n2 = reservoirs[1].t
            j = random.randint(0, n1 + n2 - 1)
            if j < n1:
                S.get()[i] = reservoirs[0].get()[k1]
                k1 += 1
                n1 -= n1
            else:
                S.get()[i] = reservoirs[1].get()[k2]
                k2 += 1
                n2 -= n2

        S.t = reservoirs[0].t + reservoirs[1].t + reservoirs[0].k
        return S

    def extract_output(self, reservoir):
        return reservoir


class AddWindowInfo(beam.DoFn):
    def process(self, x, window=beam.DoFn.WindowParam, timestamp=beam.DoFn.TimestampParam):
        x["window_start"] = window.start.to_utc_datetime()
        x["window_end"] = window.end.to_utc_datetime()
        x["watermark"] = timestamp.to_utc_datetime()
        x["late"] = x["watermark"] + datetime.timedelta(microseconds=1) < x["window_end"]
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
            | 'Read from Pub/Sub' >> ReadFromPubSub(
        subscription="projects/loyal-framework-384312/subscriptions/sfpd-sub",
        timestamp_attribute="Text.timestamp",
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
