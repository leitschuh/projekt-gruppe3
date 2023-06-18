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


# A utility function
# to print an array
def printArray(stream, n):
    for i in range(n):
        print(stream[i], end=" ");
    print();


# A function to randomly select
# k items from stream[0..n-1].
def selectKItems(stream, n, k):
    i = 0;
    # index for elements
    # in stream[]

    # reservoir[] is the output
    # array. Initialize it with
    # first k elements from stream[]
    reservoir = [0] * k;
    for i in range(k):
        reservoir[i] = stream[i];

    # Iterate from the (k+1)th
    # element to nth element
    while (i < n):
        # Pick a random index
        # from 0 to i.
        j = random.randrange(i + 1);

        # If the randomly picked
        # index is smaller than k,
        # then replace the element
        # present at the index
        # with new element from stream
        if (j < k):
            reservoir[j] = stream[i];
        i += 1;

    print("Following are k randomly selected items");
    printArray(reservoir, k);
    print(type(reservoir))


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
                                                    timestamp_attribute="timestamp",
                                                    with_attributes=False
                                                    )
    )

    #n = len(rides);
    #k = 5;
    #selectKItems(rides, n, k);

    rides_windowed = (
                    rides
                    | "Parse JSON payload" >> beam.Map(json.loads)
                    | "logging info" >> beam.Map(log_row)
    )

    result = p.run()
    result.wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    main()
