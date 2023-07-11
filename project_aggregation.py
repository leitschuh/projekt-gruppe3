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

class AddWindowInfo(beam.DoFn):
    def process(self, x, window=beam.DoFn.WindowParam,  timestamp=beam.DoFn.TimestampParam):
        x["window_start"] = window.start.to_utc_datetime()
        x["window_end"] = window.end.to_utc_datetime()
        x["watermark"] = timestamp.to_utc_datetime()
        x["late"] = x["watermark"]+ datetime.timedelta(microseconds=1) < x["window_end"]
        yield x


def encode_data(data):
    key, value = data
    data_dict = {'pddistrict': key, 'value': value}
    return data_dict

def encode_data2(data):
    key, value = data
    data_dict = {'category': key, 'value': value}
    return data_dict



def main(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""

    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    p = beam.Pipeline(options=pipeline_options)

    table_spec1 = bigquery.TableReference(
        projectId='maxis-projekt-384312',
        datasetId='sfpd_incidents',
        tableId='aggregation_districts')
    
    table_spec2 = bigquery.TableReference(
        projectId='maxis-projekt-384312',
        datasetId='sfpd_incidents',
        tableId='aggregation_categorys')
    

    schema1 = {'fields': [{'name': 'pddistrict', 'type': 'STRING', 'mode': 'NULLABLE'},
                         {'name': 'value', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                         {'name': 'window_start', 'type': 'DATETIME', 'mode': 'NULLABLE'},
                         {'name': 'window_end', 'type': 'DATETIME', 'mode': 'NULLABLE'},
                         {'name': 'watermark', 'type': 'DATETIME', 'mode': 'NULLABLE'},
                         {'name': 'late', 'type': 'BOOLEAN', 'mode': 'NULLABLE'}]
              }
              
    schema2 = {'fields': [{'name': 'category', 'type': 'STRING', 'mode': 'NULLABLE'},
                         {'name': 'value', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                         {'name': 'window_start', 'type': 'DATETIME', 'mode': 'NULLABLE'},
                         {'name': 'window_end', 'type': 'DATETIME', 'mode': 'NULLABLE'},
                         {'name': 'watermark', 'type': 'DATETIME', 'mode': 'NULLABLE'},
                         {'name': 'late', 'type': 'BOOLEAN', 'mode': 'NULLABLE'}]
               }

    incidents = (
            p
            | 'Read from Pub/Sub' >> ReadFromPubSub(subscription="projects/maxis-projekt-384312/subscriptions/sfpd_incidents-sub",
                                                    timestamp_attribute="timestamp",
                                                    with_attributes=False
                                                    )
    )

    incidents_district = (
            incidents
            | "Parse JSON payload" >> beam.Map(json.loads)
            #| "logging info2" >> beam.Map(log_row)
            | "Window into fixed windows" >> beam.WindowInto(FixedWindows(10),
                                                             trigger=AfterWatermark(
                                                           late=Repeatedly(AfterCount(1))),
                                                           allowed_lateness=60*60*24,
                                                         accumulation_mode=AccumulationMode.DISCARDING)

            | "Key Value Pairs" >> beam.Map(lambda x: (x["pddistrict"], 1))
            | "Sum" >> beam.CombinePerKey(sum)
            | "Map" >> beam.Map(encode_data)
            | "Add Window Info" >> beam.ParDo(AddWindowInfo())
            | "logging info" >> beam.Map(log_row)
            | "Write to Big Query" >> beam.io.WriteToBigQuery(table_spec1,
                                      schema=schema1,
                                      write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                      create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                      batch_size=10)
    )
    """
    incidents_category = (
            incidents
            | "Parse JSON payload2" >> beam.Map(json.loads)
            # | "logging info2" >> beam.Map(log_row)
            | "Window into fixed windows2" >> beam.WindowInto(FixedWindows(10),
                                                             trigger=AfterWatermark(
                                                                 late=Repeatedly(AfterCount(5))),
                                                             allowed_lateness=60 * 60 * 24,
                                                             accumulation_mode=AccumulationMode.DISCARDING)

            | "Key Value Pairs2" >> beam.Map(lambda x: (x["category"], 1))
            | "Sum2" >> beam.CombinePerKey(sum)
            | "Map2" >> beam.Map(encode_data2)
            | "Add Window Info2" >> beam.ParDo(AddWindowInfo())
            | "logging info2" >> beam.Map(log_row)
            | "Write to Big Query2" >> beam.io.WriteToBigQuery(table_spec2,
                                      schema=schema2,
                                      write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                      create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
    )
"""
    result = p.run()
    result.wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    main()
