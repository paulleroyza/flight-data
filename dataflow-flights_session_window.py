from __future__ import absolute_import

import logging
import argparse
import apache_beam as beam
from apache_beam import window

schema={
                            'fields': [
                                {'name': 'SessionID', 'type': 'STRING', 'mode': 'NULLABLE', 'description': 'Split the flight data based on when the plane was last seen'}, 
                                {'name': 'MT', 'type': 'STRING', 'mode': 'NULLABLE', 'description': 'SEL ID AIR STA CLK MSG info http://woodair.net/sbs/Article/Barebones42_Socket_Data.htm'}, 
                                {'name': 'TT', 'type': 'INT64', 'mode': 'NULLABLE', 'description': '1 - 8'}, 
                                {'name': 'SID', 'type': 'STRING', 'mode': 'NULLABLE', 'description': 'Database Session record number'}, 
                                {'name': 'AID', 'type': 'STRING', 'mode': 'NULLABLE', 'description': 'Database Aircraft record number'}, 
                                {'name': 'Hex', 'type': 'STRING', 'mode': 'NULLABLE', 'description': 'Aircraft Mode S hexadecimal code https://opensky-network.org/datasets/metadata/'}, 
                                {'name': 'FID', 'type': 'STRING', 'mode': 'NULLABLE', 'description': 'Database Flight record number'}, 
                                {'name': 'DMG', 'type': 'DATE', 'mode': 'NULLABLE', 'description': 'Date message generated'}, 
                                {'name': 'TMG', 'type': 'TIME', 'mode': 'NULLABLE', 'description': 'Time message generated'}, 
                                {'name': 'DML', 'type': 'DATE', 'mode': 'NULLABLE', 'description': 'Date message logged'}, 
                                {'name': 'TML', 'type': 'TIME', 'mode': 'NULLABLE', 'description': 'Time message logged'}, 
                                {'name': 'CS', 'type': 'STRING', 'mode': 'NULLABLE', 'description': 'Callsign An eight digit flight ID - can be flight number or registration (or even nothing).'}, 
                                {'name': 'Alt', 'type': 'INT64', 'mode': 'NULLABLE', 'description': 'Mode C altitude. Height relative to 1013.2mb (Flight Level). Not height AMSL..'}, 
                                {'name': 'GS', 'type': 'INT64', 'mode': 'NULLABLE', 'description': 'GroundSpeed Speed over ground (not indicated airspeed)'}, 
                                {'name': 'Trk', 'type': 'INT64', 'mode': 'NULLABLE', 'description': 'Track Track of aircraft (not heading). Derived from the velocity E/W and velocity N/S'}, 
                                {'name': 'Lat', 'type': 'FLOAT64', 'mode': 'NULLABLE', 'description': 'Latitude North and East positive. South and West negative.'}, 
                                {'name': 'Lng', 'type': 'FLOAT64', 'mode': 'NULLABLE', 'description': 'Longitude North and East positive. South and West negative.'}, 
                                {'name': 'VR', 'type': 'INT64', 'mode': 'NULLABLE', 'description': 'VerticalRate 64ft resolution'}, 
                                {'name': 'Sq', 'type': 'INT64', 'mode': 'NULLABLE', 'description': 'Assigned Mode A squawk code.'}, 
                                {'name': 'Alrt', 'type': 'INT64', 'mode': 'NULLABLE', 'description': 'Flag to indicate squawk has changed.'}, 
                                {'name': 'Emer', 'type': 'INT64', 'mode': 'NULLABLE', 'description': 'Flag to indicate emergency code has been set'}, 
                                {'name': 'SPI', 'type': 'INT64', 'mode': 'NULLABLE', 'description': 'Flag to indicate transponder Ident has been activated.'}, 
                                {'name': 'Gnd', 'type': 'INT64', 'mode': 'NULLABLE', 'description': 'Flag to indicate ground squat switch is active'}, 
                                ]
                            }

regex=r"(\w{1,3},\d{1},\d+,\d+,[0-9A-F]{6},\d+,[0-9]{4}\/[0-9]{2}\/[0-9]{2},[0-9]{2}:[0-9]{2}:[0-9]{2}\.\d{3},[0-9]{4}\/[0-9]{2}\/[0-9]{2},[0-9]{2}:[0-9]{2}:[0-9]{2}\.\d{3},[^,]*,\d*,\d*,\d*,-?\d*\.?\d*,-?\d*\.?\d*,\d*,\d*,\d*,\d*,\d*,\d*)"

def parse_pubsub(line):
  cols=['MT','TT','SID','AID','Hex','FID','DMG','TMG','DML','TML','CS','Alt','GS','Trk','Lat','Lng','VR','Sq','Alrt','Emer','SPI','Gnd']
  fields=line.split(',')
  data=dict(zip(cols,fields))
  data['SessionID']='TBC'
  data['DMG']=data['DMG'].replace('/','-')
  data['DML']=data['DML'].replace('/','-')
  for k, v in list(data.items()):
    if len(v)==0:
      del data[k]
  return (data['Hex'],data)

def convert_to_string(line):
  return line.decode("utf-8") 

def extract_time(data):
  import apache_beam as beam
  from datetime import datetime
  timestamp = datetime.strptime(data[1]['DMG']+' '+data[1]['TMG'], '%Y-%m-%d %H:%M:%S.%f').timestamp()
  return beam.window.TimestampedValue(data,timestamp)

def add_session(session_elems):
  session_id=session_elems[1][0]['Hex']+' '+session_elems[1][0]['DMG']+' '+session_elems[1][0]['TMG']
  for session in session_elems[1]:
    session['SessionID']=session_id
  return session_elems[1]

def fake_session(session_elems):
  session_elems[1]['SessionID']=""
  return session_elems[1]

def run(argv=None):
  """Build and run the pipeline."""

  parser = argparse.ArgumentParser()
  #parser.add_argument(
  #    '--input_topic', required=True,
  #    help='Input PubSub topic of the form "projects/<PROJECT>/topics/<TOPIC>".')
  parser.add_argument(
      '--input_subscription', required=True,
      help='Input PubSub subscription of the form "projects/<PROJECT>/subscriptions/<SUBS>".')
  parser.add_argument(
      '--output_table', required=True,
      help=
      ('Output BigQuery table for results specified as: PROJECT:DATASET.TABLE '
       'or DATASET.TABLE.'))
  known_args, pipeline_args = parser.parse_known_args(argv)

  with beam.Pipeline(argv=pipeline_args) as p:
    # Read the pubsub topic into a PCollection.
    lines = ( p | 'Read from {}'.format(known_args.input_subscription) >> beam.io.ReadFromPubSub(subscription=known_args.input_subscription)
                | 'Convert to String' >> beam.Map(convert_to_string)
                | 'Filter Just in Case' >> beam.Regex.matches(regex)
                | 'Convert from CSV -> elements, blanks -> Null' >> beam.Map(parse_pubsub)
                | 'Generated Timestamp' >> beam.Map(extract_time)
                #| 'Session Window by Flight' >> beam.WindowInto(window.Sessions(2 * 60))
                #| 'Keep flight until it disappears' >> beam.GroupByKey()
                #| 'Add session id' >> beam.FlatMap(add_session)
                | 'Add fake session id' >> beam.Map(fake_session)
            )

    #if ("--runner=Dataflow" in pipeline_args):
    output_to_bq = ( lines | 'Writing out to {}'.format(known_args.output_table) >> beam.io.WriteToBigQuery(known_args.output_table,schema=schema,create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))
    #else:
    #output_to_console =( lines | 'Output' >> beam.Map(print) )

#python3 dataflow-flights_session_window.py --input_subscription projects/$DEVSHELL_PROJECT_ID/subscriptions/test --streaming --output_table NONE --runner=DirectRunner
if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
