from __future__ import absolute_import

import logging
import argparse
import apache_beam as beam
from apache_beam import window
from datetime import datetime

schema={
  "fields": [
    { "description": "bq-datetime", "mode": "NULLABLE", "name": "window_start", "type": "TIMESTAMP" }, 
    { "mode": "NULLABLE", "name": "Hex", "type": "STRING", 'description': 'Aircraft Mode S hexadecimal code https://opensky-network.org/datasets/metadata/'}, 
    { "mode": "NULLABLE", "name": "MT", "type": "STRING", 'description': 'SEL ID AIR STA CLK MSG info http://woodair.net/sbs/Article/Barebones42_Socket_Data.htm' },
    { "mode": "NULLABLE", "name": "SID", "type": "INTEGER", 'description': 'Database Session record number' },
    { "mode": "NULLABLE", "name": "AID", "type": "INTEGER", 'description': 'Database Aircraft record number' },
    { "mode": "NULLABLE", "name": "FID", "type": "INTEGER", 'description': 'Database Flight record number' },
    {"mode": "REPEATED", "name": "transponder", "type": "RECORD" ,"fields": [
      { "mode": "NULLABLE", "name": "TT", "type": "INTEGER",'description': '1 - 8' },
      { "mode": "NULLABLE", "name": "MG", "type": "TIMESTAMP", 'description': 'Timestamp message generated' },
      { "mode": "NULLABLE", "name": "ML", "type": "TIMESTAMP", 'description': 'Timestamp message logged' },
      { "mode": "NULLABLE", "name": "CS", "type": "STRING", 'description': 'Callsign An eight digit flight ID - can be flight number or registration (or even nothing).' },
      { "mode": "NULLABLE", "name": "Alt", "type": "INTEGER", 'description': 'Mode C altitude. Height relative to 1013.2mb (Flight Level). Not height AMSL..' },
      { "mode": "NULLABLE", "name": "GS", "type": "INTEGER", 'description': 'GroundSpeed Speed over ground (not indicated airspeed)' },
      { "mode": "NULLABLE", "name": "Trk", "type": "INTEGER", 'description': 'Track Track of aircraft (not heading). Derived from the velocity E/W and velocity N/S' },
      { "mode": "NULLABLE", "name": "Lat", "type": "FLOAT", 'description': 'Latitude North and East positive. South and West negative.' },
      { "mode": "NULLABLE", "name": "Lng", "type": "FLOAT", 'description': 'Longitude North and East positive. South and West negative.' },
      { "mode": "NULLABLE", "name": "VR", "type": "INTEGER", 'description': 'VerticalRate 64ft resolution' },
      { "mode": "NULLABLE", "name": "Sq", "type": "INTEGER", 'description': 'Assigned Mode A squawk code.' },
      { "mode": "NULLABLE", "name": "Alrt", "type": "INTEGER", 'description': 'Flag to indicate squawk has changed.' },
      { "mode": "NULLABLE", "name": "Emer", "type": "INTEGER", 'description': 'Flag to indicate emergency code has been set' },
      { "mode": "NULLABLE", "name": "SPI", "type": "INTEGER", 'description': 'Flag to indicate transponder Ident has been activated.' },
      { "mode": "NULLABLE", "name": "Gnd", "type": "INTEGER", 'description': 'Flag to indicate ground squat switch is active' },
      ]
    },
  ]}

regex=r"(\w{1,3},\d{1},\d+,\d+,[0-9A-F]{6},\d+,[0-9]{4}\/[0-9]{2}\/[0-9]{2},[0-9]{2}:[0-9]{2}:[0-9]{2}\.\d{3},[0-9]{4}\/[0-9]{2}\/[0-9]{2},[0-9]{2}:[0-9]{2}:[0-9]{2}\.\d{3},[^,]*,\d*,\d*,\d*,-?\d*\.?\d*,-?\d*\.?\d*,\d*,\d*,\d*,\d*,\d*,\d*)"

def parse_pubsub(line):
  from datetime import datetime
  cols=['MT','TT','SID','AID','Hex','FID','DMG','TMG','DML','TML','CS','Alt','GS','Trk','Lat','Lng','VR','Sq','Alrt','Emer','SPI','Gnd']
  fields=line.split(',')
  data=dict(zip(cols,fields))
  data['SessionID']='TBC'
  data['DMG']=data['DMG'].replace('/','-')
  data['DML']=data['DML'].replace('/','-')
  for k, v in list(data.items()):
    if len(v)==0:
      del data[k]
  data['MG']=datetime.strptime(data['DMG']+' '+data['TMG'], '%Y-%m-%d %H:%M:%S.%f').timestamp()
  data['ML']=datetime.strptime(data['DML']+' '+data['TML'], '%Y-%m-%d %H:%M:%S.%f').timestamp()
  keys_to_remove = ['DMG','DML','TMG','TML']
  for key in keys_to_remove:
    del data[key]
  return (data['Hex'],data)

def convert_to_string(line):
  try:
    return line.decode("utf-8")   
  except:
    return line

def extract_time(data):
  import apache_beam as beam
  from datetime import datetime
  timestamp = data[1]['MG']
  return beam.window.TimestampedValue(data,timestamp)

def add_session(session_elems):
  session_id=session_elems[1][0]['Hex']+' '+session_elems[1][0]['MG']
  for session in session_elems[1]:
    session['SessionID']=session_id
  return session_elems[1]

def add_start_of_window(session_elems):
  window_start = window.start.to_utc_datetime()
  session_id=session_elems['Hex']+' '+window_start
  return session_elems

class BuildRecordFn(beam.DoFn):

  def process(self, session_elems,  window=beam.DoFn.WindowParam):
      window_start = window.start.to_utc_datetime()
      session_id=session_elems[0]+" "+window_start.strftime("%Y-%m-%d %H:%M:%S.%f")
      output_data=[]
      for element in session_elems[1]:
        data=element
        data_key = {}
        keys_to_key = ['MT', 'SID', 'AID', 'FID', 'Hex']
        for key in keys_to_key:
          data_key[key]=data[key]
          del data[key]
        del data['SessionID']
        output_data.append(data)
      data_key['window_start']=window_start.strftime("%Y-%m-%d %H:%M:%S.%f")
      data_key['transponder']=output_data
      return [data_key]

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
      '--output_table',
      help=
      ('Output BigQuery table for results specified as: PROJECT:DATASET.TABLE '
       'or DATASET.TABLE.'))
  known_args, pipeline_args = parser.parse_known_args(argv)
  logging.info("Started {}".format((argv)))
  with beam.Pipeline(argv=pipeline_args) as p:
    # Read the pubsub topic into a PCollection.
    input_data = (p | 'Read from {}'.format(known_args.input_subscription) >> beam.io.ReadFromPubSub(subscription=known_args.input_subscription))
    #input_data = (p | 'Read from Text' >> beam.io.textio.ReadFromText('testdata.csv'))

    lines = ( input_data  | 'Convert to String' >> beam.Map(convert_to_string)
                          | 'Filter Just in Case' >> beam.Regex.matches(regex)
                          | 'Convert from CSV -> elements, blanks -> Null' >> beam.Map(parse_pubsub)
                          | 'Generated Timestamp' >> beam.Map(extract_time)
                          | 'Session Window by Flight' >> beam.WindowInto(window.Sessions(1 * 60)) #2 * 60
                          | 'Group' >> beam.GroupByKey() 
                          | 'Add Session ID to group' >> (beam.ParDo(BuildRecordFn()))
            )

    output_to_bq = ( lines | 'Writing out to {}'.format(known_args.output_table) >> 
      beam.io.WriteToBigQuery(known_args.output_table,
        schema=schema,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))
    if ("--runner=DirectRunner" in pipeline_args):
      output_to_console =( lines | 'Output' >> beam.Map(print) )

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()

#python3 dataflow-flights_nested.py --input_subscription projects/$DEVSHELL_PROJECT_ID/subscriptions/test --streaming --output_table $DEVSHELL_PROJECT_ID:FlightData.transponder_v2 --runner=DirectRunner
