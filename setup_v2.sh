#!/bin/bash

set -eo pipefail

export PROJECT_ID=$DEVSHELL_PROJECT_ID
export BUCKET=$PROJECT_ID
export SUBSCRIPTION=flight-data
export REGION=EU
export COMPUTEREGION=europe-west1
export ZONE=europe-west1-d

gcloud services enable dataflow.googleapis.com

#Make sure beam[gcp] is loaded
python3 -m pip install -U apache-beam[gcp]

#Preparing schema file
mkdir -p temp/
cat > temp/flightdata-schema-v2.json <<EOF
[
    { "description": "bq-datetime", "mode": "NULLABLE", "name": "window_start", "type": "TIMESTAMP" }, 
    { "mode": "NULLABLE", "name": "Hex", "type": "STRING", "description": "Aircraft Mode S hexadecimal code https://opensky-network.org/datasets/metadata/"}, 
    { "mode": "NULLABLE", "name": "MT", "type": "STRING", "description": "SEL ID AIR STA CLK MSG info http://woodair.net/sbs/Article/Barebones42_Socket_Data.htm" },
    { "mode": "NULLABLE", "name": "SID", "type": "INTEGER", "description": "Database Session record number" },
    { "mode": "NULLABLE", "name": "AID", "type": "INTEGER", "description": "Database Aircraft record number" },
    { "mode": "NULLABLE", "name": "FID", "type": "INTEGER", "description": "Database Flight record number" },
    {"mode": "REPEATED", "name": "transponder", "type": "RECORD" ,"fields": [
      { "mode": "NULLABLE", "name": "TT", "type": "INTEGER","description": "1 - 8" },
      { "mode": "NULLABLE", "name": "MG", "type": "TIMESTAMP", "description": "Timestamp message generated" },
      { "mode": "NULLABLE", "name": "ML", "type": "TIMESTAMP", "description": "Timestamp message logged" },
      { "mode": "NULLABLE", "name": "CS", "type": "STRING", "description": "Callsign An eight digit flight ID - can be flight number or registration (or even nothing)." },
      { "mode": "NULLABLE", "name": "Alt", "type": "INTEGER", "description": "Mode C altitude. Height relative to 1013.2mb (Flight Level). Not height AMSL.." },
      { "mode": "NULLABLE", "name": "GS", "type": "INTEGER", "description": "GroundSpeed Speed over ground (not indicated airspeed)" },
      { "mode": "NULLABLE", "name": "Trk", "type": "INTEGER", "description": "Track Track of aircraft (not heading). Derived from the velocity E/W and velocity N/S" },
      { "mode": "NULLABLE", "name": "Lat", "type": "FLOAT", "description": "Latitude North and East positive. South and West negative." },
      { "mode": "NULLABLE", "name": "Lng", "type": "FLOAT", "description": "Longitude North and East positive. South and West negative." },
      { "mode": "NULLABLE", "name": "VR", "type": "INTEGER", "description": "VerticalRate 64ft resolution" },
      { "mode": "NULLABLE", "name": "Sq", "type": "INTEGER", "description": "Assigned Mode A squawk code." },
      { "mode": "NULLABLE", "name": "Alrt", "type": "INTEGER", "description": "Flag to indicate squawk has changed." },
      { "mode": "NULLABLE", "name": "Emer", "type": "INTEGER", "description": "Flag to indicate emergency code has been set" },
      { "mode": "NULLABLE", "name": "SPI", "type": "INTEGER", "description": "Flag to indicate transponder Ident has been activated." },
      { "mode": "NULLABLE", "name": "Gnd", "type": "INTEGER", "description": "Flag to indicate ground squat switch is active" }
      ]
    }
  ]
EOF

# prepare BigQuery
bq --location $REGION mk $PROJECT_ID:FlightData

bq mk --table \
--schema temp/flightdata-schema-v2.json \
--time_partitioning_field window_start \
--time_partitioning_type DAY \
--norequire_partition_filter \
--clustering_fields Hex \
--description "Transponder flight data" \
--label purpose:demo \
$PROJECT_ID:FlightData.transponder_v2

#prepare pubsub
gcloud --project $PROJECT_ID pubsub subscriptions create $SUBSCRIPTION \
  --topic projects/paul-leroy/topics/flight-transponder \
  --ack-deadline 300

#subs based
python3 dataflow-flights_nested.py \
  --input_subscription projects/$PROJECT_ID/subscriptions/$SUBSCRIPTION \
  --output_table $PROJECT_ID:FlightData.transponder_v2 \
  --streaming \
  --runner Dataflow \
  --job_name nested \
  --project $PROJECT_ID \
  --max_num_workers 4 \
  --num_workers 1 \
  --disk_size_gb 100 \
  --machine_type n1-standard-1 \
  --region $COMPUTEREGION \
  --temp_location gs://$BUCKET/flight-data/temp \
  --staging_location gs://$BUCKET/flight-data/staging

