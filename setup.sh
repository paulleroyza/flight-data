#!/bin/bash

set -eo pipefail

export PROJECT_ID=$DEVSHELL_PROJECT_ID
export BUCKET=$PROJECT_ID
export SUBSCRIPTION=flight-data
export REGION=EU
export COMPUTEREGION=europe-west1
export ZONE=europe-west1-d

#prepare bucket
gsutil --l $REGION mb $BUCKET

gsutil -m cp gs://paul-leroy/flight-data/FlightData-*.avro gs://$BUCKET/import/
gsutil cp gs://paul-leroy/flight-data/doc8643AircraftTypes.csv gs://$BUCKET/import/
gsutil cp gs://paul-leroy/flight-data/doc8643Manufacturers.csv gs://$BUCKET/import/
gsutil cp gs://paul-leroy/flight-data/aircraftDatabase.csv gs://$BUCKET/import/

#Make sure beam[gcp] is loaded
python3 -m pip install -U apache-beam[gcp]

#Preparing schema file
mkdir -p temp/
cat > temp/flightdata-schema.json <<EOF
[
        {"name": "SessionID", "type": "STRING", "mode": "NULLABLE", "description": "Split the flight data based on when the plane was last seen"}, 
        {"name": "MT", "type": "STRING", "mode": "NULLABLE", "description": "SEL ID AIR STA CLK MSG info http://woodair.net/sbs/Article/Barebones42_Socket_Data.htm"}, 
        {"name": "TT", "type": "INT64", "mode": "NULLABLE", "description": "1 - 8"}, 
        {"name": "SID", "type": "STRING", "mode": "NULLABLE", "description": "Database Session record number"}, 
        {"name": "AID", "type": "STRING", "mode": "NULLABLE", "description": "Database Aircraft record number"}, 
        {"name": "Hex", "type": "STRING", "mode": "NULLABLE", "description": "Aircraft Mode S hexadecimal code https://opensky-network.org/datasets/metadata/"}, 
        {"name": "FID", "type": "STRING", "mode": "NULLABLE", "description": "Database Flight record number"}, 
        {"name": "DMG", "type": "DATE", "mode": "NULLABLE", "description": "Date message generated"}, 
        {"name": "TMG", "type": "TIME", "mode": "NULLABLE", "description": "Time message generated"}, 
        {"name": "DML", "type": "DATE", "mode": "NULLABLE", "description": "Date message logged"}, 
        {"name": "TML", "type": "TIME", "mode": "NULLABLE", "description": "Time message logged"}, 
        {"name": "CS", "type": "STRING", "mode": "NULLABLE", "description": "Callsign An eight digit flight ID - can be flight number or registration (or even nothing)."}, 
        {"name": "Alt", "type": "INT64", "mode": "NULLABLE", "description": "Mode C altitude. Height relative to 1013.2mb (Flight Level). Not height AMSL.."}, 
        {"name": "GS", "type": "INT64", "mode": "NULLABLE", "description": "GroundSpeed Speed over ground (not indicated airspeed)"}, 
        {"name": "Trk", "type": "INT64", "mode": "NULLABLE", "description": "Track Track of aircraft (not heading). Derived from the velocity E/W and velocity N/S"}, 
        {"name": "Lat", "type": "FLOAT64", "mode": "NULLABLE", "description": "Latitude North and East positive. South and West negative."}, 
        {"name": "Lng", "type": "FLOAT64", "mode": "NULLABLE", "description": "Longitude North and East positive. South and West negative."}, 
        {"name": "VR", "type": "INT64", "mode": "NULLABLE", "description": "VerticalRate 64ft resolution"}, 
        {"name": "Sq", "type": "INT64", "mode": "NULLABLE", "description": "Assigned Mode A squawk code."}, 
        {"name": "Alrt", "type": "INT64", "mode": "NULLABLE", "description": "Flag to indicate squawk has changed."}, 
        {"name": "Emer", "type": "INT64", "mode": "NULLABLE", "description": "Flag to indicate emergency code has been set"}, 
        {"name": "SPI", "type": "INT64", "mode": "NULLABLE", "description": "Flag to indicate transponder Ident has been activated."}, 
        {"name": "Gnd", "type": "INT64", "mode": "NULLABLE", "description": "Flag to indicate ground squat switch is active"}
]
EOF

# prepare BigQuery
bq --location $REGION mk $PROJECT_ID:FlightData

bq mk --table \
--schema temp/flightdata-schema.json \
--time_partitioning_field DMG \
--time_partitioning_type DAY \
--norequire_partition_filter \
--clustering_fields Hex \
--description "Transponder flight data" \
--label purpose:demo \
$PROJECT_ID:FlightData.transponder

#Secondary tables
#AircraftDatabase
cat > temp/AircraftDatabase-schema.json <<EOF
[
  {"name":"icao24","type":"STRING","mode":"NULLABLE"},
  {"name":"registration","type":"STRING","mode":"NULLABLE"},
  {"name":"manufacturericao","type":"STRING","mode":"NULLABLE"},
  {"name":"manufacturername","type":"STRING","mode":"NULLABLE"},
  {"name":"model","type":"STRING","mode":"NULLABLE"},
  {"name":"typecode","type":"STRING","mode":"NULLABLE"},
  {"name":"serialnumber","type":"STRING","mode":"NULLABLE"},
  {"name":"linenumber","type":"STRING","mode":"NULLABLE"},
  {"name":"icaoaircrafttype","type":"STRING","mode":"NULLABLE"},
  {"name":"operator","type":"STRING","mode":"NULLABLE"},
  {"name":"operatorcallsign","type":"STRING","mode":"NULLABLE"},
  {"name":"operatoricao","type":"STRING","mode":"NULLABLE"},
  {"name":"operatoriata","type":"STRING","mode":"NULLABLE"},
  {"name":"owner","type":"STRING","mode":"NULLABLE"},
  {"name":"testreg","type":"STRING","mode":"NULLABLE"},
  {"name":"registered","type":"STRING","mode":"NULLABLE"},
  {"name":"reguntil","type":"STRING","mode":"NULLABLE"},
  {"name":"status","type":"STRING","mode":"NULLABLE"},
  {"name":"built","type":"STRING","mode":"NULLABLE"},
  {"name":"firstflightdate","type":"STRING","mode":"NULLABLE"},
  {"name":"seatconfiguration","type":"STRING","mode":"NULLABLE"},
  {"name":"engines","type":"STRING","mode":"NULLABLE"},
  {"name":"modes","type":"STRING","mode":"NULLABLE"},
  {"name":"adsb","type":"STRING","mode":"NULLABLE"},
  {"name":"acars","type":"STRING","mode":"NULLABLE"},
  {"name":"notes","type":"STRING","mode":"NULLABLE"},
  {"name":"categoryDescription","type":"STRING","mode":"NULLABLE"}
]
EOF

bq mk --table \
--schema temp/AircraftDatabase-schema.json \
--description "Aircraft Database" \
--label purpose:demo \
$PROJECT_ID:FlightData.AircraftDatabase

#bq show --format=json project:FlightData.AircraftTypes
cat > temp/AircraftTypes-schema.json <<EOF
[
  {"name":"AircraftDescription","type":"STRING","mode":"NULLABLE"},
  {"name":"Description","type":"STRING","mode":"NULLABLE"},
  {"name":"Designator","type":"STRING","mode":"NULLABLE"},
  {"name":"EngineCount","type":"STRING","mode":"NULLABLE"},
  {"name":"EngineType","type":"STRING","mode":"NULLABLE"},
  {"name":"ManufacturerCode","type":"STRING","mode":"NULLABLE"},
  {"name":"ModelFullname","type":"STRING","mode":"NULLABLE"},
  {"name":"WTC","type":"STRING","mode":"NULLABLE"}
]
EOF

bq mk --table \
--schema temp/AircraftTypes-schema.json \
--description "Aircraft Types" \
--label purpose:demo \
$PROJECT_ID:FlightData.Types

#check these
bq load --source_format CSV --skip_leading_rows 1 $PROJECT_ID:FlightData.Types gs://$BUCKET/import/doc8643AircraftTypes.csv temp/AircraftTypes-schema.json
bq load --source_format CSV --skip_leading_rows 1 --allow_jagged_rows --allow_quoted_newlines $PROJECT_ID:FlightData.AircraftDatabase gs://$BUCKET/import/aircraftDatabase.csv temp/AircraftDatabase-schema.json
bq load --source_format AVRO $PROJECT_ID:FlightData.historic gs://$BUCKET/import/FlightData-*.avro temp/AircraftDatabase-schema.json

# ETL historic AVRO
bq query --append_table --destination_table $PROJECT_ID:FlightData.transponder \
"#standardsql
SELECT 
cast(SessionID as STRING) as SessionID,
        cast(MT as STRING) as MT,
        cast(TT as INT64) as TT,
        cast(SID as STRING) as SID,
        cast(AID as STRING) as AID,
        cast(Hex as STRING) as Hex,
        cast(FID as STRING) as FID,
        cast(DMG as DATE) as DMG,
        cast(TMG as TIME) as TMG,
        cast(DML as DATE) as DML,
        cast(TML as TIME) as TML,
        cast(CS as STRING) as CS,
        cast(Alt as INT64) as Alt,
        cast(GS as INT64) as GS,
        cast(Trk as INT64) as Trk,
        cast(Lat as FLOAT64) as Lat,
        cast(Lng as FLOAT64) as Lng,
        cast(VR as INT64) as VR,
        cast(Sq as INT64) as Sq,
        cast(Alrt as INT64) as Alrt,
        cast(Emer as INT64) as Emer,
        cast(SPI as INT64) as SPI,
        cast(Gnd as INT64) as Gnd
FROM FlightData.historic"

#prepare pubsub
gcloud --project $PROJECT_ID pubsub subscriptions create $SUBSCRIPTION \
  --topic projects/paul-leroy/topics/flight-transponder \
  --ack-deadline 300

#subs based
python3 dataflow-flights_session_window.py \
  --input_subscription projects/$PROJECT_ID/subscriptions/$SUBSCRIPTION \
  --output_table $PROJECT_ID:FlightData.transponder \
  --streaming \
  --runner Dataflow \
  --job_name prod \
  --project $PROJECT_ID \
  --max_num_workers 4 \
  --num_workers 1 \
  --disk_size_gb 100 \
  --machine_type n1-standard-1 \
  --region $COMPUTEREGION \
  --temp_location gs://$BUCKET/flight-data/temp \
  --staging_location gs://$BUCKET/flight-data/staging


  