# Flight Data Challenge Lab

Set up some environment variables

```bash
export PROJECT_ID=$DEVSHELL_PROJECT_ID
export BUCKET=$PROJECT_ID
export SUBSCRIPTION=flight-data
export REGION=EU
export ZONE=europe-west1-d
```

We need a bucket for the pipeline, create a bucket with your project name, should be unique, if not adjust accordingly.

```bash
gsutil --l $REGION mb $BUCKET
```


Prepare to run Dataflow

```bash
python3 -m pip install -U apache-beam[gcp]
```

Create a file for the Bigquery schema. This step isn't really required unless you really want partitioned tables, so you can skip to the next optional step of getting the ancilliary data.

```bash
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
```

We create the Bigquery dataset and create the partitioned/clustered dataset . . . do you have any idea how much data planes create?

```bash
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
```

You can find the IACO source data and load some ancilliary tables

```bash
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
```

Now, you need a pubsub topic

```bash
gcloud --project $PROJECT_ID pubsub subscriptions create $SUBSCRIPTION \
  --topic projects/paul-leroy/topics/flight-transponder \
  --ack-deadline 300
```

And now run the pipeline against your subscription

```bash
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
  --worker_zone $ZONE \
  --temp_location gs://$BUCKET/flight-data/temp \
  --staging_location gs://$BUCKET/flight-data/staging
```

```sql
--query 1
WITH
  flights AS (
  SELECT
    DATETIME(DMG,
      TMG) AS DateTimeG,
    LOWER(Hex) AS icao24,
    SessionID,
    ST_GEOGPOINT(Lng,
      Lat) AS point,Alt
  FROM
    FlightData.transponder
  WHERE
    DMG = "2020-12-02"
    AND Lat IS NOT NULL
    AND Lng IS NOT NULL
    ),
  planes AS (
  SELECT
    icao24,
    manufacturername,
    model,
    operator
  FROM
    FlightData.AircraftDatabase)
SELECT
  icao24,
  SessionID,
  OPERATOR,
  min(Alt) as lowest,
  manufacturername,
  `model`,
  st_makeline(ARRAY_AGG(point
    ORDER BY
      DateTimeG)) AS flightpath,
  ARRAY_LENGTH(ARRAY_AGG(point
    ORDER BY
      DateTimeG)) AS path_length
FROM
  flights
JOIN
  planes
USING
  (icao24)
WHERE
  LENGTH(operator)>0
GROUP BY
  SessionID,
  icao24,
  operator,
  manufacturername,
  `model`
ORDER BY
  path_length DESC
LIMIT
  100



--query 2

WITH
  pointcloud AS (
  SELECT
    datetime(DMG,
      TMG) AS timestamp,
    SessionID,
    Alt,
    ST_GEOGPOINT(Lng,
      Lat) AS Pt
  FROM
    `project_id.FlightData.transponder`
  WHERE
    DMG = "2020-12-09"
    AND Lat IS NOT NULL
    AND Lng IS NOT NULL and Alt<6500),
  linesegment AS (
  SELECT
    Alt,
    Pt AS Pt1,
    LAG(Pt, 1) OVER (PARTITION BY SessionID ORDER BY timestamp) AS Pt2
  FROM
    pointcloud )
SELECT
  Alt,
  st_makeline(Pt1,
    Pt2) as line
FROM
  linesegment
WHERE
  Pt1 IS NOT NULL
  AND Pt2 IS NOT NULL


```

  
