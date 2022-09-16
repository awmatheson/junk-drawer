# Setup

This requires both Kafka or Redpanda as well as Postgres before getting started you will need both of these.

### Step 1:

Run Kafka

### Step 2: 

Install and start Postgres and then run the following commands

```SQL
create database website;
\c website;
ALTER USER bytewax WITH SUPERUSER;
create TABLE events (
user_id VARCHAR unique PRIMARY KEY,
data JSON;
);
```

### Step 3:

Load some data in the topic

```sh
python fake_events.py
```

### Step 4:

Run the dataflow

```sh
python kafka_to_pg.py
```
