import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# =====================================================
# BOOKINGS
# =====================================================

@dlt.table(name="stage_bookings")
def stage_bookings():
    return (
        spark.readStream.format("delta")
        .load("/Volumes/workspace/bronze/bronzevolume/bookings/data/")
        .drop("_rescued_data")
    )


@dlt.view(name="trans_bookings")
def trans_bookings():
    return (
        spark.readStream.table("stage_bookings")
        .withColumn("amount", col("amount").cast(DoubleType()))
        .withColumn("booking_date", to_date(col("booking_date")))
        .withColumn("modifiedDate", current_timestamp())
        .drop("_rescued_data")
    )


rules = {
    "rule1": "booking_id IS NOT NULL",
    "rule2": "passenger_id IS NOT NULL"
}


@dlt.table(name="silver_bookings")
@dlt.expect_all_or_drop(rules)
def silver_bookings():
    return spark.readStream.table("trans_bookings")


# =====================================================
# FLIGHTS
# =====================================================

@dlt.view(name="trans_flights")
def trans_flights():
    return (
        spark.readStream.format("delta")
        .load("/Volumes/workspace/bronze/bronzevolume/flights/data/")
        .drop("_rescued_data")
        .withColumn("modifiedDate", current_timestamp())
    )


dlt.create_streaming_table(name="silver_flights")

dlt.create_auto_cdc_flow(
    target="silver_flights",
    source="trans_flights",
    keys=["flight_id"],
    sequence_by=col("modifiedDate"),
    stored_as_scd_type=1
)


# =====================================================
# PASSENGERS
# =====================================================

@dlt.view(name="trans_passengers")
def trans_passengers():
    return (
        spark.readStream.format("delta")
        .load("/Volumes/workspace/bronze/bronzevolume/customers/data/")
        .drop("_rescued_data")
        .withColumn("modifiedDate", current_timestamp())
    )


dlt.create_streaming_table(name="silver_passengers")

dlt.create_auto_cdc_flow(
    target="silver_passengers",
    source="trans_passengers",
    keys=["passenger_id"],
    sequence_by=col("modifiedDate"),
    stored_as_scd_type=1
)


# =====================================================
# AIRPORTS
# =====================================================

@dlt.view(name="trans_airports")
def trans_airports():
    return (
        spark.readStream.format("delta")
        .load("/Volumes/workspace/bronze/bronzevolume/airports/data/")
        .drop("_rescued_data")
        .withColumn("modifiedDate", current_timestamp())
    )


dlt.create_streaming_table(name="silver_airports")

dlt.create_auto_cdc_flow(
    target="silver_airports",
    source="trans_airports",
    keys=["airport_id"],
    sequence_by=col("modifiedDate"),
    stored_as_scd_type=1
)

# =====================================================
# SILVER BUSINESS
# =====================================================

@dlt.table(name="silver_business")
def silver_business():

    bookings = dlt.readStream("silver_bookings")

    flights = (
        dlt.readStream("silver_flights")
        .drop("modifiedDate")
    )

    passengers = (
        dlt.readStream("silver_passengers")
        .drop("modifiedDate")
    )

    airports = (
        dlt.readStream("silver_airports")
        .drop("modifiedDate")
    )

    return (
        bookings
        .join(flights, ["flight_id"])
        .join(passengers, ["passenger_id"])
        .join(airports, ["airport_id"])
    )
