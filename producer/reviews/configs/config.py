"""
filename = config.py
name = configuration

description:

this file is responsible to retain all the metadata for this project.
the variables are available for all .py files.
"""

# file location
ds_reviews = '/Users/luanmorenomaciel/BitBucket/databricks/producer/reviews/yelp_academic_dataset_review.csv'

# avro schema definition
key_schema_str = """
{
    "namespace": "my.yelp_streaming_app",
    "name": "key",
    "type": "record",
    "fields" : [
    {
        "name" : "review_id",
        "type" : "string"
    }
            ]
}
"""

value_schema_str = """
{
    "namespace": "my.yelp_streaming_app",
    "name": "value",
    "type": "record",
    "fields" : [
    {
        "name" : "review_id",
        "type" : "string"
    },
    {
        "name" : "business_id",
        "type" : "string"
    },
    {
        "name" : "user_id",
        "type" : "string"
    },
    {
        "name" : "stars",
        "type" : "int"
    },
    {
        "name" : "useful",
        "type" : "int"
    },
   {
        "name" : "date",
        "type" : "string"
    }
        ]
}
"""
