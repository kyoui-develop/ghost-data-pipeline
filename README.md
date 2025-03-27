```mermaid
graph LR
    source["Ghost Admin API"] --> pipeline["Pipeline"]
    pipeline --> s3["AWS S3"] & bq["BigQuery"]