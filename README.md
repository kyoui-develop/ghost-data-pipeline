```mermaid
graph LR
    source["Ghost Admin API"] --> pipeline["Pipeline"]
    pipeline --> storage1["AWS S3"]
    pipeline --> storage2["BigQuery"]