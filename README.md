# Data Pipeline Architecture - Kafka Stream

![Flow2](https://github.com/user-attachments/assets/e83d471e-f691-48db-a270-2d08e286a32a)

## Overview
This project demonstrates a data pipeline architecture using Kafka, Delta Lake, and Databricks, employing a multi-layered approach (Bronze, Silver, Gold) to streamline data ingestion, processing, and analysis.

#### Data Ingestion:

- **Source**: The pipeline starts with CSV files ingested using Spark Streaming.
- **Upstash and Kafka**: The ingested data is then streamed to Kafka via Upstash, facilitating real-time data streaming and ingestion.

#### Real-Time Processing:

- **Spark Streaming**: Continues to process data from Kafka, ensuring that data remains up-to-date as it moves through the pipeline.

The subsequent steps (Bronze, Silver, and Gold layers) follow a similar processing pattern, with data moving through these layers in Databricks and Delta Lake for refinement, transformation, and feature extraction.

### Streaming Data Ingestion and Processing with Kafka and Delta Lake

![Flow1](https://github.com/user-attachments/assets/c062e26b-6a41-4520-9917-860c73d39c6e)

The data pipeline architecture leverages Kafka, Delta Lake, and Databricks. 
### Bronze Layer:

- **Source**: Data is ingested into the Bronze layer using Kafka, storing raw, unprocessed data.
- **Data Flow**: Data from Kafka is streamed into a Bronze table in Delta Lake.

### Silver Layer:

#### Transformation: The raw data from the Bronze layer is cleaned and transformed into the Silver layer.
- **Stocks**: Contains transformed stock data.
- **Close Price - SCD 2**: Handles slowly changing dimensions (Type 2) for stock close prices, tracking changes over time. Since this is not an append-only table anymore, streaming cannot be applied here.
- **Current Close**: Extracts the most recent close price from the SCD 2 table and acts as a static source for joining with the streaming data from the Stocks table.
- **Stocks Close**: Combines data from the Stocks and Current Close tables to provide a comprehensive view of the current stock status.

- **Data Flow**: 
  - Batch processing is used to move data from the Close Price (SCD 2) table to the Current Close table.
  - **Static Source Join**: The Current Close table acts as a static source and is joined with the streaming data from the Stocks table to update the Stocks Close table.

### Gold Layer:

- **Aggregated and Enriched Data**: This layer contains the final, refined data ready for analysis.
- **Table**: 
  - **Updated Stocks**: Combines data from the Stocks Close table to provide a comprehensive view of the current stock status.
- **Data Flow**: Batch processing is used to create the Updated Stocks table from the Stocks Close data.


This flow focuses on the unique elements of data ingestion and real-time processing:

#### Data Ingestion:

- **Source**: The pipeline starts with CSV files ingested using Spark Streaming.
- **Upstash and Kafka**: The ingested data is then streamed to Kafka via Upstash, facilitating real-time data streaming and ingestion.

#### Real-Time Processing:

- **Spark Streaming**: Continues to process data from Kafka, ensuring that data remains up-to-date as it moves through the pipeline.

The subsequent steps (Bronze, Silver, and Gold layers) follow a similar processing pattern, with data moving through these layers in Databricks and Delta Lake for refinement, transformation, and feature extraction.






## CI/CD Implemention using DAB and Github Actions

1. Install the Databricks CLI from https://docs.databricks.com/dev-tools/cli/databricks-cli.html

2. Authenticate to your Databricks workspace, if you have not done so already:
    ```
    $ databricks configure
    ```

3. To deploy a development copy of this project, type:
    ```
    $ databricks bundle deploy --target dev
    ```
    (Note that "dev" is the default target, so the `--target` parameter
    is optional here.)

    This deploys everything that's defined for this project.
    For example, the default template would deploy a job called
    `[dev yourname] kafka_stream_job` to your workspace.
    You can find that job by opening your workpace and clicking on **Workflows**.

4. Similarly, to deploy a production copy, type:
   ```
   $ databricks bundle deploy --target prod
   ```

   Note that the default job from the template has a schedule that runs every day
   (defined in resources/kafka_stream_job.yml). The schedule
   is paused when deploying in development mode (see
   https://docs.databricks.com/dev-tools/bundles/deployment-modes.html).

5. To run a job or pipeline, use the "run" command:
   ```
   $ databricks bundle run
   ```

6. Optionally, install developer tools such as the Databricks extension for Visual Studio Code from
   https://docs.databricks.com/dev-tools/vscode-ext.html.

7. For documentation on the Databricks asset bundles format used
   for this project, and for CI/CD configuration, see
   https://docs.databricks.com/dev-tools/bundles/index.html.
