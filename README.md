# Food Delivery Real-Time Data Analysis

## Overview

This project demonstrates a real-time data analysis pipeline for food delivery systems, leveraging AWS services for scalable and fault-tolerant data processing. The architecture ingests real-time food order data, processes it using Apache Spark Streaming on Amazon EMR, and loads it into Amazon Redshift for analytics. The processed data can then be visualized using Amazon QuickSight.

![Architecture Diagram](https://github.com/desininja/Food-Delivery-RealTime-Data-Analysis/blob/main/Food%20Delivery%20RealTime%20Data%20Analysis%20Architecture.png)

---

## Key Features

- **Real-time Data Processing:** Uses Amazon Kinesis to stream real-time data, processed using PySpark on an Amazon EMR cluster.
- **Orchestration with Airflow:** Two Amazon Managed Workflows for Apache Airflow (MWAA) DAGs manage the pipeline:
  - **`create_and_load_dim` DAG:** Initializes Redshift tables and loads dimensional data.
  - **`submit_pyspark_streaming_job_to_emr` DAG:** Triggers the Spark streaming job.
- **AWS Services Integration:** Utilizes a suite of AWS tools for efficient data processing:
  - **S3:** Storage for raw and processed data.
  - **Redshift:** Data warehousing for real-time and batch data analytics.
  - **Kinesis:** Real-time data streaming.
  - **EMR:** Spark cluster for processing streaming data.
  - **CodeBuild:** CI/CD for Spark job deployments.
  - **QuickSight:** Dashboard and analytics visualization.
- **Custom Data Generator:** Simulates user food orders for testing and demonstration purposes.

---

## Repository Structure

```plaintext
Food-Delivery-RealTime-Data-Analysis/
├── dags/
│   ├── redshift_create_tables_dag.py    # Creates Redshift tables and loads dimensional data.
│   ├── spark_streaming_dag.py          # Triggers the PySpark job on EMR.
├── scripts/
│   ├── orders_spark_streaming.py       # PySpark script for processing real-time data from Kinesis.
├── mock_data_generator.py              # Python script to simulate user food orders.
├── Food Delivery RealTime Data Analysis Architecture.png  # Architecture diagram.
└── README.md                           # Project documentation.
```

---

## AWS Services Used

- **Amazon S3:** Stores raw and processed data.
- **Amazon Redshift:** Hosts dimensional and fact tables for analytics.
- **Amazon Kinesis:** Streams real-time order data to be processed.
- **Amazon EMR:** Runs Spark Streaming for processing Kinesis data.
- **Amazon Managed Airflow (MWAA):** Orchestrates workflows with custom DAGs.
- **Amazon QuickSight:** Provides insights with data visualization dashboards.
- **AWS CodeBuild:** Automates deployment and testing of PySpark scripts.

---

## Workflow

1. **Data Simulation:**
   - A Python-based mock data generator simulates real-time food orders and publishes them to an Amazon Kinesis Data Stream.
   
2. **Orchestration with Airflow:**
   - **DAG 1:** `create_and_load_dim`
     - Creates dimensional and fact tables in Amazon Redshift.
     - Loads static data into the dimensional tables.
   - **DAG 2:** `submit_pyspark_streaming_job_to_emr`
     - Triggers the Spark streaming job on Amazon EMR after the first DAG completes.

3. **Real-Time Data Processing:**
   - The Spark streaming job processes micro-batches from Kinesis and loads the results into Redshift's fact table.
   - **Redshift JAR Dependency:** The PySpark script relies on the `Redshift JDBC Driver` JAR file for connecting to the Redshift database. Ensure the JAR file is available in the EMR cluster and referenced in the script. The JAR file can be downloaded from the [Amazon Redshift JDBC Drivers page](https://docs.aws.amazon.com/redshift/latest/mgmt/configure-jdbc-connection.html).

4. **Data Analytics:**
   - Amazon QuickSight connects to Redshift for real-time analytics and visualization.

---

## Scripts Description

### Airflow DAGs
- **[redshift_create_tables_dag.py](https://github.com/desininja/Food-Delivery-RealTime-Data-Analysis/blob/main/dags/redshift_create_tables_dag.py):**
  - Creates Redshift tables and loads dimensional data.
- **[spark_streaming_dag.py](https://github.com/desininja/Food-Delivery-RealTime-Data-Analysis/blob/main/dags/spark_streaming_dag.py):**
  - Triggers the Spark Streaming job on EMR.

### PySpark Script
- **[orders_spark_streaming.py](https://github.com/desininja/Food-Delivery-RealTime-Data-Analysis/blob/main/scripts/orders_spark_streaming.py):**
  - Reads data from Kinesis, processes it, and ingests it into Redshift in real time.
  - **Redshift Connection JAR:** Ensure the `Redshift JDBC Driver` is added to the classpath when running this script.

### Mock Data Generator
- **[mock_data_generator.py](https://github.com/desininja/Food-Delivery-RealTime-Data-Analysis/blob/main/mock_data_generator.py):**
  - Generates random food order data and publishes it to the Kinesis stream.

---

## QuickSight Dashboard

The processed data stored in Redshift can be visualized using Amazon QuickSight. Insights such as order trends, popular items, and delivery times can be displayed in an interactive dashboard.

---

## Security Considerations

- **VPC Endpoints:** Ensure proper communication between services using VPC endpoints.
- **IAM Permissions:** Assign appropriate roles and permissions for Kinesis, Redshift, EMR, and S3 access.
- **Security Groups:** Configure security groups for controlled access across services.

---

## Setup Instructions

1. Clone the repository:  
   ```bash
   git clone https://github.com/desininja/Food-Delivery-RealTime-Data-Analysis.git
   ```

2. Configure AWS CLI with appropriate permissions.

3. Deploy Airflow DAGs:
   - Upload `redshift_create_tables_dag.py` and `spark_streaming_dag.py` to your MWAA DAGs folder.

4. Download the [Redshift JDBC Driver](https://docs.aws.amazon.com/redshift/latest/mgmt/configure-jdbc-connection.html) and place it in the EMR cluster classpath.

5. Create required AWS resources:
   - Kinesis Data Stream.
   - Redshift cluster and tables.
   - S3 buckets for raw and processed data.
   - EMR cluster.

6. Run the mock data generator:
   ```bash
   python mock_data_generator.py
   ```

7. Trigger the DAGs in Airflow to initialize the pipeline.

8. Visualize the data using QuickSight by connecting it to Redshift.

---

## Future Enhancements

- Integrate monitoring tools like CloudWatch for pipeline performance.
- Implement error handling and retry mechanisms for fault tolerance.
- Extend the pipeline to support additional data sources.
- Automate infrastructure provisioning using tools like Terraform or AWS CloudFormation.

---

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

---

## Contributing

Contributions are welcome! Please open an issue or submit a pull request for any improvements.

---
