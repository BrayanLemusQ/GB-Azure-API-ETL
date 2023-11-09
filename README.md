## GB-Azure-API-ETL
Azure API, DataFactory, Databricks

This project implements a data storage and analysis solution using Azure's DataLakeGen2 architecture to provide scalable and highly accessible data storage.

## Architecture

The DataLakeGen2 architecture is built on Azure Blob Storage, enhancing metadata management capabilities and data access performance. This allows for a hierarchical data model through storage layers according to the changes and characteristics of the information.

![General Architecture](/GB-Azure-API-ETL/images/DataLakeGen2Architecture.png)
*Figure 1: General overview of the project architecture*

### Key Components

- **Azure Blob Storage**: Object storage for the Data Lake.
- **Azure Data Factory**: A data integration service that allows data transfer and transformation.
- **Azure Databricks**: An Apache Spark-based analytics platform for data processing.
- **Azure Function HttpRequest**: A service that acts as an API interface to execute the project's functionalities.
- **Azure Event DataGrid**: A service that manages events between different resources.


### Resource Group
![Resource Group](/GB-Azure-API-ETL/images/ResourceGroup.png)
*Figure 2: Resource group*

### Data Storage Layers

![Layers](/GB-Azure-API-ETL/images/DataLake-Layers.png)
*Figure 3: DataLake layers*

- **Source**: The origin of the information where files will be located for later processing.
- **Bronze Layer**: At this layer, a copy of the raw data is made exactly as it is received from the source, versioning these entries to facilitate audits, reviews, or corrections.
![bronze](/GB-Azure-API-ETL/images/bronze.png)

*Figure 4: Bronze layer*
- **Silver Layer**: This layer stores updated information from new records and updates that may have occurred in different versions of the source files. Unlike the bronze layer, multiple versions are not kept here since an updated and cleaned list is maintained, ready for processing.
![silver](/GB-Azure-API-ETL/images/silver.png)
*Figure 5: Silver layer*
- **Gold Layer**: This layer stores only the information that has undergone some process of analysis, enrichment, or filtering; in this case, the generated reports.
![gold](/GB-Azure-API-ETL/images/gold.png)
*Figure 6: Gold layer*


### Azure Data Factory Data Processing

- **Data Update**: This pipeline is responsible for reading the file located in the established path, and through a data copy activity, it passes the information from the file to a specific version in the bronze layer of the architecture, making use of the date and time when the execution began. 
After copying the information, it proceeds to execute the Databricks notebook that is in charge of cleaning and purifying the data to store the changes in the silver layer.
![gold](/GB-Azure-API-ETL/images/UpdateData.png)
*Figure 7: Data update*

- **Execution of information analysis**: For this case, two pipelines have been arranged whose mission is to obtain results according to the data crossing of the previously provisioned tables.
![gold](/GB-Azure-API-ETL/images/DataAnalysis.png)
*Figure 8: Data analysis*
- **Controlled execution**: All executions are controlled by calling the APIs generated through Azure function with Http request, and in turn linked with the trigger of each of the pipelines in Azure data factory to ensure correct execution.
![gold](/GB-Azure-API-ETL/images/ApiExecution.png)
*Figure 8: Data analysis*


## Version 
* 1.0.0

## Author
* **Brayan Lemus**
* **For educational and instructional purposes**


---