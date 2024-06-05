# **Google Cloud Cortex Framework**
[CCW Cortex Documentation](https://github.com/CortexCouncilWorkspace/ccw-documentation) CCW helps you document and understand the views that exist in the environment.

## **Deployment steps**

These are the steps for deployment:

0.  [Prerequisites](#prerequisites)
1.  [Configure cloud platform components](#configure-google-cloud-platform-components)
2.  [Clone repository](#clone-repository)
3.  [Deployment configuration](#deployment-configuration)
4.  [Run Python file](#run-python-file)

## Prerequisites
Load SAP catalog data tables:
* DD02T
* DD03L
* DD04T

Create service account for Vertex, Dataset documentation and Dataset catalog

## Configure Google Cloud Platform components

### Enable required components

The following Google Cloud components are required:

*   Google Cloud Project
*   BigQuery instance and datasets
*   Service Account
*   Vertex AI 
  
## Clone repository
Clone repositry from https://github.com/CortexCouncilWorkspace/ccw-documentation to your own repository


## Deployment configuration
* Add service accounts to the [account directory](account/)
* Configure parameters [config.json](config/config.json)

| Parameter                 | Meaning                 | Description                                                                      |
| ------------------------- | ----------------------- | ------------------------------------------------------------------------         |
| `projectId`               | Core Project ID         | Core Project ID if you are using the same project for vertex, catalogs and documentation.                      |
| `authJson`                | Core Authorization json file path | Core authorization json file path if you are using the same project for vertex, catalogs and documentation.  |
| `Catalog: projectId`      | Catalog Project ID      | Project ID for catalogs data
| `Catalog: authJson`       | Catalog Authorization json file path | Catalog authorization json file path |
| `Catalog: datasetId`      | Catalog Dataset         | Catalog Dataset for catalogs data |
| `Vertex: projectId`       | Vertex Project ID       | Project ID for vertex execution |
| `Vertex: authJson`        | Vertex Authorization json file path | Vertex authorization json file path |
| `Vertex: gemini`          | Vertex Gemini           | Gemini version (gemini-pro, gemini-1.5-pro-001, ...) |
| `Vertex: location`        | Vertex Location         | Vertex location for vertex execution |
| `Documentation: projectId`| Documentation Project ID| Project ID of the documentation generation dataset  |
| `Documentation: authJson` | Documentation Authorization json file path | Authorization json file path of the documentation generation dataset |
| `Documentation: datasetId`| Documentation Dataset   | Documentation Dataset of the documentation generation dataset  |
| `Documentation: views`    | List of Views           | List of views to generate the documentation |
| `Documentation: languages`| List of languages       | List of languages to generate the documentation |

## Run python file

On your machine run the python [createdocumentation.py](createdocumentation.py)



