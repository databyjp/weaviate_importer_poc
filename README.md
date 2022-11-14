## Weaviate Data Importer Wizard POC

This app is designed to demonstrate populating a Weaviate instance with data quickly and easily. 

**Please note that this is a proof of concept (POC) only.** It is *NOT* intended for use in a production environment or anything resembling it. 

### Installation
- Clone the repo, and install dependencies using Poetry.
- (Optional) Spin up a local instance of Weaviate, e.g. using the included docker-compose file
- From within the Poetry environment, run the app with `streamlit run app.py`

> **Note**: The initial development was carried out in a Conda/Mamba environment with Python 3.9.13, Streamlit 1.14.0, pandas 1.4.1 and weaviate-client = 3.8.0

### Usage
These instructions are for using the app GUI.

> **Note**: Make sure to press return after changing any of the values in the dialog boxes. Otherwise the variable may not be updated.

#### Connect to Weaviate
Specify the URL of the Weaviate instance in the URL text box and press enter. If successful, it should reveal additional content below. The additional content should include details about the existing data in the Weaviate instance, as well as options further below for adding data.

Throughout use, click on the **Update data** button to get the latest status from the Weaviate instance. It will not update automatically after the end of each operation.

#### Add data
There are two options to add data. One is to add pre-defined demo data, including the schema. The other is to add your own data in CSV or JSON form, from which the app will attempt to infer Weaviate data types with which to import.

**Import a demo dataset**

This section allows you to import data using a predefined dataset and schema. To import data, set the "Max objects to import"

**Import your own data**
In this section, you can select a JSON or CSV file located in the `data` subdirectory. The app will load the data and build a preliminary mapping of data types to import the data with. 

Modify the **Weaviate datatypes** as required, as well as the **class name** to be used. Then, click `Add this class` to add the class to the schema. 

Then, select the number of maximum data objects to be added, before clicking **add data**.

#### Datasets used
- The demo dataset import uses this [Wine Reviews](https://www.kaggle.com/datasets/zynicide/wine-reviews) dataset from Kaggle. The others are placeholders.
- The "import your own data" feature has been tested with:
    - [Fake/Real news dataset](https://www.kaggle.com/datasets/clmentbisaillon/fake-and-real-news-dataset) from Kaggle as `data/Fake.csv` and `data/True.csv`.
    - [Amazon Reviews - "5-Core" video game reviews subset](https://jmcauley.ucsd.edu/data/amazon/) - as `data/Video_Games_5.json`.

### TODO
- Add a selection mechanism for vectorization during import, such that the user can choose which columns to vectorize/not vectorize.
- Add other datasets for demo import (example datasets listed as placeholders)
- Test user import 
