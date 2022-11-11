## Weaviate Data Importer Wizard POC

This app is designed to demonstrate populating a Weaviate instance with data quickly and easily. 

The app is built with Streamlit; and a proof of concept (POC) only. It is *NOT* intended for use in a production environment. 

#### Installation

Clone the repo, and install dependencies using Poetry
(Optional) Spin up a local instance of Weaviate, e.g. using the included docker-compose file
From within the Poetry environment, run the app with `streamlit run app.py`

#### Usage
These instructions are applicable within the app

**Connect to Weaviate**
Specify the URL of the Weaviate instance in the URL text box and press enter. If successful, it should reveal additional options to add data below.

**Add data**
There are two options to add data. One is to add pre-defined demo data, including the schema. The other is to add your own data in CSV or JSON form, from which the app will attempt to infer Weaviate data types with which to import.
