import streamlit as st
from pathlib import Path
import json
import pandas as pd
import weaviate as wv
import utils    


def delete_schema():
    client.schema.delete_all()
    return True 


# ===== START STREAMLIT APP =====

st.set_page_config(layout="wide")
if 'schema' not in st.session_state:
    st.session_state['schema'] = 'UNK'

if 'obj_count' not in st.session_state:
    st.session_state['obj_count'] = 'UNK'


st.header("Weaviate data importer POC")
wv_url = st.text_input("Server URL:", value="http://localhost:8080/")

# ===== SETUP WEAVIATE CONNECTIONS =====
client = wv.Client(wv_url)
client.batch.configure(
    batch_size=100,
    dynamic=False,
    timeout_retries=3,
)          

wv_classname = "Review"
wv_classdesc = f"Contains {wv_classname} data"
 

def update_schema(client): st.session_state['schema'] = utils.get_schema(client)
def update_object_count(client): st.session_state['obj_count'] = utils.get_object_count(client, wv_classname)


# ===== SET UP FILE PARSING =====

def parse(path):
    with open(path, 'r') as g:
        line = g.readline()
        while line:
            yield json.loads(line)
            line = g.readline()  
datadir = "./data"
path = Path(datadir)
data_files = [f for f in path.glob('*.json')]



st.subheader("Database state")
st.button("Get schema", on_click=update_schema, args=[client])
st.button("Delete schema", on_click=delete_schema)
st.write(st.session_state['schema'])

st.button("Get number of objects", on_click=update_object_count(client))
st.write(st.session_state['obj_count'])

st.markdown("-----")


fpath = st.selectbox('Select file to import', data_files, 0)

col1, col2 = st.columns(2)

# ===== Build DataFrame for schema building
st.subheader("Data Preview")
st.markdown("Adjust the detected datatypes as appropriate for the schema.")
dlist = list()
for i, l in enumerate(parse(fpath)):
    dlist.append(l)
    if i+1 >= 100:
        break

df = pd.DataFrame(dlist)
st.dataframe(df)


# ===== Select data types for schema
st.subheader("Add a schema")


dtype_maps = {object: "string", int: "int", bool: "boolean", float: "number", "default": "string"}
dtype_map_keys = list(dtype_maps.keys())


def get_dtype_index(datatype):
    try:
        return dtype_map_keys.index(datatype)
    except:
        return dtype_map_keys.index(dtype_maps["default"])


wv_dtypes = [
    st.selectbox(
        f"{colname} - datatype", 
        dtype_maps.values(), 
        get_dtype_index(datatype), 
        key=colname,
    ) for colname, datatype in df.dtypes.iteritems()
]


# ===== Add Schema
props = [
    {
        "dataType": [wv_dtypes[i]],
        "name": df.columns[i],
        "description": f"Contains_{df.columns[i]}"
    } for i in range(len(wv_dtypes))
]


class_obj = {
    "class": wv_classname,
    "description": wv_classdesc,
    "properties": props,
}


def build_schema(class_obj_in):
    client.schema.create_class(class_obj_in)
    return True


st.button("Add this ☝️ schema", on_click=build_schema, args=[class_obj])
st.subheader("Add data")



def add_data(max_objs=200):
    with client.batch as batch:
        for i, l in enumerate(parse(fpath)):
            upload_progbar.progress(i / max_objs)
            batch.add_data_object(
                data_object=l,
                class_name=wv_classname,
            )
            print(i)
            if i+1 >= max_objs:
                break
        # client.batch.flush()
    st.session_state['import_progress'] = 100
    return True


max_objs = st.number_input("Max objects", value=500)
if 'import_progress' not in st.session_state:
    st.session_state['import_progress'] = 0
upload_progbar = st.progress(st.session_state['import_progress']) 

st.button("Add data", on_click=add_data, args=[max_objs])