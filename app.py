import streamlit as st
from pathlib import Path
import json
import pandas as pd
import weaviate as wv
client = wv.Client("http://localhost:8080/")
client.batch.configure(
    batch_size=100,
    dynamic=False,
    timeout_retries=3,
)          

def parse(path):
    with open(path, 'r') as g:
        line = g.readline()
        while line:
            yield json.loads(line)
            line = g.readline()  


def get_schema():
    schema = client.schema.get()
    st.session_state['schema'] = schema
    return True    


def delete_schema():
    client.schema.delete_all()
    return True 


def get_object_count():
    obj_count = client.query.aggregate("Review").with_fields('meta { count }').do()
    st.session_state['obj_count'] = obj_count
    return True      


datadir = "./data"
path = Path(datadir)
data_files = [f for f in path.glob('*.json')]


if 'schema' not in st.session_state:
    st.session_state['schema'] = 'UNK'

if 'obj_count' not in st.session_state:
    st.session_state['obj_count'] = 'UNK'


st.header("Weaviate data importer POC")
fpath = st.selectbox('Select file to import', data_files, 0)

col1, col2 = st.columns(2)
with col1:
    st.button("Get schema", on_click=get_schema)
    st.button("Delete schema", on_click=delete_schema)
    st.write(st.session_state['schema'])

    st.button("Get number of objects", on_click=get_object_count)
    st.write(st.session_state['obj_count'])
    # st.button("Delete schema", on_click=delete_schema)    

with col2:
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
        "class": "Review",
        "description": "An Amazon review entry",
        "properties": props,
    }


    def build_schema(class_obj_in):
        client.schema.create_class(class_obj_in)
        return True


    st.button("Add this ☝️ schema", on_click=build_schema, args=[class_obj])


    # ===== Add data
    st.subheader("Add data")

    

    def add_data(max_objs=200):
        with client.batch as batch:
            for i, l in enumerate(parse(fpath)):
                batch.add_data_object(
                    data_object=l,
                    class_name="Review",
                )
                if i+1 >= max_objs:
                    break
            # client.batch.flush()
    
    
    max_objs = st.number_input("Max objects", value=500)
    st.button("Add data", on_click=add_data, args=[max_objs])