import streamlit as st
from pathlib import Path
import weaviate as wv
import utils    
import math

# ===== START STREAMLIT APP =====

st.set_page_config(layout="wide")
st.header("Weaviate data importer POC")
st.markdown("-----")

# ===== QUERY CURRENT DATABASE =====
st.write("### Database contents")
wv_url = st.text_input("Server URL:", value="http://localhost:8080/")

client = wv.Client(wv_url)
client.batch.configure(
    batch_size=100,
    dynamic=False,
    timeout_retries=3,
)          

# TODO - add text input for class name 
# TODO - check class name validation
wv_classname = "Review"
wv_classdesc = f"Contains {wv_classname} data"

if 'schema' not in st.session_state:
    st.session_state['schema'] = utils.get_schema(client)

if 'obj_count' not in st.session_state:
    st.session_state['obj_count'] = utils.get_object_count(client, wv_classname)

def update_dbstats(client): 
    st.session_state['schema'] = utils.get_schema(client)
    st.session_state['obj_count'] = utils.get_object_count(client, wv_classname)

# ===== FETCH CURRENT STATUS =====
summ_c1, summ_c2 = st.columns(2)
n_classes = len(st.session_state['schema']['classes'])
with summ_c1:
    st.write(f"{n_classes} class(es) in schema")
    st.button("Update database stats", on_click=update_dbstats, args=[client])
with summ_c2:
    st.write(f"{st.session_state['obj_count']} object(s) in database")

with st.expander("Preview schema / data here"):
    state_c1, state_c2 = st.columns(2)
    with state_c1:
        sch_select = st.selectbox("Pick a class", ['Reviews', 'CatNames'])
        st.write("**Schema**")
        st.write(st.session_state['schema'])
    with state_c2:
        st.write("**Example objects**")        
        st.write(client.data_object.get())

st.markdown("-----")

# ===== SET UP FILE PARSING =====
st.write("### Add your own dataset")
st.write("You can also bring your own dataset. Select a file to import from, or create rows manually with the table here.")
t1, t2 = st.tabs(['Import', 'Create'])

with t1:
    st.write("**Import data from file**")
    datadir = "./data"
    path = Path(datadir)
    data_files = [f for f in path.glob('*.json')]

    fpath = st.selectbox('Select file to import', data_files, 0, key=3)

    col1, col2 = st.columns(2)
    wv_classname = st.text_input("Class name", placeholder="UserData")

    # ===== Build DataFrame for schema building
    st.write("**Data Preview**")
    st.markdown("Adjust the detected datatypes as appropriate for the schema.")

    df = utils.get_preview_df(fpath)
    st.dataframe(df)


    # ===== Select data types for schema
    st.write("**Schema builder**")


    dtype_maps = {object: "string", int: "int", bool: "boolean", float: "number", "default": "string"}
    dtype_map_keys = list(dtype_maps.keys())


    def get_dtype_index(datatype):
        try:
            return dtype_map_keys.index(datatype)
        except:
            return dtype_map_keys.index(dtype_maps["default"])



    n_cols = 3
    cols = st.columns(n_cols)
    n_elms = math.ceil(len(df.dtypes) / n_cols)
    dtype_series_parts = [df.dtypes[n_elms * i:n_elms * (i+1)] for i in range(n_cols)]

    def build_schema_boxes(dtype_sers):
        return [
            st.selectbox(
                f"{colname}", 
                dtype_maps.values(), 
                get_dtype_index(datatype), 
                key=colname,
            ) for colname, datatype in dtype_sers.iteritems()
        ]    


    wv_dtypes_parts = [[]] * n_cols
    for i in range(n_cols):
        with cols[i]:
            wv_dtypes_parts[i] = build_schema_boxes(dtype_series_parts[i])


    wv_dtypes = wv_dtypes_parts[0] + wv_dtypes_parts[1] + wv_dtypes_parts[2]


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


    # ===== Add Data
    st.write("**Import data**")


    def add_data(max_objs=200):
        upload_progbar.progress(0)
        with client.batch as batch:
            for i, l in enumerate(utils.parse_json(fpath)):  # TODO - add CSV parsing option
                upload_progbar.progress(i / max_objs)
                batch.add_data_object(
                    data_object=l,
                    class_name=wv_classname,
                )
                print(i)
                if i+1 >= max_objs:
                    break
        
        st.session_state['import_progress'] = 100
        return True


    max_objs = st.number_input("Max objects", value=500)
    if 'import_progress' not in st.session_state:
        st.session_state['import_progress'] = 0
    upload_progbar = st.progress(st.session_state['import_progress']) 

    st.button("Add data", on_click=add_data, args=[max_objs])


with t2:
    # st.write("**Create your own dataset**")
    # manual_data = st.text_area("Specify your own data as a JSON", value="example JSON goes here")
    # ===== Dataset =====

    demo_datasets = [
        {"name": "Wine reviews", "description": "1M written wine reviews from magazines."},
        {"name": "Amazon reviews", "description": "10,000 video game reviews."},
        {"name": "GitHub issues", "description": "500,000 entries of GitHub issues (exluding xxx)."},
        {"name": "Mini wiki", "description": "3M Wikipedia entries."},
    ]
    st.write("### Demo datasets")
    bcs = st.columns(4)
    for i, bc in enumerate(bcs):
        with bcs[i]:
            st.write(f"This will import the {demo_datasets[i]['name']} dataset {demo_datasets[i]['description']}")
            st.button(f"Import", key=f"default_import_button_{i}")
    st.markdown("-----")
    # - If the class does not exist, enable the import button    
