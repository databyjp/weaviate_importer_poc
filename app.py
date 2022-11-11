import streamlit as st
from pathlib import Path
import weaviate as wv
import utils    
import math
import pandas as pd

# ===== START STREAMLIT APP =====

st.set_page_config(layout="wide")
st.header("Weaviate data importer POC")

# ===== CONNECT TO A DATABASE =====
st.write("#### Connect to Weaviate")
if 'wv_url' not in st.session_state:
    st.session_state['wv_url'] = "http://"
wv_url = st.text_input("Server URL:", value=st.session_state['wv_url'])
st.session_state['wv_url'] = wv_url

client = False 
if len(st.session_state['wv_url']) > 7:
    try:
        client = wv.Client(st.session_state['wv_url'])
        client.batch.configure(
            batch_size=100,
            dynamic=False,
            timeout_retries=3,
        )
    except Exception as e:
        st.write("No server found.")
        st.write("**Connection error:**")
        st.warning(e)

# ===== QUERY THE DATABASE =====
if client:
    st.write(f"**CONNECTED** TO {st.session_state['wv_url']}")
    # TODO - add text input for class name 
    # TODO - check class name validation

    if 'schema' not in st.session_state:
        st.session_state['schema'] = utils.get_schema(client)

    if 'obj_count' not in st.session_state:
        st.session_state['obj_count'] = utils.get_tot_object_count(client)

    if 'custom_import_progress' not in st.session_state:
        st.session_state['custom_import_progress'] = 0

    if 'import_progress' not in st.session_state:
        st.session_state['import_progress'] = 0        

    # ===== FETCH CURRENT STATUS =====
    summ_c1, summ_c2 = st.columns(2)
    class_objs = st.session_state['schema']['classes']
    n_classes = len(class_objs)
    if n_classes > 0:
        n_objs = st.session_state['obj_count']
        class_names = [i['class'] for i in class_objs]
    else:
        n_objs = 0
        class_names = []

    with summ_c1:

        def update_dbstats(client): 
            st.session_state['schema'] = utils.get_schema(client)
            st.session_state['obj_count'] = utils.get_tot_object_count(client)
        st.write(f"{n_classes} class(es) and {n_objs} object(s) found in the database!")
        st.button("Update database stats", on_click=update_dbstats, args=[client])
    # with summ_c2:
        # print(st.session_state['obj_count'])
        # st.write(f"{st.session_state['obj_count']} object(s) in database")

    with st.expander("Preview schema / data here"):
        state_c1, state_c2 = st.columns(2)
        if len(st.session_state['schema']['classes']) > 0:
            with state_c1:
                if type(st.session_state['schema']) == dict:
                    wv_classes = [i['class'] for i in st.session_state['schema']['classes']]
                    sch_select = st.selectbox("Pick a class", wv_classes)
                    st.write("**Schema**")
                    st.write(st.session_state['schema']['classes'][wv_classes.index(sch_select)])
            with state_c2:
                st.write("**Example objects**")        
                st.write(client.data_object.get())
        else:
            st.write("Nothing to see here! Try adding a schema and some data!")

    st.markdown("-----")

    # ===== SET UP FILE PARSING =====
    st.write("#### Add Data")
    st.write("You can import data in just a few clicks. Import a demo dataset just one click, or choose your own data.")
    t1, t2 = st.tabs(['Demo Datasets', 'Your Own Data'])

    with t1:
        # st.write("**Create your own dataset**")
        # manual_data = st.text_area("Specify your own data as a JSON", value="example JSON goes here")

        # ===== Dataset =====
        st.write("**Import a demo dataset**")
        st.write("Import any of the below datasets in just one click.")
        demo_max_objs = st.number_input("Max objects to import", value=500)
        st.markdown("-----")        

        def import_csv_data(client, fpath, class_name, max_objs=1000, skip_schema=False):
            class_obj = utils.get_csv_to_class(fpath, class_name)
            if not skip_schema:
                utils.build_schema(client, class_obj)
            df = pd.read_csv(fpath, index_col=0)
            df = df.fillna(df.mode().iloc[0])

            with client.batch as batch:
                for i, rowdata in df.iterrows():
                    batch.add_data_object(
                        data_object=rowdata.to_dict(),
                        class_name=class_obj["class"],
                    )        
                    import_progbar.progress(i / max_objs)
                    if i+1 >= max_objs:
                        break
            st.session_state['import_progress'] = 100
            return True


        demo_datasets = [
            {"name": "Wine reviews", "description": "Wine reviews from [Kaggle](https://www.kaggle.com/datasets/zynicide/wine-reviews) containing 150k reviews.", "callback": import_csv_data, "args": [client, "data/winemag-data-130k-v2.csv", "WineReview", demo_max_objs]},
            # {"name": "Amazon reviews", "description": "10,000 video game reviews.", "callback": import_csv_data, "args": [client, "data/winemag-data-130k-v2.csv", "WineReview", demo_max_objs]},
            {"name": "Yelp reviews", "description": "Placeholder: 7M Yelp Reviews.", "callback": import_csv_data, "args": [client, "data/winemag-data-130k-v2.csv", "YelpReview", demo_max_objs]},
            {"name": "Tiny ImageNet", "description": "Placholder: 100000 images of 200 classes.", "callback": import_csv_data, "args": [client, "data/winemag-data-130k-v2.csv", "ImageNetImage", demo_max_objs]},
        ]        
        bcs = st.columns(len(demo_datasets))        
        for i, bc in enumerate(bcs):
            with bcs[i]:
                st.write(f"**{demo_datasets[i]['name']}**")
                st.write(f"{demo_datasets[i]['description']}")
                if demo_datasets[i]['args'][2] not in class_names:
                    st.button(f"Import", key=f"default_import_button_{i}",  on_click=demo_datasets[i]['callback'], args=demo_datasets[i]['args'])
                else:
                    st.button("Import more data", on_click=demo_datasets[i]['callback'], args=demo_datasets[i]['args'] + [True])

        import_progbar = st.progress(st.session_state['import_progress'])
        st.markdown("-----")

        # - If the class does not exist, enable the import button    

    with t2:
        st.write("**Import data from file**")
        datadir = "./data"
        path = Path(datadir)
        data_files = ["."] + [f for f in path.glob('*.json')]

        fpath = st.selectbox('Select file to import', data_files, 0, key=3)
        if fpath != ".":
            col1, col2 = st.columns(2)

            # ===== Build DataFrame for schema building
            st.write("**Data Preview**")
            st.markdown("Adjust the detected datatypes as appropriate for the schema.")

            dlist = utils.get_preview_data(fpath)
            df = pd.DataFrame(dlist)   
            st.dataframe(df)

            # ===== Select data types for schema
            st.write("**Schema builder**")

            n_cols = 3
            cols = st.columns(n_cols)
            n_elms = math.ceil(len(df.dtypes) / n_cols)
            dtype_series_parts = [df.dtypes[n_elms * i:n_elms * (i+1)] for i in range(n_cols)]

            def build_schema_boxes(dtype_sers):
                return [
                    st.selectbox(
                        f"{colname}", 
                        utils.dtype_maps.values(), 
                        utils.get_dtype_index(datatype), 
                        key=colname,
                    ) for colname, datatype in dtype_sers.iteritems()
                ]    

            wv_dtypes_parts = [[]] * n_cols
            for i in range(n_cols):
                with cols[i]:
                    wv_dtypes_parts[i] = build_schema_boxes(dtype_series_parts[i])


            wv_dtypes = wv_dtypes_parts[0] + wv_dtypes_parts[1] + wv_dtypes_parts[2]

            # ===== Add Schema
            custom_classname = st.text_input("Class name", value="UserData")
            props = [
                {
                    "dataType": [wv_dtypes[i]],
                    "name": df.columns[i],
                    "description": f"Contains_{df.columns[i]}"
                } for i in range(len(wv_dtypes))
            ]
            class_obj = {
                "class": custom_classname,
                "description": f"Contains {custom_classname} data",
                "properties": props,
            }

            st.button("Add this ☝️ schema", on_click=utils.build_schema, args=[client, class_obj])        

            # ===== Add Data
            st.write("**Import data**") 

            def add_data(progbar, max_objs=200):
                progbar.progress(0)
                with client.batch as batch:
                    for i, l in enumerate(utils.parse_json(fpath)):  # TODO - add CSV parsing option
                        progbar.progress(i / max_objs)
                        batch.add_data_object(
                            data_object=l,
                            class_name=custom_classname,
                        )
                        if i+1 >= max_objs:
                            break
                
                st.session_state['custom_import_progress'] = 100
                return True


            cust_max_objs = st.number_input("Max objects", value=500)
            upload_progbar = st.progress(st.session_state['custom_import_progress'])
            if custom_classname not in class_names:
                st.button("Add data", on_click=add_data, args=[upload_progbar, cust_max_objs], key="custom_data")
            else:
                st.button("Class found", disabled=True, key="custom_found")