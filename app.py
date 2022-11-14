import streamlit as st
from pathlib import Path
import weaviate as wv
import utils    
import math
import pandas as pd

# ===== START STREAMLIT APP =====

st.set_page_config(layout="wide")
st.header("Data importer wizard 🧙‍♀️🪄 POC")
with st.expander("Turn users into Wizards!"):
    st.markdown("![Image](data:image/jpeg;base64,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)")
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
    # t1, t2 = st.tabs(['Demo Datasets', 'Your Own Data'])

    # with t1:
    #     # st.write("**Create your own dataset**")
    #     # manual_data = st.text_area("Specify your own data as a JSON", value="example JSON goes here")

    with st.expander("Demo Datasets"):
        # ===== Dataset =====
        st.write("**Import a demo dataset**")
        st.write("Import any of the below datasets in just one click.")
        demo_max_objs = st.number_input("Max objects to import", value=500)
        st.markdown("-----")        

        def import_csv_data(client, fpath, class_name, max_objs=1000, skip_schema=False):
            class_obj = utils.get_csv_to_class(fpath, class_name)
            if not skip_schema:
                utils.build_schema(client, class_obj)
                obj_count = utils.get_object_count(client, class_obj["class"])
            else:
                obj_count = 0
            df = pd.read_csv(fpath, index_col=0)
            df = df.fillna(df.mode().iloc[0])

            with client.batch as batch:
                for i, rowdata in df[obj_count:].iterrows():
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
            {"name": "Wine reviews", "description": "Wine reviews from [Kaggle](https://www.kaggle.com/datasets/zynicide/wine-reviews) containing 150k reviews.", "callback": import_csv_data, "args": [client, "data/winemag-data-130k-v2.csv", "WineReview", demo_max_objs], "disabled": False},
            {"name": "Yelp reviews", "description": "Placeholder: 7M Yelp Reviews.", "callback": import_csv_data, "args": [client, "data/winemag-data-130k-v2.csv", "YelpReview", demo_max_objs], "disabled": True},
            {"name": "Tiny ImageNet", "description": "Placholder: 100000 images of 200 classes.", "callback": import_csv_data, "args": [client, "data/winemag-data-130k-v2.csv", "ImageNetImage", demo_max_objs], "disabled": True},
        ]        
        bcs = st.columns(len(demo_datasets))        
        for i, bc in enumerate(bcs):
            with bcs[i]:
                st.write(f"**{demo_datasets[i]['name']}**")
                st.write(f"{demo_datasets[i]['description']}")
                if demo_datasets[i]['args'][2] not in class_names:
                    st.button(f"Import", key=f"default_import_button_{i}",  on_click=demo_datasets[i]['callback'], args=demo_datasets[i]['args'], disabled=demo_datasets[i]['disabled'])
                else:
                    st.button("Import more data", on_click=demo_datasets[i]['callback'], args=demo_datasets[i]['args'] + [True])

        import_progbar = st.progress(st.session_state['import_progress'])
        st.markdown("-----")

        # - If the class does not exist, enable the import button    

with st.expander("Your own data"):
        st.write("**Import data from file**")
        datadir = "./data"
        path = Path(datadir)
        data_files = ["."] + [f for f in path.glob('*.json')] + [f for f in path.glob('*.csv')]

        if 'datafile_ind' not in st.session_state:
            st.session_state['datafile_ind'] = 0
        
        fpath = st.selectbox('Select file to import', data_files, st.session_state['datafile_ind'], key=3)
        st.session_state['datafile_ind'] = data_files.index(fpath)

        if fpath != ".":
            col1, col2 = st.columns(2)

            # ===== Build DataFrame for schema building
            st.write("**Data Preview**")
            st.markdown("Adjust the detected datatypes as appropriate for the schema.")

            df = utils.get_preview_df(fpath) 
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

            if custom_classname not in class_names:
                st.button("Add this ☝️ class", on_click=utils.build_schema, args=[client, class_obj])
            else:
                st.button("Class found", disabled=True, key="found")

            # ===== Add Data
            st.write("**Import data**") 

            def add_data(fpath, progbar, max_objs=200):
                if fpath.suffix == '.json':
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
                else:
                    progbar.progress(0) 
                    df = pd.read_csv(fpath)
                    with client.batch as batch:
                        for i, rowdata in df.iterrows():
                            batch.add_data_object(
                                data_object=rowdata.to_dict(),
                                class_name=class_obj["class"],
                            )        
                            import_progbar.progress(i / max_objs)
                            if i+1 >= max_objs:
                                break                                        
                
                st.session_state['custom_import_progress'] = 100
                return True


            cust_max_objs = st.number_input("Max objects", value=500)
            upload_progbar = st.progress(st.session_state['custom_import_progress'])
            st.button("Add data", on_click=add_data, args=[fpath, upload_progbar, cust_max_objs], key="custom_data")
