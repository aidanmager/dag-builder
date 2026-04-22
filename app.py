import streamlit as st
import json
from streamlit import session_state as ss
from utils.dag_builder import dag_builder



DEFAULT_PROJECT_NAME = "SQUARE_POS"
DEFAULT_DATABASE = "SQUARE_POS"
DEFAULT_INGEST_SCHEMA = "INGEST"
DEFAULT_CLEANSE_SCHEMA = "CLEANSE"
DEFAULT_STAGE = "SQUARE_INGEST"
DEFAULT_OUTPUT_FOLDER = "_output"

# datasets
DATASET_EXAMPLE = {
    "NAME": "location",
    "JSON": {
        "catalog_object_id": "4JTZG4SWSSQQCGULR7KPQLOT",
        "catalog_object_type": "ITEM_VARIATION",
        "state": "IN_STOCK",
        "location_id": "LNNXM9HJ66XE9",
        "quantity": "53",
        "calculated_at": "2024-12-02T18:39:37.915Z"
    },
    "PK": "catalog_object_id, location_id, calculated_at"
}

st.set_page_config(page_title="DAG Builder", layout='centered')


col1,col2 = st.columns([0.5, 0.5])
with col1:
    st.title('DAG BUILDER')

with col2:
    st.image("utils/extras/dag_builder_dino_2.png")

project_name        = st.text_input("Project Name", 
                                    value=DEFAULT_PROJECT_NAME)
database_name       = st.text_input("Database Name", 
                                    value=DEFAULT_DATABASE)
ingest_schema_name  = st.text_input("Ingest Schema Name", 
                                    value=DEFAULT_INGEST_SCHEMA)
cleanse_schema_name = st.text_input("Cleanse Schema Name", 
                                    value=DEFAULT_CLEANSE_SCHEMA)
stage_name          = st.text_input("Ingest Stage Name",
                                    value=DEFAULT_STAGE)

cron_schedule = "0 7 * * *"

create_files:bool = st.checkbox(label="Create Files?", value=False)
if create_files:
    output_folder = st.text_input("Output Folder", value=DEFAULT_OUTPUT_FOLDER)
else:
    output_folder = ""


json_help = f"Give an example record of the data in JSON. Ex:\n" \
    f"{json.dumps(DATASET_EXAMPLE["JSON"], indent=2)}"
if "dataset_key" not in ss.keys(): 
    ss.dataset_key = 1

st.header("Datasets", divider="grey", 
          help="Add different data retrieved from the data source" \
            "to generate SQL files to load data into Snowflake")

for key in range(ss.dataset_key):
    with st.container(border=True):
        st.subheader(key+1, divider="blue")
        st.text_input("Name", value=DATASET_EXAMPLE["NAME"], key=f"{key}_name", 
                      help="This will be the name of the ingest/cleanse table in Snowflake")
        st.checkbox("Add Time Bounds", key=f"{key}_add_time_bounds", 
                    help="For time series data, this will create columns based " \
                        "on the data_interval_start and data_interval_end " \
                        "variables of the DAG run. Will automatically add " \
                        "these fields to the primary_key.")

        
        st.text_area("Data as JSON:", 
                     value=json.dumps(DATASET_EXAMPLE["JSON"]), 
                     key=f"{key}_data",
                     height=350,
                     help=json_help)
        st.text_input("Primary Keys (as comma seperated list)", value=DATASET_EXAMPLE["PK"], key=f"{key}_pk")


def add_dataset():
    ss.dataset_key += 1

def remove_dataset():
    ss.dataset_key -= 1


col1, col2 = st.columns(2)
with col1:
    st.button("Add dataset", on_click=add_dataset, use_container_width=True)
with col2:
    st.button("Remove dataset", on_click=remove_dataset, use_container_width=True)


def create_dag():
    if project_name is None or project_name == "":
        st.text("Error: No project name given...")
        return
    
    if cron_schedule is None or cron_schedule == "":
        st.text("Error: No cron schedule given...")
        return

    datasets = []
    for i in range(ss.dataset_key):

        table_name = ss[f"{i}_name"]
        data = json.loads(ss[f"{i}_data"])
        config = {
            "primary_key": [s.strip() for s in ss[f"{key}_pk"].split(",")], 
            "add_time_bounds":ss[f"{key}_add_time_bounds"]
        }

        datasets.append((table_name, {"data":data, "_config":config}))


    result = dag_builder(
        project_name=project_name,
        datasets=datasets,
        cron_schedule=cron_schedule,
        output_folder=output_folder,
        database_name=database_name,
        ingest_schema_name=ingest_schema_name,
        cleanse_schema_name=cleanse_schema_name,
        simple_mode = not create_files
    ).run_dag_builder()

    if not result:
        return
    
    if not create_files:
        for table_name, dataset in result.items():
            with st.container(border=True):
                st.header(table_name.upper())
                if table_name == "ddl":
                    st.code(dataset, language="sql")
                else:
                    for sql_name, sql_text in dataset.items():
                        st.subheader(sql_name)
                        st.code(sql_text, language="sql")

    else:
        st.text("Done Creating Files")
        # st.download_button("Download Files", data=output_folder)

st.button("CREATE!", on_click=create_dag, use_container_width=True)