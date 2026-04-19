import os
import json
from datetime import datetime


class dag_builder():
    """
    This is the main class

    Parameters
    ----------
    project_name: str
        The name of the project. Often the data source name (ie "Square")
    datasets: str or list[tuple[str, dict]]
        Either a path to data files or a list of tuples containing
        data examples. If a path, it should be a directory containing one or more 
        JSON files with the following structure:
        ```
        {"data": {a single data record}, "_config": {"primary_key": [<pk>]}}
        ```
        If a list of tuples, it should be:
        ```
        (table_name, {"data": {a single data record}, "_config": {"primary_key": [<pk>]}})
        ```
    cron_schedule: str, optional
        The schedule for the DAG. Default: "0 7 * * *"
    output_folder: str, optional
        The path to save the DAG files to. Default: "_output"
    """
    def __init__(self,
                 project_name:str,
                 datasets,
                 cron_schedule="0 7 * * *",
                 output_folder="_output",
                 database_name="SQUARE_POS",
                 ingest_schema_name="INGEST",
                 cleanse_schema_name="CLEANSE",
                 read_schema_name="READ",
                 stage_name="SQUARE_INGEST",
                 simple_mode=True):
        
        self.OUTPUT_FOLDER       = output_folder
        self.DATASETS_ARG        = datasets
        self.CRON_SCHEUDLE       = cron_schedule
        self.DATABASE_NAME       = database_name
        self.INGEST_SCHEMA_NAME  = ingest_schema_name
        self.CLEANSE_SCHEMA_NAME = cleanse_schema_name
        self.READ_SCHEMA_NAME    = read_schema_name
        self.STAGE_NAME          = stage_name
        self.simple_mode         = simple_mode
        self.PROJECT_NAME        = project_name.upper()
        self.CUSTOM_OP_NAME      = self.to_camel(self.PROJECT_NAME) + "ToSnowflakeOperator"
        self.DAG_ID              = self.PROJECT_NAME + "_INGEST"
        self.JSON_FORMAT         = self.PROJECT_NAME + "_JSON"
        self.CONN_ID             = self.PROJECT_NAME.lower() + "_conn"
        self.DAG_DIR             = os.path.join(self.OUTPUT_FOLDER, self.PROJECT_NAME.lower())
        self.SNOWFLAKE_ROLE      = "ETL_INGEST_READ"

        self.datasets_json       = self.get_datasets_json()

    def to_camel(self, s):
        "Converts string to camel case. ie 'This is test' -> 'ThisIsTest'"
        return "".join([s.capitalize() for s in s.replace(" ", "_").split('_')])

    def parse_timestamp(self, value:str)->str|None:
        """
        Determines if value is a timestamp.
        If yes, returns the datetime_format as a string. 
        If no, returns ``None``
        """
        possible_formats = [
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y/%m/%dT%H:%M:%S.%f",
            "%Y/%m/%dT%H:%M:%S",
            "%Y-%m-%d",
            "%Y/%m/%d",
            "%Y-%m-%d %H:%M:%s.%f%z",
            "%Y-%m-%d %H:%M:%S.%f%z",
            "%Y-%m-%d %H:%M:%S%z"
        ]
        for format in possible_formats:
            # print(f"Trying to turn {value} into format {format}")
            try:
                datetime.strptime(value, format)
                # print("Success")
                return format
            except Exception:
                continue
        return None
    
    def get_datatype(self, key:str, value, composite_col_name:str|None=None)->list[tuple]:
        """
        Returns (key, col_name, datatype, function) as tuple.

        ```get_datatype(key="name", value="Doug")
        returns 
            [("\\"name\\"", "NAME", "VARCHAR", None)]
        
            
        get_datatype(
            key="shipping",
            value={
                "name": "Doug", 
                "address": "123 St", 
                "num_orders": 12,
                "date": "2025-07-01T00:00:00Z"
            }
        )
        returns
            [("\\"shipping\\":\\"name\\"", "SHIPPING_NAME", "VARCHAR", None),
            ("\\"shipping\\":\\"address\\"", "SHIPPING_ADDRESS", "VARCHAR", None),
            ("\\"shipping\\":\\"num_orders\\"", "SHIPPING_NUM_ORDERS", "NUMBER", None),
            ("\\"shipping\\":\\"date\\"", "SHIPPING_DATE", "VARCHAR", "TO_TIMESTAMP(%)")]
        ```
        """
        if composite_col_name:
            col_name = composite_col_name
        else:
            col_name = key.upper()
            key = "\"" + key + "\""

        if key == "last_updated":
            return [(key, col_name, "TIMESTAMP_NTZ", None)]
        elif key == "data_interval_start":
            return [(key, col_name, "TIMESTAMP_NTZ", None)]
        elif key == "data_interval_end":
            return [(key, col_name, "TIMESTAMP_NTZ", None)]


        if isinstance(value, dict): # Handle inner dictionaries recursively
            result = []
            for inner_key, inner_value in value.items():
                result.extend(self.get_datatype(
                    f"{key}:\"{inner_key}\"", #new_key
                    inner_value,
                    composite_col_name = col_name + inner_key.upper()) #new col name
                )
            return result
        
        if isinstance(value, list):
            return [(key, col_name, "ARRAY", None)]
        elif isinstance(value, bool):
            return [(key, col_name, "BOOLEAN", None)]
        elif isinstance(value, float):
            num_decimal_pts = len(str(value).split(".")[-1])+1 # add one for safety
            return [(key, col_name, f"DECIMAL(38,{num_decimal_pts})", None)]
        elif isinstance(value, int):
            return [(key, col_name, "NUMBER", None)]
    
        # check if timestamp
        dt_format = self.parse_timestamp(value)
        if dt_format:
            if dt_format in ("%Y-%m-%d", "%Y/%m/%d"):
                ts_func = "TO_DATE(%)"
            elif dt_format.endswith("Z"): # deal with timezone awareness
                ts_func = "TO_TIMESTAMP_NTZ(%)"
            else:
                ts_func = "TO_TIMESTAMP_NTZ(%)"
            return [(key, col_name, "VARCHAR", ts_func)]

        if isinstance(value, str):
            return [(key, col_name, "VARCHAR", None)]
    
        return [(key, col_name, "VARIANT", None)]
    

    def parse_datapoint_ddl(self, _key:str, _value)->list[str]:
        """
        ```
        parse_datapoint_ddl(_key="name", _value="Doug")
        returns 
            ["NAME VARCHAR"]
        
            
        parse_datapoint_ddl(
            _key="shipping",
            _value={
                "name": "Doug", 
                "address": "123 St", 
                "num_orders": 12,
                "date":"2025-07-01T00:00:00Z"
            }
        )
        returns
            ["SHIPPING_NAME VARCHAR",
             "SHIPPING_ADDRESS VARCHAR",
             "SHIPPING_NUM_ORDERS NUMBER",
             "SHIPPING_ADDRESS TIMSTAMP_NTZ"]
        ```
        """
        result = []
        for _, col_name, datatype, func in self.get_datatype(_key, _value):
            if func is None:
                result.append(col_name + " " + datatype)
                
            elif func.startswith("TO_TIMESTAMP_NTZ"):
                result.append(col_name + " TIMESTAMP_NTZ")
            elif func.startswith("TO_TIMESTAMP_LTZ"):
                result.append(col_name + " TIMESTAMP_LTZ")
            elif func.startswith("TO_TIMESTAMP_TZ"):
                result.append(col_name + " TIMESTAMP_TZ")
            elif func.startswith("TO_TIMESTAMP"):
                result.append(col_name + " TIMESTAMP")
            elif func.startswith("TO_DATE"):
                result.append(col_name + " DATE")
        
        return result

    def process_sql_json_unpack(self, _key:str, _value)->list[str]:
        """
        Generates SQL used to unpack JSON. Used for stage_to_ingest SQL.
        Returns a list. If ``value`` is a dictionary, there will be mmore
        than one in the list. Examples: 
        ```process_sql_json_unpack(_key="name", _value="Doug")
        returns 
            ['$1:"name"::VARCHAR AS NAME']
        
            
        process_sql_json_unpack(
            _key="shipping",
            _value={
                "name": "Doug", 
                "address": "123 St", 
                "date": "2025-07-01T00:00:00Z"
            }
        )
        returns
            ['$1:"shipping":"name"::VARCHAR AS SHIPPING_NAME',
            '$1:"shipping":"address"::VARCHAR AS SHIPPING_ADDRESS',
            'TO_TIMESTAMP_NTZ($1:"shipping":"date"::VARCHAR) AS SHIPPING_ADDRESS']
        """
        arr = []
        for key, col_name, datatype, func in self.get_datatype(_key, _value):
            sql = f"RAW_DATA:{key}::{datatype}"
            if func is not None:
                sql = func.replace('%', sql, 1)
            arr.append(f"{sql} AS {col_name}")

        return arr
    
    def get_col_name_list(self, key:str, value)->list:
        """
        Returns the column names present in the data as a list. If value is
        a dictionary, the list will have more than 1 element. Examples:
        ```
        get_col_name_list(
            key="name", 
            value="john"
        ) 
        returns ["NAME"]

        
        get_col_name(
            key="shipping", 
            value={
                "name": "Doug", 
                "address": "123 St", 
                "date":"2025-07-01T00:00:00Z"
            }
        )
        returns ["SHIPPING_NAME", "SHIPPING_ADDRESS", "SHIPPING_DATE"]
        ```
        """
        return [col_name for _, col_name, _, _ in self.get_datatype(key, value)]

    def get_ingest_to_cleanse_sql(self, d:dict, table_name:str)->str:
        """
        Assumes d = {
            "data": {a single data record in dictionary form},
            "_config": {"primary_key": "id"}
        }
        Returns sql as a a string
        """
        template_file = "utils/templates/ingest_to_cleanse.txt"
        with open(template_file, "r") as f:
            template = f.read()

        data:dict = d["data"]
        _config:dict = d["_config"]

        add_time_bounds = _config.get("add_time_bounds", False)
        pk = _config["primary_key"]

        key_list = data.keys()
        if add_time_bounds:
            key_list = [*key_list, "data_interval_start", "data_interval_end"]
            pk = [*pk, "data_interval_start", "data_interval_end"]
        key_list = [*key_list, "last_updated"]
        upper_pk = [s.upper() for s in pk]
        
        expanded_key_list = [] #unpacked nested json
        for _key in key_list:
            expanded_key_list.extend(self.get_col_name_list(_key, data.get(_key)))

        #### COLS_LIST_SYSDATE ####
        arr = [key.upper() for key in expanded_key_list if key != "LAST_UPDATED"]
        cols_list_sysdate = ",\n\t".join(arr) + ",\n\tSYSDATE() AS LAST_UPDATED"

        #### MERGE_ON ####      ->  "    trg.{COL_NAME} = src.{COL_NAME} AND..."
        merge_on = "\n\tAND ".join([f"trg.{col.upper()} = src.{col.upper()}" for col in pk])

        #### WHEN_MATCHED ####  ->  "    trg.{COL_NAME} = src.{COL_NAME}"
        arr = []
        for key in expanded_key_list:
            if key in upper_pk:
                continue
            arr.append(f"trg.{key.upper()} = src.{key.upper()}")
        # arr = [f"trg.{key.upper()} = src.{key.upper()}" for key in expanded_key_list if key not in upper_pk]
        when_matched = ",\n\t".join(arr)
        
        #### COLS_LIST ####     ->  "    {COL_NAME},"
        cols_list = ",\n\t".join(expanded_key_list)

        #### COLS_LIST_SRC #### ->  "    src.{COL_NAME},"
        cols_list_src = "src." + ",\n\tsrc.".join(expanded_key_list)

        ### UNPACK_JSON
        arr = []
        for key, val in data.items():
            arr.extend(self.process_sql_json_unpack(key, val))
        unpack_json = ",\n\t".join(arr)
        if _config.get("add_time_bounds", False):
            unpack_json += ",\n\tTO_TIMESTAMP_NTZ('{{ data_interval_start }}')" \
                + " AS DATA_INTERVAL_START"
            unpack_json += ",\n\tTO_TIMESTAMP_NTZ('{{ data_interval_end }}')" \
                + " AS DATA_INTERVAL_END"

        ### PK
        # if len(upper_pk) == 1:
        #     pk_str = upper_pk[0]
        # else:
        pk_str = ", ".join(upper_pk)

        # template = template.replace("{{PROJECT_NAME}}", self.PROJECT_NAME)
        template = template.replace("{{DATABASE}}", self.DATABASE_NAME)
        template = template.replace("{{INGEST_SCHEMA}}", self.INGEST_SCHEMA_NAME)
        template = template.replace("{{CLEANSE_SCHEMA}}", self.CLEANSE_SCHEMA_NAME)
        template = template.replace("{{READ_SCHEMA}}", self.READ_SCHEMA_NAME)
        template = template.replace("{{TABLE_NAME}}", table_name)
        template = template.replace("{{COLS_LIST_SYSDATE}}", cols_list_sysdate)
        template = template.replace("{{MERGE_ON}}", merge_on)
        template = template.replace("{{WHEN_MATCHED}}", when_matched)
        template = template.replace("{{COLS_LIST}}", cols_list)
        template = template.replace("{{COLS_LIST_SRC}}", cols_list_src)
        template = template.replace("{{UNPACK_JSON}}", unpack_json)
        template = template.replace("{{PK}}", pk_str)

        return template
    

    def get_stage_to_ingest_sql(self, table_name:str):
        """
        Returns Stage to Ingest SQL for the given table name as a string.
        """
        template_file = "utils/templates/stage_to_ingest.txt"
        with open(template_file, "r") as f:
            template = f.read()

        template = template.replace("{{DATABASE}}", self.DATABASE_NAME)
        template = template.replace("{{INGEST_SCHEMA}}", self.INGEST_SCHEMA_NAME)
        template = template.replace("{{STAGE_NAME}}", self.STAGE_NAME)
        template = template.replace("{{TABLE_NAME}}", table_name)

        return template

    def get_datasets_json(self):
        """
        Returns the datasets being used in this DAG build.
        
        Returns
        -------
        List of tuples
            - name: name of the dataset
            - data: {"data":{<single record of JSON}>, "_config":{"primary_key":<pk>, "add_time_bounds":<bool>}}
        """
        if isinstance(self.DATASETS_ARG, str): # if path
            result = []
            with os.scandir(self.DATASETS_ARG) as entries:
                for entry in entries:
                    file_name = entry.name.replace(".json", "")
                    with open(entry, 'r') as f:
                        d = json.load(f)
                        result.append((file_name, d))

            return result
        
        elif isinstance(self.DATASETS_ARG, list):
            return self.DATASETS_ARG
        
        else:
            raise Exception(
                "Invalid DATASETS argument given. Must be a path or tuple[str, list[dict]]"
            )

    def get_table_list(self)->list[str]:
        return [table_name for table_name, _ in self.datasets_json]
        
    def create_sql_files(self)->None:
        for table_name, data in self.datasets_json:

            stage_to_ingest = self.get_stage_to_ingest_sql(table_name.upper())
            ingest_to_cleanse = self.get_ingest_to_cleanse_sql(data, table_name.upper())

            directory_path = f"{self.DAG_DIR}/sql/{table_name}"
            os.makedirs(directory_path, exist_ok=True)
            
            with open(f"{directory_path}/stage_to_ingest.sql", "w") as f:
                f.write(stage_to_ingest) 
            with open(f"{directory_path}/ingest_to_cleanse.sql", "w") as f:
                f.write(ingest_to_cleanse) 


    def create_custom_operator(self):
        op_directory = f"{self.DAG_DIR}/operators"
        os.makedirs(op_directory, exist_ok=True)


        template_file = "utils/templates/op.txt"
        with open(template_file, "r") as f:
            template = f.read()

        template = template.replace("{{CUSTOM_OP_NAME}}", self.CUSTOM_OP_NAME)
        template = template.replace("{{CONN_ID}}", self.CONN_ID)
        template = template.replace("{{PROJECT_MAME}}", self.PROJECT_NAME)

        self.custom_op_text = template

        with open(f"{op_directory}/{self.CUSTOM_OP_NAME}.py", "w") as f:
            f.write(template)

    def create_dag_file(self):
        template_file = "utils/templates/dag.txt"
        with open(template_file, "r") as f:
            template = f.read()
        
        template = template.replace("{{SCHEDULE}}", self.CRON_SCHEUDLE)
        template = template.replace("{{TABLES}}", str(self.get_table_list()))
        template = template.replace("{{DAG_ID}}", self.DAG_ID)
        template = template.replace("{{CUSTOM_OP_NAME}}", self.CUSTOM_OP_NAME)
        template = template.replace("{{DAG_FILE}}", self.PROJECT_NAME.lower())

        self.dag_text = template

        with open(f"{self.DAG_DIR}/{self.DAG_ID.lower()}.py", "w") as f:
            f.write(template)



    def get_create_table_ddl(self)->str:
        result = []
        for name, d in self.datasets_json:
            ddl_ingest = ""
            ddl_cleanse = ""

            data:dict = d["data"]
            _config:dict = d["_config"]
            add_time_bounds = _config.get("add_time_bounds", False)
            key_list = data.items()

            ddl_ingest += "CREATE TABLE IF NOT EXISTS "
            ddl_ingest += f"{self.INGEST_SCHEMA_NAME}.{name.upper()} (\n\t"
            ddl_cleanse += "CREATE TABLE IF NOT EXISTS "
            ddl_cleanse += f"{self.CLEANSE_SCHEMA_NAME}.{name.upper()} (\n\t"

            arr = []
            for key, value in key_list:
                arr.extend(self.parse_datapoint_ddl(key, value))
            ddl_cleanse += ",\n\t".join(arr) + ",\n"

            ddl_ingest += "RAW_DATA VARIANT,\n"
            ddl_ingest += "\t__FILENAME VARCHAR,\n"
            ddl_ingest += "\t__FILE_LAST_MODIFIED TIMESTAMP_NTZ,\n"
            
            if add_time_bounds:
                ddl_cleanse += "\tDATA_INTERVAL_START TIMESTAMP_NTZ,\n"
                ddl_cleanse += "\tDATA_INTERVAL_END TIMESTAMP_NTZ,\n"

            ddl_ingest += "\tDAG_RUN_KEY VARCHAR\n);\n"
            ddl_cleanse += "\tLAST_UPDATED TIMESTAMP_NTZ\n);\n"
            
            result.append(ddl_ingest + ddl_cleanse)

        return "\n---\n\n".join(result)
    
    def get_simple_output(self)->dict:
        """
        Returns the SQL for this DAG as a dict. Looks like
        ```
        {
            <table_n>: {
                "stage_to_ingest_sql":<SQL>, 
                "ingest_to_cleanse":<SQL>
            },
            ...
            ,
            "ddl": <DDL_SQL_for_all_tables>
        }
        """
        result = {}
        for table_name, data in self.datasets_json:
            table_name = table_name.upper()
            stg_to_ing = self.get_stage_to_ingest_sql(table_name)
            ing_to_clns = self.get_ingest_to_cleanse_sql(data, table_name)
            result[table_name] = {
                "stage_to_ingest.sql":stg_to_ing,
                "ingest_to_cleanse.sql":ing_to_clns
            }
        result["ddl"] = self.get_create_table_ddl()
        return result

    def create_ddl_file(self):
        """
        Opens the DDL template (at ``utils/templates/ddl.txt``), renders using
        the information given, and saves it to the path 
        ``{self.DAG_DIR}/ddl/ddl.sql``
        """

        template_psth = "utils/templates/ddl.txt"
        with open(template_psth, "r") as f:
            template = f.read()

        template = template.replace("{{PROJECT_NAME}}", self.PROJECT_NAME)
        template = template.replace("{{INGEST_SCHEMA}}", self.INGEST_SCHEMA_NAME)
        template = template.replace("{{CLEANSE_SCHEMA}}", self.CLEANSE_SCHEMA_NAME)
        template = template.replace("{{SF_ROLE}}", self.SNOWFLAKE_ROLE)
        template = template.replace("{{JSON_FORMAT}}", self.JSON_FORMAT)
        template = template.replace("{{STAGE_NAME}}", self.STAGE_NAME)
        template = template.replace("{{DATABASE}}", self.DATABASE_NAME)
        template = template.replace("{{CREATE_TABLE_DDL}}", self.get_create_table_ddl())


        ddl_directory = f"{self.DAG_DIR}/ddl"
        os.makedirs(ddl_directory, exist_ok=True)

        with open(f"{ddl_directory}/ddl.sql", "w") as f:
            f.write(template)

    def run_dag_builder(self):
        if self.simple_mode:  
            return self.get_simple_output()

        self.create_sql_files()
        self.create_custom_operator()
        self.create_dag_file()
        self.create_ddl_file()


def main():
    TEST_CONFIG = {
        "PROJECT_NAME": "SQUARE",
        "DATASETS": "utils/datasets/",
        "SIMPLE_MODE": False
    }

    _dag_builder = dag_builder(
        project_name=TEST_CONFIG["PROJECT_NAME"],
        datasets=TEST_CONFIG["DATASETS"],
        simple_mode=TEST_CONFIG["SIMPLE_MODE"]
    )
    _dag_builder.run_dag_builder()
            
if __name__ == "__main__":
    main()