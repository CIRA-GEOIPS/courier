"""Dispatcher plugin which spawns processes via a multiprocessing pool."""

# import multiprocessing as mp
# from os.path import basename
# import time
# from types import SimpleNamespace

# import sqlite3 as sql

interface = "dispatchers"
name = "pool"
family = "multiprocessing"

# def query_db(db_path, source_names):
#     db_name = basename(db_path)[:-3]
#     conn = sql.connect(db_path)
#     cursor = conn.cursor()
#     files = {}
#     for sname in source_names:
#         cursor.execute(
#             f"""
#             SELECT fpath, stime, sat, sensor FROM {db_name}
#             WHERE sensor = '{sname}'
#             """
#         )
#         files_by_sensor = cursor.fetchall()
#         for entry in files_by_sensor:
#             fpath, stime, sat, sensor = entry
#             if stime not in list(files.keys()):
#                 files[stime] = {}
#             if f"{sat}_{sensor}" not in list(files[stime].keys()):
#                 files[stime][f"{sat}_{sensor}"] = []
#             files[stime][f"{sat}_{sensor}"].append(fpath)
#             # source name for satellite is sat sensor
#             # model based would just be the name of the model (hrrr)
#             # ARM_site_name...
#     return files


# def call(db_path, stop_event, kwargs):
#     kwargs = SimpleNamespace(kwargs)

#     while not stop_event.is_set():
#         # Check for ready products

#         relevant_files = query_db(db_path, kwargs.source_names)

#         for start_time in relevant_files:
#             for sat_sensor in relevant_files[start_time]:


#         for product in products:
#             product_id, product_name, required_files = product
#             required_files = required_files.split(",")

#             # Check if all required files are in the database
#             cursor.execute(
#                 """
#                 SELECT COUNT(*) FROM files WHERE filename IN ({})
#                 """.format(
#                     ",".join("?" * len(required_files))
#                 ),
#                 required_files,
#             )
#             count = cursor.fetchone()[0]

#             if count == len(required_files):
#                 # Update product status to 'processing'
#                 cursor.execute(
#                     """
#                     UPDATE required_products SET status = 'processing'
#                     WHERE id = ?
#                     """,
#                     (product_id,),
#                 )
#                 conn.commit()

#                 # Spawn the process to handle output generation
#                 process = mp.Process(target=generate_output, args=(product_name,))
#                 process.start()

#         time.sleep(1)  # Avoid excessive polling

#     conn.close()


# def generate_output(product_name):
#     print(f"Generating output for {product_name}...")
#     time.sleep(2)  # Simulate processing
#     print(f"Output generated for {product_name}")


def call(**kwargs):
    pass
