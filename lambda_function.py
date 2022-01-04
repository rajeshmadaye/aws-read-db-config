#************************************************************************
## Lambda Function  : read_db_config
## Description      : Lambda Function to read config
## Author           :
## Copyright        : Copyright 2019
## Version          : 1.0.0
## Mmaintainer      :
## Email            :
## Status           : In Review
##************************************************************************
##
##************************************************************************
## Version Info:
## 1.0.0 : 18-Dec-2021 : Created first version to read config data
##************************************************************************
##
##************************************************************************
##  Input/Output of Lambda Function:
##  Input:
##    event = {'job_id' : 1}
##
##  Output:
##    Invoke another lambda to load data into redshift
##************************************************************************
import sys, os, json, traceback, csv
import boto3, pymysql, time
from botocore.exceptions import ClientError
from datetime import datetime, timedelta, date
# from load_db_data import LoadDBData

##************************************************************************
## Global Default Parameters :: User should modify default values.
## Or create environment variables to set those values.
boto3.setup_default_session(profile_name='raj')
##************************************************************************
DEF_RDS_HOST   = 'aurora-new-instance-1.a8bcdefeghim.us-east-1.rds.amazonaws.com'
DEF_RDS_PORT   = 3306

##************************************************************************
## Default local parameters
##************************************************************************
DEF_CTL_USER    = 'user-name'
DEF_CTL_PASS    = 'password'
DEF_CTL_DB      = 'dev'
##***********************************************************************
## Class Definition
##***********************************************************************
class ReadDBConfig:
  #************************************************************************
  # Class constructor
  #************************************************************************
  def __init__(self, event):
    self.s3 =  boto3.client("s3")
    # Set argument variables
    if(event and 'job_id' in event):
      self.job_id = event['job_id']
      self.init_db()
    else:
      print("Error: Job ID is mandatory argument to proceed with this function.")

    return

  #************************************************************************
  # Initiate DB connection
  #************************************************************************
  def init_db(self):
    rc = True
    # Define variables
    self.rds_host       = self.get_env_value('RDS_HOST', DEF_RDS_HOST)
    self.rds_port       = self.get_env_value('RDS_PORT', DEF_RDS_PORT)
    self.ctl_user       = DEF_CTL_USER
    self.ctl_pass       = DEF_CTL_PASS
    self.ctl_db         = DEF_CTL_DB

    # Define methods
    self.ctl_table_map      = self.get_ctl_table_map()
    self.ctl_db_conn        = self.get_ctl_db_connection()
    self.ctl_master_context = self.get_ctl_master_context(self.job_id)
    return rc

  #************************************************************************
  # Setup environment parameters if exists
  #************************************************************************
  def get_env_value(self, key, default):
    value = os.environ[key] if key in os.environ else default
    if(type(value) == 'str'):
      value = value.strip()
    return value

  #************************************************************************
  # Function to run read-metadata logic
  #  - Go to job master and connect to source and test connectivity
  #  - Read the schema list and then table list to get object information
  #  - Iterate and pull the schema information for all tables
  #  - Load schema information into dl_object_schema_def table
  #  - Invoke lambda "load_db_data" to store data into redshift
  #************************************************************************
  def run(self):
    rc = False
    if self.job_id:
      if self.is_ctl_db_connected():
        self.src_db_conn    = self.get_src_db_connection()
        if self.src_db_conn:
          self.src_table_list = self.get_src_table_list()
          self.src_metadata   = self.get_src_metadata()
          rc = self.store_src_object_schema()
          self.ctl_object_schema_def   = self.get_ctl_object_schema_def()
        else:
          print("Error: Unable to connect to source DB")
      else:
        print("Error:: Control DB connection not found.")
    else:
      print("Error: Job ID not found. Stopped processing request")
    return rc

  #************************************************************************
  # Set Control database connection in object
  #************************************************************************
  def get_ctl_table_map(self):
    table_map = {}
    table_map['job_master']     = '{}.dl_job_master'.format(self.ctl_db)
    table_map['schema_list']    = '{}.dl_schema_list'.format(self.ctl_db)
    table_map['table_list']     = '{}.dl_src_table_list'.format(self.ctl_db)
    table_map['object_schema']  = '{}.dl_object_schema_def'.format(self.ctl_db)
    table_map['object_sts_log'] = '{}.dl_object_sts_log'.format(self.ctl_db)
    table_map['cntx_s3']        = '{}.Cntx_s3'.format(self.ctl_db)

    return table_map

  def get_ctl_db_connection(self):
    conn = self.get_db_connection(self.rds_host
                      ,self.rds_port
                      ,self.ctl_user
                      ,self.ctl_pass
                      ,self.ctl_db
                      )
    return conn

  def is_ctl_db_connected(self):
    rc = False
    if self.ctl_db_conn:
      rc = True
    return rc

  def get_ctl_master_context(self, job_id):
    result = {}
    try:
      query = '''
      SELECT job_id, job_name, source_system_name, source_type
            ,source_system_type, database_name, db_override_name_ind, db_override_name
            ,ingest_ind, src_context_table_name, target_context_table_name, last_updt
      FROM {}
      WHERE job_id = {}
      '''.format(self.ctl_table_map['job_master'], job_id)

      cur = self.ctl_db_conn.cursor()
      cur.execute(query)
      for row in cur:
        result['job_id'] = row[0]
        result['job_name'] = row[1]
        result['source_system_name'] = row[2]
        result['source_type'] = row[3]
        result['source_system_type'] = row[4]
        result['database_name'] = row[5]
        result['db_override_name_ind'] = row[6]
        result['db_override_name'] = row[7]
        result['ingest_ind'] = row[8]
        result['src_context_table_name'] = row[9]
        result['target_context_table_name'] = row[10]
        result['last_updt'] = row[11]
      cur.close()
    except Exception as identifier:
      print("get_ctl_master_context Error:", identifier)
    return result

  #************************************************************************
  # Check if can connect to DB
  #************************************************************************
  def get_db_connection(self, host, port, user, password, dbname):
    conn = None
    if(host and port and user and password and dbname):
      try:
        conn = pymysql.connect(host=host, port=int(port), user=user, passwd=password, db=dbname, connect_timeout=5)
      except Exception as identifier:
        print("Unable to connect to database, Error: ", identifier)
    else:
      print("Error :: DB Connection parameters are missing.")
      print("host: {}, port: {}, user: {}, password: {}, dbname: {}", host, port, user, password, dbname)
    return conn

  #************************************************************************
  # 1. Get source schema detail from control table
  # 2. Check connectivity with source table.
  #************************************************************************
  def get_src_db_connection(self):
    conn = None
    conn_detail = self.get_source_conn_detail()
    if(len(conn_detail)):
      conn = self.get_db_connection(
                      conn_detail['host']
                      ,conn_detail['port']
                      ,conn_detail['username']
                      ,conn_detail['password']
                      ,conn_detail['database']
                      )
    return conn

  def get_source_conn_detail(self):
    conn_detail = {}
    schema_name = self.ctl_master_context['src_context_table_name'].strip()
    if schema_name:
      try:
        query = '''SELECT * FROM {}.{}'''.format(self.ctl_db, schema_name)
        cur = self.ctl_db_conn.cursor()
        cur.execute(query)
        for row in cur:
          conn_detail[row[0]] = row[1]
        cur.close()
      except Exception as identifier:
        print("Error:", identifier)
    else:
      print("Error:: Unable to find source schema name for job_id : {}".format(self.job_id))

    return conn_detail

  #************************************************************************
  # 1. Get schema list
  # 2. Get table list
  #************************************************************************
  def get_src_table_list(self):
    all_table_list = []
    schema_list = self.get_schema_list()
    for schema in schema_list:
      schema_name, table_pull_method = schema[0], schema[1]
      table_list = self.get_table_names(schema_name)
      all_table_list.extend(table_list)
    return all_table_list

  def get_schema_list(self):
    schema_list = []
    try:
      query = '''select schema_name, table_pull_method FROM {}'''.format(self.ctl_table_map['schema_list'])
      cur = self.ctl_db_conn.cursor()
      cur.execute(query)
      for row in cur:
        schema_list.append(list(row))
      cur.close()
    except Exception as identifier:
      print("Error:", identifier)
    return schema_list

  def get_table_names(self, schema_name):
    table_list = []
    schema_name = schema_name.lower()
    try:
      query = '''
      select job_id, job_name, schema_name, src_table_name, target_table_name
      from {}
      where job_id = {} and schema_name = '{}'
      and active_flag = 'Y'
      order by schema_id, table_id
      '''.format(self.ctl_table_map['table_list'], self.job_id, schema_name)

      cur = self.ctl_db_conn.cursor()
      cur.execute(query)
      for row in cur:
        data = {
          'job_id'            : row[0],
          'job_name'          : row[1],
          'schema_name'       : row[2].lower(),
          'src_table_name'    : row[3],
          'target_table_name' : row[4]
        }
        table_list.append(data)
      cur.close()
    except Exception as identifier:
      print("Error:", identifier)
    return table_list

  #************************************************************************
  # 1. Get table metadata from source database
  #************************************************************************
  def get_src_metadata(self):
    all_table_matadata = []
    for table in self.src_table_list:
      table_matadata = self.get_src_table_metadata(table)
      all_table_matadata.extend(table_matadata)
    return all_table_matadata

  def get_src_table_metadata(self, table):
    table_matadata = []
    try:
      query = '''
        SELECT distinct T.TABLE_SCHEMA,
        T.TABLE_NAME,
        C.COLUMN_NAME,
        C.DATA_TYPE,
        C.COLUMN_KEY as CONSTRAINT_NAME,
        C.CHARACTER_MAXIMUM_LENGTH as DATA_LENGTH,
        C.NUMERIC_PRECISION as DATA_PRECISION,
        C.NUMERIC_SCALE as DATA_SCALE,
        C.ORDINAL_POSITION as POSITION
        FROM INFORMATION_SCHEMA.TABLES T
        INNER JOIN
        INFORMATION_SCHEMA.COLUMNS C
        ON C.TABLE_SCHEMA = T.TABLE_SCHEMA AND C.TABLE_NAME = T.TABLE_NAME
        where T.TABLE_SCHEMA = '{}'
        and T.TABLE_NAME = '{}'
        ORDER BY C.ORDINAL_POSITION
      '''.format(table['schema_name'], table['src_table_name'])

      cur = self.src_db_conn.cursor()
      cur.execute(query)
      for row in cur:
        data = {
            'table_schema'    : row[0],
            'table_name'      : row[1],
            'column_name'     : row[2],
            'data_type'       : row[3],
            'constraint_name' : row[4],
            'data_length'     : row[5],
            'data_precision'  : row[6],
            'data_scale'      : row[7],
            'position'        : row[8],
            'job_id'          : table['job_id'],
            'job_name'        : table['job_name'],
            'src_table_name'     : table['src_table_name'],
            'target_table_name'  : table['target_table_name']
          }
        table_matadata.append(data)
      cur.close()
    except Exception as identifier:
      print("Error:", identifier)
    return table_matadata

  #************************************************************************
  # 1. Store Object Information into dl_object_schema_def table
  #************************************************************************
  def store_src_object_schema(self):
    rc = False
    success_count = 0
    failure_count = 0
    for metadata in self.src_metadata:
      query = self.get_store_schema_query(metadata)
      # print("IQ:", query)
      rc = self.exec_query(query)
      if rc:
        success_count = success_count + 1
      else:
        failure_count = failure_count + 1
    print('''Total [{}] insert records attempted with [{}] successfull and [{}] failed.'''.format(success_count+failure_count, success_count, failure_count))
    return rc

  def exec_query(self, query):
    rc = False
    try:
      cur = self.ctl_db_conn.cursor()
      cur.execute(query)
      self.ctl_db_conn.commit()
      cur.close()
      rc = True
    except Exception as identifier:
      print("Error:", identifier)
    return rc

  def get_store_schema_query(self, metadata):
    ts = time.time()
    epoch = datetime.fromtimestamp(ts).strftime('%s')
    timestamp = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    query = '''
    INSERT IGNORE INTO %s
    (job_id, process_id, job_name, database_name
    ,schema_name, source_object_name, target_object_name, field_name
    ,target_field_name, field_datatype, target_field_datatype, field_ordinal_position
    ,field_size, field_precision, field_scale, field_primary_key_ind
    ,ingst_ts, run_ts)
    VALUES (
             %s, '%s', '%s', '%s'
            ,'%s', '%s', '%s', '%s'
            ,'%s', '%s', '%s', %s
            , %s, %s, %s, '%s'
            ,'%s', '%s'
            )
    ''' % (self.get_null_val(self.ctl_table_map['object_schema'])
          ,self.get_null_val(self.job_id)
          ,epoch
          ,self.get_null_val(self.ctl_master_context['job_name'])
          ,self.get_null_val(self.ctl_master_context['database_name'])
          ,self.get_null_val(metadata['table_schema'])
          ,self.get_null_val(metadata['src_table_name'])
          ,self.get_null_val(metadata['target_table_name'])
          ,self.get_null_val(metadata['column_name'])
          ,self.get_null_val(metadata['column_name'])
          ,self.get_null_val(metadata['data_type'])
          ,self.get_null_val(metadata['data_type'])
          ,self.get_null_val(metadata['position'])
          ,self.get_null_val(metadata['data_length'])
          ,self.get_null_val(metadata['data_precision'])
          ,self.get_null_val(metadata['data_scale'])
          ,self.get_null_val(metadata['constraint_name'])
          ,timestamp
          ,timestamp
          )
    return query

  def get_null_val(self,value):
    if value or value == 0:
      return value
    else:
      return 'NULL'

  #************************************************************************
  # 1. Get object schema definition
  #************************************************************************
  def get_ctl_object_schema_def(self):
    rc = False
    eventList = []
    eventCallLimit = 5    # Next lambda will be invoked for 'N' objects
    index = 0
    schema_list = self.get_all_object_schema()
    for schema in schema_list:
      objects = self.get_all_object_names(schema)
      for object in objects:
        fields = self.get_all_object_fields(schema, object)
        event = { 'schema' : schema, 'object' : object, 'fields' : fields }
        eventList.append(event)
        if(index >= eventCallLimit or index == len(schema_list)):
          self.run_event_list(eventList)     ##Scope to invoke another lambda - TODO
          eventList.clear()
        index = index + 1
    return rc

  def get_all_object_schema(self):
    data = []
    try:
      query = '''
        select distinct schema_name from {}
      '''.format(self.ctl_table_map['object_schema'])

      cur = self.src_db_conn.cursor()
      cur.execute(query)
      for row in cur:
        data.append(row[0])
      cur.close()
    except Exception as identifier:
      print("Error:", identifier)
    return data

  def get_all_object_names(self, schema):
    data = []
    try:
      query = '''
        select distinct source_object_name from {}
        where schema_name = '{}'
        order by source_object_name asc
      '''.format(self.ctl_table_map['object_schema'], schema)

      cur = self.src_db_conn.cursor()
      cur.execute(query)
      for row in cur:
        data.append(row[0])
      cur.close()
    except Exception as identifier:
      print("Error:", identifier)
    return data

  def get_all_object_fields(self, schema, object):
    data = []
    try:
      query = '''
        select * from {}
        where schema_name = '{}'
        and source_object_name = '{}'
        order by field_ordinal_position asc
      '''.format(self.ctl_table_map['object_schema'], schema, object)

      cur = self.src_db_conn.cursor(pymysql.cursors.DictCursor)
      cur.execute(query)
      for row in cur:
        data.append(row)
      cur.close()
    except Exception as identifier:
      print("Error:", identifier)
    return data


#************************************************************************
# Run event list
#************************************************************************
  def run_event_list(self, eventList):
    rc = False
    object_data = {}
    inst_success_count = 0
    inst_failure_count = 0
    updt_success_count = 0
    updt_failure_count = 0
    for event in eventList:
      object_data = self.process_object(event)
      ## Insert records
      rc = self.insert_object_sts_log(object_data)
      if rc:
        inst_success_count = inst_success_count + 1
      else:
        inst_failure_count = inst_failure_count + 1

      ## Add data to redshift DB

      ## Update record into control table as complete.
      object_data['status_code'] = 'D'
      rc = self.update_object_sts_log(object_data)
      if rc:
        updt_success_count = updt_success_count + 1
      else:
        updt_failure_count = updt_failure_count + 1


    print('''Total [{}] insert records attempted with [{}] successfull and [{}] failed in [{}] table.
    '''.format(inst_success_count+inst_failure_count, inst_success_count, inst_failure_count, self.ctl_table_map['object_sts_log']))
    print('''Total [{}] records update completed with [{}] successfull and [{}] failed in [{}] table.
    '''.format(updt_success_count+updt_failure_count, updt_success_count, updt_failure_count, self.ctl_table_map['object_sts_log']))
    return rc

  def process_object(self, object):
    rc = False
    select_fields = None
    select_from   = None
    job_id        = None
    object_data   = {}

    if 'fields' in object:
      for field in object['fields']:
        # print("MYF:", field)
        if len(object_data) == 0:
          object_data = field
        if select_fields:
          select_fields = '{}, {}'.format(select_fields, field['field_name'])
        else:
          select_fields = field['field_name']
        if not select_from:
          select_from = '{}.{}'.format(field['schema_name'], field['source_object_name'])
        if not job_id:
          job_id = field['job_id']
          self.ctl_master_context = self.get_ctl_master_context(job_id)

      query = 'SELECT {} FROM {}'.format(select_fields, select_from)
      csv_file = self.prepare_csv(query, select_fields, select_from)
      object_data = self.upload_csv(object_data, csv_file)
    return object_data

  def upload_csv(self, object_data, csv_file):
    ts = time.time()
    yyyymmdd = datetime.fromtimestamp(ts).strftime('%Y%m%d')
    yyyymmdd_hhmmss = datetime.fromtimestamp(ts).strftime('%Y%m%d_%H%M%S')
    schema_name = object_data['schema_name']
    object_name = object_data['source_object_name']
    bucket_name = self.get_s3_bucket_name()
    bucket_key = 'cntx_s3/{}/{}/{}/{}_{}.csv'.format(schema_name, object_name, yyyymmdd, object_name, yyyymmdd_hhmmss)
    rc = self.s3_put(csv_file, bucket_name, bucket_key)
    object_data['s3_file_path'] = '{}/{}'.format(bucket_name, bucket_key)
    object_data['status_code'] = False
    # print("OBJ :", object_data)
    if os.path.exists(csv_file):
      os.remove(csv_file)
    print("File uploaded to S3 Bucket: ", object_data['s3_file_path'])
    return object_data

  def get_s3_bucket_name(self):
    bucket_name = None #'s3-sync-samplebucket-input'  ##TODO :: Bucket name should be taken from DB.
    data = {}
    try:
      query = '''
        SELECT * from {}
      '''.format(self.ctl_table_map['cntx_s3'])
      cur = self.src_db_conn.cursor(pymysql.cursors.DictCursor)
      cur.execute(query)
      for row in cur:
        data[row['key']] = row['value']
      cur.close()
      if 'bucketname' in data:
        bucket_name = data['bucketname']
    except Exception as identifier:
      print("Error:", identifier)

    if not bucket_name:
      print("Error:: Unable to find bucket name, processing will be aborted.")

    return bucket_name

  def prepare_csv(self, query, headers, table):
    ts = time.time()
    epoch = datetime.fromtimestamp(ts).strftime('%s')
    csv_file = '/tmp/{}.{}.csv'.format(epoch, table)
    self.fetch_src_data(query, csv_file, headers)
    # print("CSV File created :", csv_file)
    return csv_file

  def fetch_src_data(self, query, csv_file, headers):
    data = []
    if self.is_ctl_db_connected():
      self.src_db_conn    = self.get_src_db_connection()
      data = self.get_src_table_data(query, self.src_db_conn, csv_file, headers)
    return data

  def get_src_table_data(self, query, dbconn, csv_file, headers):
    data = []
    with open(csv_file, mode='w') as csv_fh:
      csv_writer = csv.writer(csv_fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
      csv_writer.writerow(headers.split(","))
      try:
        cur = dbconn.cursor()
        cur.execute(query)
        for row in cur:
          csv_writer.writerow(row)
        cur.close()
      except Exception as identifier:
        print("Error:", identifier)
    return data

#************************************************************************
# insert data in dl_object_sts_log
#************************************************************************
  def insert_object_sts_log(self, object_data):
    rc = False
    query = self.get_insert_object_sts_log_query(object_data)
    # print("IQ:", query)
    rc = self.exec_query(query)
    return rc

  def get_insert_object_sts_log_query(self, metadata):
    ts = time.time()
    epoch = datetime.fromtimestamp(ts).strftime('%s')
    timestamp = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

    query = '''
    INSERT IGNORE INTO %s
    (job_id, process_id, job_name, database_name
    ,schema_name, source_object_name, target_object_name, s3_file_path
    ,target_database_name, ingst_ts, run_ts)
    VALUES (
             %s, '%s', '%s', '%s'
            ,'%s', '%s', '%s', '%s'
            ,'%s', '%s', '%s'
            )
    ''' % (self.ctl_table_map['object_sts_log']
          ,self.get_null_val(metadata['job_id'])
          ,epoch
          ,self.get_null_val(metadata['job_name'])
          ,self.get_null_val(metadata['database_name'])
          ,self.get_null_val(metadata['schema_name'])
          ,self.get_null_val(metadata['source_object_name'])
          ,self.get_null_val(metadata['target_object_name'])
          ,self.get_null_val(metadata['s3_file_path'])
          ,self.get_null_val(metadata['database_name'])
          ,timestamp
          ,timestamp
          )
    return query

#************************************************************************
# Update data in dl_object_sts_log
#************************************************************************
  def update_object_sts_log(self, object_data):
    rc = False
    query = self.get_update_object_sts_log_query(object_data)
    # print("IQ:", query)
    rc = self.exec_query(query)
    return rc

  def get_update_object_sts_log_query(self, metadata):
    ts = time.time()
    epoch = datetime.fromtimestamp(ts).strftime('%s')
    timestamp = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

    query = '''
    UPDATE %s
    SET status_code = '%s'
    WHERE job_id = %s and source_object_name = '%s' and target_object_name = '%s'
    ''' % (self.ctl_table_map['object_sts_log']
          ,self.get_null_val(metadata['status_code'])
          ,self.get_null_val(metadata['job_id'])
          ,self.get_null_val(metadata['source_object_name'])
          ,self.get_null_val(metadata['target_object_name'])
          )
    return query

#************************************************************************
# Upload CSV to S3 bucket
#************************************************************************
  def s3_put(self, file, bucket_name, key):
      rc = False
      try:
          self.s3.upload_file(file, bucket_name, key)
          rc = True
      except ClientError:
          print(f"Error in uplodaing file to S3 bucket")
          pass
      return rc

  def s3_exists(self, bucket_name, key):
      try:
          return key, self.s3.head_object(Bucket=bucket_name, Key=key)
      except ClientError:
          return key, None


#************************************************************************
# main lambda handler
#************************************************************************
def lambda_handler(event, context):
  rc = False
  print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
  print("INFO :: Lambda function executon initiated")
  try:
    RDC = ReadDBConfig(event)
    rc = RDC.run()
    print("Lambda Execution Status :", rc)
  except Exception as inst:
    print("Error:: Unable to process request:", inst)
    traceback.print_exc()

  print("INFO :: Lambda function executon completed")
  print("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")

  return rc

if __name__ == "__main__":
    event = {'job_id' : 1}
    context = ''
    lambda_handler(event,context)
