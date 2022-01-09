# AWS - Lambda functon to interact with RDS Aurora database
Python based lambda function created to interact with AWS Aurora RDS. This is simple practice project created using boto3 libraries to interact with Aurora RDS and to fetch requested job detail and then load data into S3 bucker as CSV.

## Features
- Read job master database connection detail for requested job-id.
- Go to job master and connect to source and test connectivity
- Read the schema list and then table list to get object information
- Iterate and pull the schema information for all tables
- Load schema information into dl_object_schema_def table
- Store data as CSV into S3 bucket. In current version is synchronous call, this can be made as another lambda and call async. 

## Tech
Below are list of technologies used.
- [Python] - Lambda function is written in lambda
- [boto3] - Python boto3 SDK used to interact with AWS services.
- [pymysql] - Python library to interact with databases.

Below are list of AWS services used in this project.
- [Lambda] - Lambda function created using Python.
- [RDS] - AWS Aurora RDS service used to store and fetch the data.
- [S3] - AWS S3 service used to store CSV data.

## Installation

Follow below steps to deploy this function to your environment. Ideal way to deploy lambda function is using SAM or serverless framework. Please follow my other projects for further assistance. Below I have mentioned way to create simple zip package and upload as a lambda.

Create local package with dependencies.
```sh
cd read_db_config
rm -rf package
mkdir package
pip install --target ./package pymysql
```

Create Lambda Package
```sh
cd package
zip -r9 ${OLDPWD}/${FUNCTION_NAME}.zip .
cd ${OLDPWD}
zip -g ${FUNCTION_NAME}.zip lambda_function.py
```

Deploy package to AWS environment.
```sh
aws lambda create-function --function-name ${FUNCTION_NAME} --zip-file fileb://${FUNCTION_NAME}.zip --profile $PROFILE --role $ROLE --runtime "python3.6" --handler $HANDLER --timeout 120 --region ${REGION_NAME}
```

## License
MIT
