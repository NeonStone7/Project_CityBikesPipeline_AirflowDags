# emr.py
from typing import Any

SPARK_CONFIG = [
    {
        "Classification": "iceberg-defaults",
        "Properties": {
            "iceberg.enabled": "true"
        }
    },
    {
    "Classification": "spark-defaults",
    "Properties": {
        "spark.sql.catalog.spark_catalog": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.catalog.spark_catalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
        "spark.sql.catalog.spark_catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        "spark.hadoop.hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
        "spark.hadoop.proxyuser.hive.hosts": "*",
        "spark.hadoop.proxyuser.hive.groups": "*",
        "spark.hadoop.fs.s3a.endpoint.region": "eu-west-1"
    }
}

    ]

CLUSTER_CONFIG: dict[str, Any] = {
    "Name": "Spark Iceberg Cluster",
    "ReleaseLabel": "emr-7.5.0",
    "LogUri": "s3://citybikes-raw-data/logs/",
    "Applications": [{"Name": "Spark"}],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Primary node",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
        "Ec2SubnetId": "subnet-047ce6dde68bc8f56",
    },
    "Configurations": SPARK_CONFIG,
    "ServiceRole": "EMR_DefaultRole",
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "VisibleToAllUsers": True,
}