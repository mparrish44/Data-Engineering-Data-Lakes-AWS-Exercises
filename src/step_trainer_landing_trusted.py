import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Step Trainer Landing to Trusted
StepTrainerLandingtoTrusted_node1740021013223 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="StepTrainerLandingtoTrusted_node1740021013223")

# Script generated for node Step Trainer Trusted
SqlQuery6080 = '''
select * from myDataSource
'''
StepTrainerTrusted_node1740021085568 = sparkSqlQuery(glueContext, query = SqlQuery6080, mapping = {"myDataSource":StepTrainerLandingtoTrusted_node1740021013223}, transformation_ctx = "StepTrainerTrusted_node1740021085568")

# Script generated for node Step Trainer Landing to Trusted
EvaluateDataQuality().process_rows(frame=StepTrainerTrusted_node1740021085568, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1740049730866", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
StepTrainerLandingtoTrusted_node1740050672465 = glueContext.getSink(path="s3://mp-d609/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerLandingtoTrusted_node1740050672465")
StepTrainerLandingtoTrusted_node1740050672465.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
StepTrainerLandingtoTrusted_node1740050672465.setFormat("json")
StepTrainerLandingtoTrusted_node1740050672465.writeFrame(StepTrainerTrusted_node1740021085568)
job.commit()