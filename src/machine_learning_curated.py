import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

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

# Script generated for node Step Trainer Trusted to Curated 
StepTrainerTrustedtoCurated_node1740043168795 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrustedtoCurated_node1740043168795")

# Script generated for node Accelerometer Trusted to Curated
AccelerometerTrustedtoCurated_node1740043098839 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrustedtoCurated_node1740043098839")

# Script generated for node Join
Join_node1740043054169 = Join.apply(frame1=AccelerometerTrustedtoCurated_node1740043098839, frame2=StepTrainerTrustedtoCurated_node1740043168795, keys1=["timestamp"], keys2=["sensorreadingtime"], transformation_ctx="Join_node1740043054169")

# Script generated for node Machine Learning Curated
EvaluateDataQuality().process_rows(frame=Join_node1740043054169, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1740042904520", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
MachineLearningCurated_node1740044422015 = glueContext.getSink(path="s3://mp-d609/step_trainier/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MachineLearningCurated_node1740044422015")
MachineLearningCurated_node1740044422015.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
MachineLearningCurated_node1740044422015.setFormat("json")
MachineLearningCurated_node1740044422015.writeFrame(Join_node1740043054169)
job.commit()