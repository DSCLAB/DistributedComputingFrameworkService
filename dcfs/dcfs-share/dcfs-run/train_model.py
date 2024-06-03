import mlflow
import mlflow.spark
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import LogisticRegression
import logging

# logging.basicConfig(level=logging.DEBUG)

import h2o
from h2o.automl import H2OAutoML

h2o.init()

def train_h2o_automl(features, target, max_runtime_secs=600):
    """
    Train an H2O AutoML model.
    
    Args:
        features (pd.DataFrame): A DataFrame containing the feature columns.
        target (pd.Series): A Series containing the target column.
        max_runtime_secs (int): The maximum time in seconds to run the AutoML training.

    Returns:
        H2OAutoML: The trained H2O AutoML model.
    """
    # Convert the Pandas DataFrame to an H2O Frame
    h2o_frame = h2o.H2OFrame(pd.concat([features, target], axis=1))

    # Split the data into training and validation sets
    train, valid = h2o_frame.split_frame(ratios=[0.8], seed=42)

    # Get the target column name
    target_column = target.name

    # Initialize the H2O AutoML model
    automl = H2OAutoML(max_runtime_secs=max_runtime_secs, seed=42, project_name="automl_metadata_recommendation")

    # Train the model
    automl.train(y=target_column, training_frame=train, validation_frame=valid)

    return automl
