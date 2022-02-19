from dagster import op, In, Out, Output, String
from sklearn.preprocessing import LabelEncoder
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.linear_model import SGDClassifier
import numpy as np
import pandas as pd
import scipy
import sklearn


@op(
    description="Fetch branded-to-nonbranded features training data",
    out={
        "df": Out(
            description="pandas dataframe containing branded features to non-branded features training or test data",
            dagster_type=pd.DataFrame,
        )
    },
    config_schema={
        "full_path": String
    }
)
def fetch_branded_nonbranded_training_or_test_data(context) -> pd.DataFrame:
    df = pd.read_csv(context.op_config["full_path"])

    context.log.info(f"Number of (rows, columns): {df.shape}")

    yield Output(df, output_name="df")


@op(
    description="Separate features from the target label.",
    ins={
        "df": In(
            description="pandas dataframe",
        )
    },
    out={
        "X": Out(
            description="Feature matrix",
            dagster_type=pd.Series,
        ),
        "y": Out(
            description="Target label",
            dagster_type=pd.Series,
        ),
    }
)
def separate_features_from_target_label(context, df: pd.DataFrame):
    """
    It is assumed the 1st column of the dataframe is the text features column
    and the 2nd column is the target label
    """

    X = df.iloc[:, 0]
    y = df.iloc[:, 1]

    context.log.info(f"Type of X is: {type(X)}")
    context.log.info(f"Dimensions of X: {X.shape}")
    context.log.info(f"Type of y is: {type(y)}")
    context.log.info(f"Dimensions of y: {y.shape}")

    yield Output(X, output_name="X")
    yield Output(y, output_name="y")


@op(
    description="Label encode the categorical target label",
    ins={
        "y": In(
            description="Target label",
        )
    },
    out={
        "y_encoded": Out(
            description="Target label that has been label encoded",
            dagster_type=np.ndarray
        ),
        "label_encoder": Out(
            description="Label encoder",
            dagster_type=sklearn.preprocessing._label.LabelEncoder,
        ),
    }
)
def label_encode_target(context, y: pd.Series):
    le = LabelEncoder()
    le.fit(y)
    y_encoded = le.fit_transform(y)

    context.log.info(f"Dimenions of encoded y: {y_encoded.shape}")

    yield Output(y_encoded, output_name="y_encoded")
    yield Output(le, output_name="label_encoder")


@op(
    description="Encode the features of training data: CountVectorize then TfIdTransform",
    ins={
        "X": In(
            description="pandas series containing unencoded features/text document data",
        )
    },
    out={
        "X_encoded_train": Out(
            description="Features data that has been both count vectorized and tfid transformed",
            dagster_type=scipy.sparse.csr.csr_matrix
        ),
        "count_vect": Out(
            description="Fitted count vector model",
            dagster_type=sklearn.feature_extraction.text.CountVectorizer
        ),
        "tfid_vect": Out(
            description="Fitted tfid model",
            dagster_type=sklearn.feature_extraction.text.TfidfTransformer
        ),
    },
)
def count_tfid_transform_train(context, X: pd.Series):
    count_vect = CountVectorizer()
    X_count = count_vect.fit_transform(X)

    tfid_vect = TfidfTransformer()
    X_tfid = tfid_vect.fit_transform(X_count)

    context.log.info(f"Dimenions of encoded X: {X_tfid.shape}")

    yield Output(X_tfid, output_name="X_encoded_train")
    yield Output(count_vect, output_name="count_vect")
    yield Output(tfid_vect, output_name="tfid_vect")


@op(
    description="Encode the features of test data: CountVectorize and then TfIdTransform",
    ins={
        "df": In(
            description="Pandas dataframe of test data",
        ),
        "count_vect": In(
            description="Count vector model already fitted on test data",
        ),
        "tfid_vect": In(
            description="tfidt vector model already fitted on test data",
        ),
    },
    out={
        "X_encoded_test": Out(
            description="Encoded features of test data",
            dagster_type=scipy.sparse.csr.csr_matrix
        )
    }
)
def count_tfid_transform_test(
        context,
        df: pd.DataFrame,
        count_vect: sklearn.feature_extraction.text.CountVectorizer,
        tfid_vect: sklearn.feature_extraction.text.TfidfTransformer
):
    """
    It is assumed the 1st column of the file is the text features data
    """

    X_test = df.iloc[:, 0]
    X_test_count_vect = count_vect.transform(X_test)
    X_test_tfid = tfid_vect.transform(X_test_count_vect)

    context.log.info(f"Dimensions of encoded X test: {X_test_tfid.shape}")

    yield Output(X_test_tfid, output_name="X_encoded_test")


@op(
    description="Create SGD Classifier model",
    ins={
        "X_encoded": In(
            description="Feature matrix encoded with count vectorizer and tfid transform",
        ),
        "y_encoded": In(
            description="Feature matrix encoded with count vectorizer and tfid transform",
        ),
    },
    out={
        "clf": Out(
            description="Trained SGD classifier model",
            dagster_type=sklearn.linear_model._stochastic_gradient.SGDClassifier,
        )
    }
)
def create_sgd_classifier_model(
        context,
        X_encoded: scipy.sparse.csr.csr_matrix,
        y_encoded: np.ndarray
):
    clf = SGDClassifier().fit(X_encoded, y_encoded)

    yield Output(clf, output_name="clf")


@op(
    description="Make prediction",
    ins={
        "X_test_encoded": In(
            description="Encoded features matrix",
        ),
        "clf": In(
            description="Classification model already fitted on training data",
        ),
        "label_encoder": In(
            description="Label encoder fitted on the training data",
        ),
    },
    out={
        "prediction": Out(
            description="Predicted values",
            dagster_type=np.ndarray
        )
    }
)
def predict(
        context,
        X_test_encoded: scipy.sparse.csr.csr_matrix,
        clf,
        label_encoder: sklearn.preprocessing._label.LabelEncoder
):
    predicted = clf.predict(X_test_encoded)

    context.log.info(f"Dimenions of encoded X test: {X_test_encoded.shape}")
    context.log.info(f"Sample prediction values: {label_encoder.classes_[predicted[:5]]}")

    yield Output(predicted, output_name="prediction")


@op(
    description="Measure accuracy score",
    ins={
        "predicted_values": In(
            description="predicted values from model",
        ),
        "actual_values": In(
            description="Actual values",
        )
    }
)
def get_accuracy_score(context, predicted_values: np.ndarray, actual_values: np.ndarray):
    pass
