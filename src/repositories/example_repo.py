from dagster import job, repository
from ops.sklearn_ops import (
    fetch_freehand_text_to_generic_data,
    separate_features_from_target_label,
    label_encode_target,
    count_tfid_transform_train,
    count_tfid_transform_test,
    create_sgd_classifier_model,
    predict
)


@ job(
    description="Scikit-Learn multi-class text classification: classify free-hand text computer skills descriptions to generic descriptions"
)
def text_classify():
    X_train, y_train = separate_features_from_target_label.alias("separate_features_from_target_train")(
        fetch_freehand_text_to_generic_data.alias("fetch_training_data")()
    )

    df_test = fetch_freehand_text_to_generic_data.alias("fetch_test_data")()

    y_encoded_train, label_encoder_train = label_encode_target.alias("label_encode_train")(y_train)
    X_encoded_train, count_vect, tfid_vect = count_tfid_transform_train.alias("count_tfid_transform_train")(X_train)

    clf = create_sgd_classifier_model(X_encoded_train, y_encoded_train)

    X_encoded_test = count_tfid_transform_test(df_test, count_vect, tfid_vect)

    predict(X_encoded_test, clf, label_encoder_train)


@repository
def examples_repo():
    return [
        text_classify,
    ]
