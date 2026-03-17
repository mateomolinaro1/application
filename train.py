"""
Prediction de la survie d'un individu sur le Titanic
"""

import os
import argparse
import logging
from dotenv import load_dotenv

from sklearn.preprocessing import OneHotEncoder
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import MinMaxScaler
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import confusion_matrix
import skops.io as sio

import duckdb

from src.validation.check import (
    check_name_formatting,
    check_missing_values,
    check_data_leakage,
)


load_dotenv()
con = duckdb.connect(database=":memory:")

logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
    level=logging.DEBUG,
    handlers=[logging.FileHandler("recording.log"), logging.StreamHandler()],
)


# PARAMETERS ---------------------------------------


N_TREES = 20
MAX_DEPTH = None
MAX_FEATURES = "sqrt"
NUMERIC_FEATURES = ["Age", "Fare"]
CATEGORICAL_FEATURES = ["Embarked", "Sex"]
URL_RAW = "https://minio.lab.sspcloud.fr/lgaliana/ensae-reproductibilite/data/raw/data.parquet"

jeton_api = os.environ["JETON_API"]


# ENVIRONMENT CONFIGURATION ---------------------------

<<<<<<< HEAD
logger.add("recording.log", rotation="500 MB")
load_dotenv()

parser = argparse.ArgumentParser(description="Paramètres du random forest")
parser.add_argument(
    "--n_trees", type=int, default=20, help="Nombre d'arbres"
)
args = parser.parse_args()

URL_RAW = "https://minio.lab.sspcloud.fr/lgaliana/ensae-reproductibilite/data/raw/data.csv"

n_trees = args.n_trees
jeton_api = os.environ.get("JETON_API", "")
data_path = os.environ.get("data_path", URL_RAW)
data_train_path = os.environ.get("train_path", "data/derived/train.parquet")
data_test_path = os.environ.get("test_path", "data/derived/test.parquet")
MAX_DEPTH = None
MAX_FEATURES = "sqrt"

if jeton_api.startswith("$"):
    logger.info("API token has been configured properly")
else:
    logger.warning("API token has not been configured")


# IMPORT ET STRUCTURATION DONNEES --------------------------------

p = pathlib.Path("data/derived/")
p.mkdir(parents=True, exist_ok=True)

TrainingData = pd.read_csv(data_path)

y = TrainingData["Survived"]
X = TrainingData.drop("Survived", axis="columns")

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.1
)
pd.concat([X_train, y_train], axis = 1).to_parquet(data_train_path)
pd.concat([X_test, y_test], axis = 1).to_parquet(data_test_path)


# PIPELINE ----------------------------


# Create the pipeline
pipe = create_pipeline(
    n_trees, max_depth=MAX_DEPTH, max_features=MAX_FEATURES
)


# ESTIMATION ET EVALUATION ----------------------

pipe.fit(X_train, y_train)

with open("model.joblib", "wb") as f:
    dump(pipe, f)

# Evaluate the model
score, matrix = evaluate_model(pipe, X_test, y_test)

logger.success(f"{score:.1%} de bonnes réponses sur les données de test pour validation")
logger.debug(20 * "-")
logger.info("Matrice de confusion")
logger.debug(matrix)
=======
parser = argparse.ArgumentParser(description="Paramètres du random forest")
parser.add_argument("--n_trees", type=int, default=20, help="Nombre d'arbres")
args = parser.parse_args()

n_trees = args.n_trees

logging.debug(f"Valeur de l'argument n_trees: {n_trees}")


# QUALITY DIAGNOSTICS  ---------------------------------------

logging.debug(f"\n{80 * '-'}\nStarting data validation step\n{80 * '-'}")

query_definition = (
    f"CREATE TEMP TABLE titanic AS (SELECT * FROM read_parquet('{URL_RAW}'))"
)
con.sql(query_definition)

column_names = con.sql("SELECT column_name FROM (DESCRIBE titanic)").to_df()[
    "column_name"
]  # DuckDB ici, sinon titanic.columns serait OK

check_name_formatting(connection=con)

for var in column_names:
    check_missing_values(connection=con, variable=var)


# FEATURE ENGINEERING    -----------------------------------------

logging.debug(f"\n{80 * '-'}\nStarting feature engineering phase\n{80 * '-'}")

titanic = con.sql(
    f"SELECT Survived, {', '.join(CATEGORICAL_FEATURES + NUMERIC_FEATURES)} FROM titanic"
).to_df()

y = titanic["Survived"]
X = titanic.drop("Survived", axis="columns")

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.1)

for string_var in CATEGORICAL_FEATURES:
    check_data_leakage(X_train, X_test, string_var)


# MODEL DEFINITION -----------------------------------------

numeric_transformer = Pipeline(
    steps=[
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler", MinMaxScaler()),
    ]
)

categorical_transformer = Pipeline(
    steps=[
        ("imputer", SimpleImputer(strategy="most_frequent")),
        ("onehot", OneHotEncoder()),
    ]
)


preprocessor = ColumnTransformer(
    transformers=[
        ("Preprocessing numerical", numeric_transformer, NUMERIC_FEATURES),
        (
            "Preprocessing categorical",
            categorical_transformer,
            CATEGORICAL_FEATURES,
        ),
    ]
)

pipe = Pipeline(
    [
        ("preprocessor", preprocessor),
        (
            "classifier",
            RandomForestClassifier(
                n_estimators=N_TREES, max_depth=MAX_DEPTH, max_features=MAX_FEATURES
            ),
        ),
    ]
)

# TRAINING AND EVALUATION --------------------------------------------

logging.debug(f"\n{80 * '-'}\nStarting model fitting phase\n{80 * '-'}")

pipe.fit(X_train, y_train)
obj = sio.dump(pipe, "model.skops")

rdmf_score = pipe.score(X_test, y_test)
rdmf_score_tr = pipe.score(X_train, y_train)

logging.info(
    f"{rdmf_score:.1%} de bonnes réponses sur les données de test pour validation"
)

logging.info("Matrice de confusion:")
logging.info(confusion_matrix(y_test, pipe.predict(X_test)))

logging.debug(f"\n{80 * '-'}\nFILE ENDED SUCCESSFULLY!\n{80 * '-'}")
>>>>>>> 728eee7cd355554b28be87544f20a13adeab08ba
