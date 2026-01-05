import pandas as pd
import joblib
import os
from sqlalchemy import create_engine
from sklearn.ensemble import RandomForestRegressor, VotingRegressor
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from dotenv import load_dotenv

# load environment variables
env_path = os.path.join("..", ".env")
load_dotenv(dotenv_path=env_path)

# Retrieve database connection parameters
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)


# function to read data from features
def load_data():
    """
    Load sales feature data from gold table for training ML models.
    Data is sorted by date to maintain temporal integrity.
    """
    query = """
    SELECT
        distributor,
        channel,
        sub_channel,
        city,
        product_name,
        product_class,
        sales_team,
        year,
        month,
        total_quantity,
        total_sales,
        avg_price,
        rolling_avg_3m_sales,
        sales_growth_pct
    FROM feautures.sales_feature
    ORDER BY
        distributor, channel, sub_channel, city, 
        product_name, product_class, sales_team, 
        year, month
    """
    try:
        df = pd.read_sql(query, con=engine)
        print(f"Data loaded successfully with {len(df)} records.")
        return df
    except Exception as e:
        print(f"Error loading data: {e}")
        raise ValueError("Failed to load data from database.")


# load data
df = load_data()

# Feature engineering for date
df["date"] = pd.to_datetime(df[["year", "month"]].assign(DAY=1))
df = df.sort_values(
    by=[
        "distributor",
        "channel",
        "sub_channel",
        "city",
        "product_name",
        "product_class",
        "sales_team",
        "date",
    ]
)

# Define features and target variable
FEATURES = [
    "total_quantity",
    "total_sales",
    "avg_price",
    "rolling_avg_3m_sales",
    "sales_growth_pct",
]
TARGET = "total_sales"

# Split data into features and target
X = df[FEATURES]
y = df[TARGET]

# split data into train and test sets
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)
print(f"Training data shape: {X_train.shape}, Testing data shape: {X_test.shape}")


# function for build base models
def build_base_models(random_state=42) -> dict:
    """
    Build base machine learning models.
    """
    return {
        "linear_regression": LinearRegression(),
        "random_forest": RandomForestRegressor(
            n_estimators=100, random_state=random_state, n_jobs=-1
        ),
    }


# function for voting regressor building
def build_voting_regressor(
    models: dict, weights: list | None = None
) -> VotingRegressor:
    """
    Build a Voting Regressor ensemble model from base models.
    """
    estimators = [(name, model) for name, model in models.items()]
    voting_regressor = VotingRegressor(estimators=estimators, weights=weights)
    return voting_regressor


# train models
def train_models(model, X_train, y_train):
    """
    Train the provided model on the training data.
    """
    model.fit(X_train, y_train)
    print("Model training completed.")
    return model


# Evalueate models
def evaluate_model(model, X_test, y_test) -> dict:
    """
    Evaluate the model on the test data and return performance metrics.
    """
    from sklearn.metrics import (
        mean_absolute_error,
        mean_squared_error,
        r2_score,
        mean_absolute_percentage_error,
    )

    y_pred = model.predict(X_test)
    return {
        "MAE": mean_absolute_error(y_test, y_pred),
        "MSE": mean_squared_error(y_test, y_pred),
        "R2_Score": r2_score(y_test, y_pred),
        "MAPE": mean_absolute_percentage_error(y_test, y_pred),
    }


# call build, train, and evaluate
if __name__ == "__main__":

    # 1.  Load data
    df = load_data()

    # 2.  Feature engineering
    df["date"] = pd.to_datetime(df[["year", "month"]].assign(DAY=1))
    df = df.sort_values(
        by=[
            "distributor",
            "channel",
            "sub_channel",
            "city",
            "product_name",
            "product_class",
            "sales_team",
            "date",
        ]
    )

    # 3.  Define features and target variable
    FEATURES = [
        "total_quantity",
        "total_sales",
        "avg_price",
        "rolling_avg_3m_sales",
        "sales_growth_pct",
    ]
    TARGET = "total_sales"
    X = df[FEATURES]
    y = df[TARGET]
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    # 4.  Build models
    base_models = build_base_models()
    voting_regressor = build_voting_regressor(base_models, weights=[1, 2])
    trained_model = train_models(voting_regressor, X_train, y_train)

    # 5.  Evaluate model
    metrics = evaluate_model(trained_model, X_test, y_test)
    print("Model Evaluation Metrics:")
    for metric, value in metrics.items():
        print(f"{metric}: {value:.4f}")

    # 6.  Save the trained model
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    models_dir = os.path.join(base_dir, "models")
    os.makedirs(models_dir, exist_ok=True)

    # save individual base models
    for name, model in trained_model.estimators_:
        model_path = os.path.join(models_dir, f"{name}.joblib")
        joblib.dump(model, model_path)
        print(f"Saved {name} model to {model_path}")

    # save voting regressor model
    voting_model_path = os.path.join(models_dir, "voting_regressor.joblib")
    joblib.dump(trained_model, voting_model_path)
    print(f"Saved Voting Regressor model to {voting_model_path}")
    print("All models saved successfully.")
