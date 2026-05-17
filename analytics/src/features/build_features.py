import pandas as pd
import numpy as np


def load_data(train_path: str, test_path: str) -> pd.DataFrame:
    train = pd.read_csv(train_path)
    test  = pd.read_csv(test_path)
    df    = pd.concat([train, test], ignore_index=True)
    df['trans_date_trans_time'] = pd.to_datetime(df['trans_date_trans_time'])
    df['dob'] = pd.to_datetime(df['dob'])
    return df


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Temporelles
    df['hour']        = df['trans_date_trans_time'].dt.hour
    df['day_of_week'] = df['trans_date_trans_time'].dt.dayofweek
    df['month']       = df['trans_date_trans_time'].dt.month
    df['is_night']    = ((df['hour'] >= 22) | (df['hour'] <= 6)).astype(int)

    # Géographique
    df['distance'] = np.sqrt(
        (df['lat'] - df['merch_lat'])**2 +
        (df['long'] - df['merch_long'])**2
    ) * 111

    # Démographique
    df['age'] = (df['trans_date_trans_time'] - df['dob']).dt.days // 365

    # Transformations log
    df['amt_log']      = np.log1p(df['amt'])
    df['distance_log'] = np.log1p(df['distance'])
    df['city_pop_log'] = np.log1p(df['city_pop'])

    # Encodage cyclique heure
    df['hour_sin'] = np.sin(2 * np.pi * df['hour'] / 24)
    df['hour_cos'] = np.cos(2 * np.pi * df['hour'] / 24)

    # Encodage cyclique jour
    df['dow_sin'] = np.sin(2 * np.pi * df['day_of_week'] / 7)
    df['dow_cos'] = np.cos(2 * np.pi * df['day_of_week'] / 7)

    # Catégorie risque
    high_risk = ['shopping_net', 'misc_net', 'grocery_pos', 'shopping_pos']
    df['high_risk_category'] = df['category'].isin(high_risk).astype(int)

    # Target encoding catégorie (taux de fraude par catégorie)
    cat_fraud_rate = df.groupby('category')['is_fraud'].mean()
    df['category_fraud_rate'] = df['category'].map(cat_fraud_rate)

    return df


FEATURE_COLS = [
    'amt_log', 'distance_log', 'city_pop_log',
    'hour_sin', 'hour_cos', 'dow_sin', 'dow_cos',
    'is_night', 'high_risk_category', 'category_fraud_rate', 'age'
]
