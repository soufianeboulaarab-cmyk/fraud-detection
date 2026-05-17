import pytest
import pandas as pd
import numpy as np
from src.features.build_features import build_features, FEATURE_COLS


@pytest.fixture
def sample_df():
    return pd.DataFrame({
        'trans_date_trans_time': pd.to_datetime(['2019-06-15 02:30:00', '2019-06-15 14:00:00']),
        'cc_num':     [1234567890, 9876543210],
        'amt':        [250.0, 15.0],
        'lat':        [36.07, 48.88],
        'long':       [-81.17, -118.21],
        'merch_lat':  [37.50, 49.15],
        'merch_long': [-82.00, -118.18],
        'city_pop':   [3495, 149],
        'category':   ['shopping_net', 'grocery_pos'],
        'merchant':   ['fraud_Rippin', 'fraud_Heller'],
        'dob':        pd.to_datetime(['1988-03-09', '1978-06-21']),
        'gender':     ['F', 'F'],
        'is_fraud':   [1, 0],
    })


def test_build_features_returns_dataframe(sample_df):
    result = build_features(sample_df)
    assert isinstance(result, pd.DataFrame)


def test_all_feature_cols_present(sample_df):
    result = build_features(sample_df)
    for col in FEATURE_COLS:
        assert col in result.columns, f'Colonne manquante : {col}'


def test_is_night_correct(sample_df):
    result = build_features(sample_df)
    assert result.iloc[0]['is_night'] == 1   # 02h30 → nuit
    assert result.iloc[1]['is_night'] == 0   # 14h00 → jour


def test_distance_positive(sample_df):
    result = build_features(sample_df)
    assert (result['distance'] >= 0).all()


def test_amt_log_no_nan(sample_df):
    result = build_features(sample_df)
    assert result['amt_log'].notna().all()


def test_high_risk_category(sample_df):
    result = build_features(sample_df)
    assert result.iloc[0]['high_risk_category'] == 1   # shopping_net → risque
    assert result.iloc[1]['high_risk_category'] == 1   # grocery_pos  → risque
