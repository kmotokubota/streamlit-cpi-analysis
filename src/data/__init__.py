"""
Data Module
データ読み込み・処理モジュール
"""

from .loader import (
    get_snowflake_session,
    load_cpi_categories,
    load_cpi_timeseries,
    load_cpi_timeseries_for_contribution
)

from .processor import (
    calculate_yoy_monthly_data,
    calculate_inflation_metrics,
    filter_data_by_criteria,
    prepare_products_for_category_selection
)

__all__ = [
    'get_snowflake_session',
    'load_cpi_categories',
    'load_cpi_timeseries', 
    'load_cpi_timeseries_for_contribution',
    'calculate_yoy_monthly_data',
    'calculate_inflation_metrics',
    'filter_data_by_criteria',
    'prepare_products_for_category_selection'
]
