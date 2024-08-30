from datetime import datetime
from typing import Dict, Optional, Union

import numpy as np
import pandas as pd
import yfinance as yf
from loguru import logger
from peewee import DoesNotExist

from praice.data_handling.db_ops import crud
from praice.data_handling.models import Symbol

pd.set_option("future.no_silent_downcasting", True)


def process_financial_data(
    financials: pd.DataFrame,
    balance_sheet: pd.DataFrame,
    cashflow: pd.DataFrame,
    income_statement: pd.DataFrame,
) -> Dict[str, Dict[str, Optional[float]]]:
    """
    Process financial data by concatenating the given dataframes and converting them into a dictionary.

    Args:
        financials (pd.DataFrame): The financials dataframe.
        balance_sheet (pd.DataFrame): The balance sheet dataframe.
        cashflow (pd.DataFrame): The cashflow dataframe.
        income_statement (pd.DataFrame): The income statement dataframe.

    Returns:
        Dict[str, Dict[str, Optional[float]]]:
            A dictionary containing the processed financial data,
            where the keys are dates and the values are dictionaries
            with column names as keys and corresponding values as optional floats.
    """
    cols = sorted(
        set(financials.columns)
        .intersection(balance_sheet.columns)
        .intersection(cashflow.columns)
        .intersection(income_statement.columns),
        reverse=True,
    )
    _financials = financials[cols].copy()
    _balance_sheet = balance_sheet[cols].copy()
    _cashflow = cashflow[cols].copy()
    _income_statement = income_statement[cols].copy()

    # concat all dataframes
    data = pd.concat(
        [_financials, _balance_sheet, _cashflow, _income_statement], axis=0
    )
    data.index = data.index.str.lower().str.replace(" ", "_")
    data.columns = data.columns.astype(str)
    data = data.fillna(value=np.nan)
    data_dict = data.to_dict(orient="dict")
    for date, values in data_dict.items():
        for k, v in values.items():
            if np.isnan(v):
                data_dict[date][k] = None

    return data_dict


def collect_fundamental_data(symbol: Union[Symbol, str]) -> dict:
    """
    Collects fundamental data for a given symbol.

    Args:
        symbol (Union[Symbol, str]): The symbol for which to collect fundamental data.

    Returns:
        dict: A dictionary containing the collected fundamental data,
            including the symbol, annual data, and quarterly data.
    """
    try:
        symbol_obj = crud._ensure_symbol(symbol)
    except DoesNotExist:
        logger.error(f"Symbol {symbol} not found in database")
        return

    logger.info(f"Collecting fundamental data for {symbol_obj.symbol}")

    ticker = yf.Ticker(symbol_obj.symbol)

    try:
        annual_financials = ticker.financials
        quarterly_financials = ticker.quarterly_financials
        annual_balance_sheet = ticker.balance_sheet
        quarterly_balance_sheet = ticker.quarterly_balance_sheet
        annual_cashflow = ticker.cashflow
        quarterly_cashflow = ticker.quarterly_cashflow
        annual_income_statement = ticker.income_stmt
        quarterly_income_statement = ticker.quarterly_income_stmt

        annual_data = process_financial_data(
            annual_financials,
            annual_balance_sheet,
            annual_cashflow,
            annual_income_statement,
        )
        quarterly_data = process_financial_data(
            quarterly_financials,
            quarterly_balance_sheet,
            quarterly_cashflow,
            quarterly_income_statement,
        )

        return {
            "symbol": symbol_obj.symbol,
            "annual": annual_data,
            "quarterly": quarterly_data,
        }

    except Exception as e:
        logger.error(f"Error collecting fundamental data for {symbol}: {str(e)}")
        return {"symbol": symbol_obj.symbol, "annual": {}, "quarterly": {}}


def store_fundamental_data(fundamental_data: dict) -> None:
    """
    Store fundamental data in the database.

    Args:
        fundamental_data (dict): A dictionary containing the fundamental data.
            The dictionary should have the following structure:
            {
                "symbol": str,
                "annual": {
                    "YYYY-MM-DD": {
                        "key1": value1,
                        "key2": value2,
                        ...
                    },
                    ...
                },
                "quarterly": {
                    "YYYY-MM-DD": {
                        "key1": value1,
                        "key2": value2,
                        ...
                    },
                    ...
                }
            }

    Returns:
        None
    """

    symbol = fundamental_data["symbol"]
    try:
        symbol_obj = crud._ensure_symbol(symbol)
    except DoesNotExist:
        logger.error(f"Symbol {symbol} not found in database")
        return

    data_to_upsert = []
    for period, data in fundamental_data.items():
        if period in ["annual", "quarterly"]:
            for date_str, values in data.items():
                date = datetime.strptime(date_str, "%Y-%m-%d").date()
                data_to_upsert.append({"date": date, "period": period, "data": values})

    upserted_count = crud.bulk_upsert_fundamental_data(symbol_obj, data_to_upsert)
    logger.info(f"Upserted {upserted_count} fundamental data records for {symbol}")


def collect_and_store_fundamental_data(symbol: Union[Symbol, str]) -> None:
    """
    Collects fundamental data for a given symbol and stores it.

    Parameters:
        symbol (Union[Symbol, str]): The symbol for which to collect fundamental data.

    Returns:
        None
    """
    fundamental_data = collect_fundamental_data(symbol)
    if fundamental_data:
        store_fundamental_data(fundamental_data)


def collect_and_store_fundamental_data_for_all_symbols() -> None:
    """
    Collects and stores fundamental data for all symbols
        that have the `collect_fundamental_data` flag set to True in `symbol_configs`.

    Returns:
        None
    """
    configs = crud.list_symbol_configs()
    for config in configs:
        if config.collect_fundamental_data:
            collect_and_store_fundamental_data(config.symbol.symbol)
