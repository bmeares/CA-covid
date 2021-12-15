#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Fetch county-level COVID-19 data from the state of California.
"""

from __future__ import annotations
from meerschaum.utils.typing import Optional, Dict, Any
from meerschaum.config._paths import PLUGINS_TEMP_RESOURCES_PATH
__version__ = '0.1.0'
import datetime
import pathlib

CSV_URL = "https://data.chhs.ca.gov/dataset/f333528b-4d38-4814-bebb-12db1f10f535/resource/046cdd2b-31e5-4d34-9ed3-b48cdbc4be7a/download/covid19cases_test.csv"
TMP_PATH = PLUGINS_TEMP_RESOURCES_PATH / 'CA-covid_data'
COUNTIES_PATH = pathlib.Path(__file__).parent / 'counties.csv'
CSV_PATH  = TMP_PATH / 'covid19cases_test.csv'
required = ['requests', 'python-dateutil', 'pandas', 'duckdb']

def register(pipe: meerschaum.Pipe, **kw):
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.prompt import prompt, yes_no
    while True:
        fips_str = prompt("Please enter a list of FIPS codes separated by commas:")
        fips = fips_str.replace(' ', '').split(',')

        valid = True
        for f in fips:
            if not f.startswith("06"):
                warn("All FIPS codes must begin with 06 (prefix for the state of California).", stack=False)
                valid = False
                break
        if not valid:
            continue

        question = "Is this correct?"
        for f in fips:
            question += f"\n  - {f}"
        question += '\n'

        if not fips or not yes_no(question):
            continue
        break

    return {
        'columns': {
            'datetime': 'date',
            'id': 'fips',
            'value': 'cases'
        },
        'CA-covid': {
            'fips': fips,
        },
    }


def fetch(
        pipe: meerschaum.Pipe,
        begin: Optional[datetime.datetime] = None,
        end: Optional[datetime.datetime] = None,
        debug: bool = False,
        **kw
    ):
    from meerschaum.utils.misc import wget
    import pandas as pd
    import duckdb
    import textwrap
    TMP_PATH.mkdir(exist_ok=True, parents=True)
    wget(CSV_URL, CSV_PATH)
    dtypes = {
        'date': 'datetime64[ms]',
        'county': str,
        'fips': str,
        'cases': int,
        'deaths': int,
    }
    fips = pipe.parameters['CA-covid']['fips']
    fips_where = "'" + "', '".join(fips) + "'"
    counties_df = pd.read_csv(COUNTIES_PATH, dtype={'fips': str, 'counties': str, 'state': str})

    query = textwrap.dedent(f"""
        SELECT
            CAST(d.date AS DATE) AS date, 
            c.fips,
            c.county,
            d.cumulative_cases AS cases,
            d.cumulative_deaths AS deaths
        FROM read_csv_auto('{str(CSV_PATH)}') AS d
        INNER JOIN counties_df AS c ON c.county = d.area
        WHERE c.fips IN ({fips_where})
            AND d.cumulative_deaths IS NOT NULL
            AND d.cumulative_cases IS NOT NULL
            AND d.date IS NOT NULL
            AND c.fips IS NOT NULL"""
    )
    begin = begin if begin is not None else pipe.get_sync_time(debug=debug)
    if begin is not None:
        begin -= datetime.timedelta(days=2)
        query += f"\n    AND CAST(d.date AS DATE) >= CAST('{begin}' AS DATE)"
    if end is not None:
        query += f"\n    AND CAST(d.date AS DATE) <= CAST('{end}' AS DATE)"

    result = duckdb.query(query)
    df = result.df()[dtypes.keys()].astype(dtypes)
    return df
