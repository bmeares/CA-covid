#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Fetch county-level COVID-19 data from the state of California.
"""

from __future__ import annotations
from meerschaum.utils.typing import Optional, Dict, Any
from meerschaum.config._paths import PLUGINS_TEMP_RESOURCES_PATH

CSV_URL = "https://data.chhs.ca.gov/dataset/f333528b-4d38-4814-bebb-12db1f10f535/resource/046cdd2b-31e5-4d34-9ed3-b48cdbc4be7a/download/covid19cases_test.csv"
TMP_PATH = PLUGINS_TEMP_RESOURCES_PATH / 'CA-covid_data'
CSV_PATH  = TMP_PATH / 'covid19cases_test.csv'
required = ['requests', 'python-dateutil', 'pandas']

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


def fetch(pipe: meerschaum.Pipe, **kw):
    from meerschaum.utils.misc import wget
    TMP_PATH.mkdir(exist_ok=True, parents=True)
    wget(CSV_URL, CSV_PATH)
    dtypes = {
        'date': 'datetime64[ms]',
        'area': str,
        'cases': float,
    }
    df = (
        pd.read_csv(CSV_PATH).dropna(subset=['date']).dropna(subset=['cumulative_cases']).dropna(subset=['cumulative_deaths'])
    )
    pass
