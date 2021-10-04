from datetime import datetime
import pandas as pd
import numpy as np

DECIMAL = 6
INT_DIGITS = 6
nothing = [None, np.nan, 'None', 'n/a', 'none', 'NONE']


def check_result(result, to_df):
    if to_df:
        if result:
            result = pd.DataFrame.from_records(result)
        else:
            result = pd.DataFrame()
    else:
        if not result:
            result = False
    return result


def round_value(value, decimal=DECIMAL, int_digit=INT_DIGITS):
    value = round(value, decimal) if value not in nothing else None
    if value:
        if value <= 1 / 10 ** decimal:
            value = 0
        if value >= 10 ** int_digit:
            value = 10 ** int_digit - 1
    return value


def check_value(tpe=None, row=None, key=None, trunc=None):
    try:
        if key in row.keys():
            res = row[key]
            if res:
                if res == '{}':
                    return None
                if tpe == 'Timestamp':
                    return datetime.fromtimestamp(int(float(res)))
                if tpe == 'Datetime':
                    return datetime.strptime(res.split(' ')[0], "%Y-%m-%d")
                if tpe == 'Isotime':
                    return datetime.strptime(res.split(' ')[0], "%d/%m/%Y")
                if type(res) is dict:
                    return None
                if tpe == float:
                    return float(round_value(res))
                if tpe == int:
                    return int(float(res))
                if trunc:
                    return res[0:trunc]
                return res
        return None
    except Exception as e:
        return None
