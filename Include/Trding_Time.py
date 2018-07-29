import arrow as ar
import pandas as pd
from tushare import trade_cal


def is_trading_time():
    all_days = trade_cal()
    all_days = all_days[all_days['isOpen'] == 1]
    today = ar.now()
    all_days['calendarDate'] = pd.to_datetime(all_days['calendarDate'], utc=today.tzinfo)

    if today > today.replace(hour=15).floor('hour'):
        valid_days = all_days[all_days['calendarDate'] > today.floor('day').datetime]
    else:
        valid_days = all_days[all_days['calendarDate'] >= today.floor('day').datetime]
    trading_day = ar.get(valid_days.iloc[0, 0])

    if trading_day <= today:  # Today is trading day
        if today < today.replace(hour=9, minute=30).floor('minute'):
            return today.replace(hour=9, minute=30).floor('minute').float_timestamp - today.float_timestamp
        elif today.replace(hour=13).floor('hour') > today > today.replace(hour=11, minute=30).floor('minute'):
            return today.replace(hour=13).floor('hour').float_timestamp - today.float_timestamp
    else:  # Today is not trading day
        return trading_day.replace(hour=9, minute=30).floor('minute').float_timestamp - today.float_timestamp
    return 0


if __name__ == '__main__':
    print(is_trading_time())
