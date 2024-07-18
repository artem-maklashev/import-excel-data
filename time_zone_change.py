from datetime import timedelta, datetime
import pytz
import config


def main():
    local_tz=pytz.timezone(config.timezone)
    date = "14.07.2024"
    # Преобразование строки даты в datetime объект
    date_time = datetime.strptime(date, "%d.%m.%Y") + timedelta(hours=8)
    print("Local Time without TZ:", date_time)

    # Привязка локальной временной зоны к datetime объекту
    local_date_time = local_tz.localize(date_time)
    print("Local Time with TZ:", local_date_time)

    # Преобразование локального времени в UTC
    utc_date_time = local_date_time.astimezone(pytz.utc)
    print("UTC Time:", utc_date_time.strftime("%Y-%m-%d %H:%M:%S %Z%z"))

if __name__ == '__main__':
    main()