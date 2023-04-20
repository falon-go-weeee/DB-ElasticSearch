from datetime import datetime


def get_date_time():
    time_stamp = datetime.now()
    dt_string = time_stamp.strftime("%d/%m/%Y %H:%M:%S")
    # print("date and time =", dt_string)
    return dt_string