import pymysql
from config import *
import logging
from datetime import datetime

# 配置日志
logging.basicConfig(filename='.\log\log.txt', level=logging.INFO,
                    format='%(asctime)s [%(levelname)s]: %(message)s')

def alarm_characteristic_statistics(start_time,end_time):
    """ 网络告警特征统计,并存入数据库
    :param start_time: 起始时间
    :param end_time: 结束时间
    :return: 源IP、目的IP、告警类型的TOP5
    """
    estimate_time = datetime.now()
    try:
        # 连接数据库
        connection = pymysql.connect(host=dataaddress, port=int(port_num), user=user_name, passwd=password, db=datause)
        cursor = connection.cursor()
    except BaseException as E:
        logging.error(f"Error occurred: {E}", exc_info=True)
        raise Exception

    sql = "SELECT source_ip, COUNT(*) as count FROM ems_information_network_alarm WHERE alarm_time BETWEEN '{}' AND '{}' GROUP BY source_ip ORDER BY count DESC LIMIT 5".format(start_time,end_time)
    cursor = connection.cursor()
    cursor.execute(sql)
    top5_source_ip = cursor.fetchall()
    sql = "SELECT dest_ip, COUNT(*) as count FROM ems_information_network_alarm WHERE alarm_time BETWEEN '{}' AND '{}' GROUP BY dest_ip ORDER BY count DESC LIMIT 5".format(start_time,end_time)
    cursor = connection.cursor()
    cursor.execute(sql)
    top5_dest_ip = cursor.fetchall()
    sql = "SELECT alarm_type, COUNT(*) as count FROM ems_information_network_alarm WHERE alarm_time BETWEEN '{}' AND '{}' GROUP BY alarm_type ORDER BY count DESC LIMIT 5".format(start_time,end_time)
    cursor = connection.cursor()
    cursor.execute(sql)
    top5_alarm_type = cursor.fetchall()

    # 查询时间区间内总共多少行告警
    sql = f"SELECT COUNT(*) FROM ems_information_network_alarm WHERE alarm_time BETWEEN '{start_time}' AND '{end_time}'"
    cursor.execute(sql)
    result = cursor.fetchone()
    total_rows = result[0]

    # Print or process the result as needed
    if result:
        # TOP5源IP及百分比
        source_ip1 = top5_source_ip[0][0]
        source_ip2 = top5_source_ip[1][0]
        source_ip3 = top5_source_ip[2][0]
        source_ip4 = top5_source_ip[3][0]
        source_ip5 = top5_source_ip[4][0]
        source_ip1_percentage = round(100*(top5_source_ip[0][1]/total_rows),2)
        source_ip2_percentage = round(100*(top5_source_ip[1][1]/total_rows),2)
        source_ip3_percentage = round(100*(top5_source_ip[2][1]/total_rows),2)
        source_ip4_percentage = round(100*(top5_source_ip[3][1]/total_rows),2)
        source_ip5_percentage = round(100*(top5_source_ip[4][1]/total_rows),2)

        # TOP5目的IP及百分比
        dest_ip1 = top5_dest_ip[0][0]
        dest_ip2 = top5_dest_ip[1][0]
        dest_ip3 = top5_dest_ip[2][0]
        dest_ip4 = top5_dest_ip[3][0]
        dest_ip5 = top5_dest_ip[4][0]
        dest_ip1_percentage = round(100*(top5_dest_ip[0][1]/total_rows),2)
        dest_ip2_percentage = round(100*(top5_dest_ip[1][1]/total_rows),2)
        dest_ip3_percentage = round(100*(top5_dest_ip[2][1]/total_rows),2)
        dest_ip4_percentage = round(100*(top5_dest_ip[3][1]/total_rows),2)
        dest_ip5_percentage = round(100*(top5_dest_ip[4][1]/total_rows),2)

        # TOP5源IP及百分比
        alarm_type1 = top5_alarm_type[0][0]
        alarm_type2 = top5_alarm_type[1][0]
        alarm_type3 = top5_alarm_type[2][0]
        alarm_type4 = top5_alarm_type[3][0]
        alarm_type5 = top5_alarm_type[4][0]
        alarm_type1_percentage = round(100*(top5_alarm_type[0][1]/total_rows),2)
        alarm_type2_percentage = round(100*(top5_alarm_type[1][1]/total_rows),2)
        alarm_type3_percentage = round(100*(top5_alarm_type[2][1]/total_rows),2)
        alarm_type4_percentage = round(100*(top5_alarm_type[3][1]/total_rows),2)
        alarm_type5_percentage = round(100*(top5_alarm_type[4][1]/total_rows),2)

        # 插入结果到数据库表中
        insert_sql = "INSERT INTO ems_alarm_characteristic_statistics (estimate_time, start_time, end_time, source_ip1, source_ip1_percentage, source_ip2, source_ip2_percentage,source_ip3, source_ip3_percentage,source_ip4, source_ip4_percentage,source_ip5, source_ip5_percentage,dest_ip1, dest_ip1_percentage, dest_ip2, dest_ip2_percentage,dest_ip3, dest_ip3_percentage,dest_ip4, dest_ip4_percentage,dest_ip5, dest_ip5_percentage,alarm_type1, alarm_type1_percentage, alarm_type2, alarm_type2_percentage,alarm_type3, alarm_type3_percentage,alarm_type4, alarm_type4_percentage,alarm_type5, alarm_type5_percentage) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s)"

        # 使用 execute 函数执行 SQL 语句
        cursor.execute(insert_sql, (
            estimate_time,start_time, end_time,
            source_ip1, source_ip1_percentage,
            source_ip2, source_ip2_percentage,
            source_ip3, source_ip3_percentage,
            source_ip4, source_ip4_percentage,
            source_ip5, source_ip5_percentage,
            dest_ip1, dest_ip1_percentage,
            dest_ip2, dest_ip2_percentage,
            dest_ip3, dest_ip3_percentage,
            dest_ip4, dest_ip4_percentage,
            dest_ip5, dest_ip5_percentage,
            alarm_type1, alarm_type1_percentage,
            alarm_type2, alarm_type2_percentage,
            alarm_type3, alarm_type3_percentage,
            alarm_type4, alarm_type4_percentage,
            alarm_type5, alarm_type5_percentage
        ))

        # 提交更改
        connection.commit()
    else:
        logging.info(f"No data found for the specified time range.")

if __name__ == "__main__":
    alarm_characteristic_statistics("2023/4/10 8:00:00", "2023/4/10 8:10:00")