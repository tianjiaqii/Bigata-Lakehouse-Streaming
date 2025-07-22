from DrissionPage import Chromium,ChromiumOptions
from lxml import etree
import pandas as pd
import pymysql

def get_conn():
    db = pymysql.connect(host='localhost',
                         user='root',
                         password='root',
                         database='weather_db')
    return db

def weater_get_days(tree):

    provice= tree.xpath("//*[@id='cityPosition']/div[3]/button/span[1]/text()")

    city=tree.xpath("//*[@id='cityPosition']/div[5]/button/text()")

    days = tree.xpath("//div[contains(@class, 'pull-left day')]")

    weekday_list = []

    date_list = []

    day_wether_list=[]

    temp_high_list=[]

    temp_low_list=[]

    for day in days:
        weekday = day.xpath("./div[contains(@class, 'day-item')][1]/text()[1]")[0].strip()

        weekday_list.append(weekday)

        date = day.xpath("./div[contains(@class, 'day-item')][1]/text()[2]")[0].strip()

        date_list.append(date)

        day_weather = day.xpath("./div[contains(@class, 'day-item')][3]/text()")[0].strip()

        day_wether_list.append(day_weather)

        high_temp = day.xpath(".//div[@class='high']/text()")[0].strip()

        temp_high_list.append(high_temp)

        low_temp = day.xpath(".//div[@class='low']/text()")[0].strip()

        temp_low_list.append(low_temp)

    weather={
        "week":weekday_list,
        "date":date_list,
        "day_weather":day_wether_list,
        "high_temp":temp_high_list,
        "low_temp":temp_low_list
    }
    db = get_conn()
    for i in range(len(weather['week'])):

        cursor = db.cursor()
        try:
            sql = "insert into weather_country (province,city,weekday,date,weather,max_tem,min_tem) values ('%s','%s','%s','%s','%s','%s','%s');" % (
                provice[0], city[0], weather['week'][i], weather['date'][i], weather['day_weather'][i],
                weather['high_temp'][i], weather['low_temp'][i])
            cursor.execute(sql)
            db.commit()
        except:
            db.rollback()

        print(f"省份:{provice[0]},城市:{city[0]},"
              f"星期:{weather['week'][i]},日期:{weather['date'][i]},"
              f"天气:{weather['day_weather'][i]}, "
              f"最高温度:{weather['high_temp'][i]},最低温度:{weather['low_temp'][i]}")
    db.close()
def get_list():

    df=pd.read_excel(r"E:\爬虫\中国地面气象数据-站点信息 (1).xlsx")

    l1=df['Station_Id_C'].tolist()

    return l1

def main():
    co = ChromiumOptions().set_browser_path(r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe")

    browser = Chromium(addr_or_opts=co)

    tab = browser.new_tab()

    l1=get_list()

    for e in l1:
        tab.get(f'https://weather.cma.cn/web/weather/{e}.html')

        html = tab.html

        tree = etree.HTML(html)

        weater_get_days(tree)

        tab.wait(0.5)

    tab.close()

    browser.quit()

if __name__ == '__main__':

    main()
