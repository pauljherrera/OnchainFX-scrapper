import sys
import datetime
import re
import time
import pandas as pd
import pymongo
import urllib.parse

from pymongo.errors import ConfigurationError
from bs4 import BeautifulSoup as BS
from bs4 import SoupStrainer
from apscheduler.schedulers.blocking import BlockingScheduler
from selenium import webdriver

PRODUCTION = False

# onchainfx
# martes 24 11:00 pm

# this function parse the html, find all the tags with the relevant data of the table
# then returns a pandas DataFrame with the content


def load_html_dataframe():

    # open chrome the gets the html code after the dinamic data loads
    # unzip the PhantomJS driver in the current path (Windows)
    # install phantomjs via apt-get/yum, etc (Linux)

    if sys.platform == 'win32':
        browser = webdriver.PhantomJS("phantomjs-2.1.1/bin/phantomjs.exe")
    elif sys.platform == 'linux':
        try:
            browser = webdriver.PhantomJS()  # Do not change please. Install phantomjs with apt/yum.
        except:
            browser = webdriver.PhantomJS("phantomjs-2.1.1-linux-x86_64/bin/phantomjs")
    else:
        sys.exit('Unsupported OS')
    browser.get("http://onchainfx.com/v/L5KTFZ")

    time.sleep(15)

    html_page = browser.page_source
    browser.quit()

    matrix_head = SoupStrainer("thead")
    matrix_body = SoupStrainer("tbody")

    # parse the fraction of the html code the head and the body of the matrix
    soup_h = BS(html_page, "html.parser", parse_only=matrix_head)
    soup_b = BS(html_page, "html.parser", parse_only=matrix_body)


    # finds all the elements that contains the data we're looking for
    indexes = soup_h.find_all("th")
    names = soup_b.find_all("a", "table_asset_link")
    data_b = soup_b.find_all("td")

    table_data = [re.sub('[$,cents]', '', i.get_text()) for i in data_b]

    # loads the data of the matrix by type
    indexlist = [item.get_text() for item in indexes[2:]]
    indexlist.append('Date')

    namelist = [i.get_text() for i in names]
    capital = [i for i in table_data[3::10]]
    current = [i for i in table_data[4::10]]
    change = [i for i in table_data[5::10]]
    price = [i for i in table_data[6::10]]
    volume = [i for i in table_data[7::10]]
    supply = [i for i in table_data[8::10]]
    supply_per = [i for i in table_data[9::10]]

    # time stamps
    timenow = "%s" % datetime.datetime.now()
    timenow = timenow[:16]
    timestmp = [timenow for i in range(len(namelist))]

    # creates the dataframe that's going to be loaded into the database
    columns_content = [namelist, capital, current, change, price, volume, supply, supply_per, timestmp]
    dt_dict = dict(zip(indexlist, columns_content))

    try:
        df = pd.DataFrame(dt_dict)
    except ValueError:
        sys.exit(50)

    return df


def maintainload(database):

    dataf_cont = load_html_dataframe()

    names = dataf_cont["Name"]
    del dataf_cont["Name"]

    # keys values of the dictionary of our document for the collections
    keys = dataf_cont.columns

    for j, objs in enumerate(names):
        act_collct = database.get_collection(objs)
        act_collct.insert(dict(zip(keys, dataf_cont.iloc[j])))


def main():

    sched = BlockingScheduler()

    opc = input('Create or maintain a database?  (1 -Create | 2 -Maintain):')
    # Permissions for maintain: 'readWrite'.

    dbname = input("Database name:")
    user = input('Username:')
    password = input('Password:')

    usr = urllib.parse.quote_plus(user)
    pwd = urllib.parse.quote_plus(password)

    if PRODUCTION:
        client = pymongo.MongoClient('mongodb://%s:%s@10.8.0.2' % (usr, pwd))
    else:
        client = pymongo.MongoClient('mongodb://%s:%s@127.0.0.1' % (usr, pwd))
    db = client[dbname]
    if opc == "1":
        db.add_user(user, password)

    try:
        client.admin.command('ismaster')
    except ConfigurationError:
        print('Server not available')

    print("Press 'CTRL + C' to exit")

    dataf = load_html_dataframe()

    # getting the name of each cryptocurrency
    collects = dataf["Name"]
    del dataf["Name"]

    if opc == "1":
        for items in collects:
        # Create the collections if it hasnt being created to avoid conflicts
            if items not in db.collection_names():
                db.create_collection(items)
                act_collect = db.get_collection(items)


    # every hour runs maintainload and insert documents into the collections
    sched.add_job(maintainload, trigger='cron', args=[db], minute=0, second=0)
    try:
        print("Initialized")
        sched.start()
    except (KeyboardInterrupt, SystemExit):
        pass


if __name__ == "__main__":
    main()