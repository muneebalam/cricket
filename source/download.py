"""
The purpose of this module is to download data from cricsheet.org
"""

import os.path
import os
from datetime import datetime
import urllib.request
import feather
import pandas as pd
import zipfile
import re
import yaml
import sqlite3

def folder_setup():
    """
    Create directories if need be
    :return:
    """
    for fname in ['../data/', '../data/raw/', '../data/interim/', '../data/sqlite/', '../data/feather/',
                  '../notebooks/', '../source/']:
        if not os.path.exists(fname):
            os.mkdir(fname)


def get_data_raw_dir():
    """
    Gets raw directory for data--zipped files

    :return: ../data/raw/
    """
    return os.path.join('..', 'data', 'raw')


def get_data_interim_dir(date=None):
    """
    Gets interim directory for data--unzipped files, one folder per date

    :param date: str, YYYY-MM-DD

    :return: ../data/interim or ../data/interim/[date]
    """
    if date is None:
        return os.path.join('..', 'data', 'interim')
    return os.path.join('..', 'data', 'interim', date)


def get_data_sql_dir():
    """
    Gets processed directory for data--unzipped yaml files aggregated into sqlite

    :return: ../data/sqlite/
    """
    return os.path.join('..', 'data', 'sqlite')


def get_data_feather_dir():
    """
    Gets processed directory for data--unzipped yaml files aggregated into sqlite

    :return: ../data/sqlite/
    """
    return os.path.join('..', 'data', 'feather')


def get_data_raw_filename(date):
    return os.path.join(get_data_raw_dir(), '{0:s}.zip'.format(date))


def get_notebook_dir():
    """
    Returns directory for notebooks

    :return: ../notebooks/
    """
    return os.path.join('..', 'notebooks')


def get_source_dir():
    """
    Returns directory for .py files

    :return: ../source/
    """
    return os.path.join('..', 'source')


def todays_date():
    """
    Quick helper method to get today's date

    :return: str, YYYY-MM-DD
    """
    strdate = str(datetime.now())
    strdate = strdate[:strdate.index(' ')]
    return strdate


def get_new_data_zip_filename():
    """
    Quick helper method to get the filename of a new zipped file scraped today

    :return: raw directory / [today's date].zip
    """
    return os.path.join(get_data_raw_dir(), '{0:s}.zip'.format(todays_date()))


def most_recent_file(directory):
    """
    A quick helper method that gets files in directory, sorts, and returns the most recent. Filenames should be
    directory/YYYY-MM-DD.ext format

    :param directory: str

    :return: a file in directory
    """
    return os.path.join(directory, sorted(os.listdir(directory))[-1])


def most_recent_date(directory):
    """
    Similar to most_recent_file, but extracts date from that file

    :param directory: str

    :return: str, YYYY-MM-DD
    """
    fname = most_recent_file(directory)
    return re.search('(\d{4}-\d{2}-\d{2})', fname).group(0)


def unzip_to_folder(date=None):
    """
    Extracts files from a zip in raw data to a directory of files in interim.

    :param date: YYYY-MM-DD, defaults to most recent in raw folder

    :return:
    """
    if date is None:
        date = most_recent_date(get_data_raw_dir())
    if not os.path.exists(get_data_interim_dir(date)):
        os.mkdir(get_data_interim_dir(date))

    zfiles = zipfile.ZipFile(get_data_raw_filename(date))
    zfiles.extractall(path=get_data_interim_dir(date))


def get_zip_url():
    """https://cricsheet.org/downloads/all.zip"""
    return 'https://cricsheet.org/downloads/all.zip'


def save_new_zip():
    """
    Reads and saves the zip from cricsheet.org
    :return:
    """
    with urllib.request.urlopen(get_zip_url()) as url:
        page = url.read()
    with open(get_new_data_zip_filename(), 'wb') as w:
        w.write(page)


def get_sql_db_fname(dbname=None):
    lst = {'matchinfo': os.path.join(get_data_sql_dir(), 'info.sqlite'),
           'delivery': os.path.join(get_data_sql_dir(), 'delivery.sqlite')}
    if dbname is None or dbname not in lst:
        return lst
    return lst[dbname]


def gt_sql_db_names():
    return {'matchinfo', 'delivery'}


def get_sql_db_table_names(dbname):
    return {'matchinfo': ['outcome'],  #, 'toss', 'venue', 'city', 'pom', 'dates', 'teams', 'umpires', 'mf'],
            'delivery': []}[dbname]


def get_sql_db_table_code(dbname, tblname):
    fun = {'matchinfo': {'outcome': outcome_table_sql}}[dbname][tblname]
    return fun()


def outcome_table_sql():
    header = 'CREATE TABLE outcome'
    colnames = ['ID', 'winner', 'bytype', 'byamt']
    coltypes = ['TEXT PRIMARY KEY', 'TEXT', 'TEXT', 'INTEGER']
    cols = ', '.join(['{0:s} {1:s}'.format(cname, ctype) for cname, ctype in zip(colnames, coltypes)])
    return '{0:s} ({1:s})'.format(header, cols)


def add_to_outcomes(ids, winners=None, bytypes=None, byamts=None):
    conn = sqlite3.connect(get_sql_db_fname('matchinfo'))
    c = conn.cursor()

    if not isinstance(ids, list):
        ids = [ids]
    if not isinstance(winners, list):
        winners = [winners]
    if not isinstance(bytypes, list):
        bytypes = [bytypes]
    if not isinstance(byamts, list):
        byamts = [byamts]

    for id, winner, bytype, byamt in zip(ids, winners, bytypes, byamts):
        if winner is None:
            c.execute("INSERT OR IGNORE INTO outcome (ID) VALUES ({0:s})".format(id))
        else:
            c.execute("INSERT OR IGNORE INTO outcome (ID, winner, bytype, byamt) VALUES ({0:s}, {1:s}, {2:s}, {3:d})" \
                  .format(id, winner, bytype, byamt))
    conn.commit()
    conn.close()


def aggregate_yaml_to_feather(date=None, limit=None):
    """

    :param date:
    :param limit:
    :return:
    """
    if date is None:
        date = most_recent_date(get_data_interim_dir())

    parent = get_data_interim_dir(date)
    files = os.listdir(parent)

    fnames = [os.path.join(parent, file) for file in files]
    info_dfs = []
    innings_dfs = []
    for i, file in enumerate(fnames):
        with open(file, 'r') as r:
            yfile = yaml.load(r)

            # assign an ID--the number part of filename before yaml and after the date
            matchid = re.search('\d{4}-\d{2}-\d{2}\\\\(\d+)', file).group(0)[11:]

            info = pd.io.json.json_normalize(yfile['info']).assign(ID=matchid)
            innings = pd.io.json.json_normalize(yfile['innings']).assign(ID=matchid)
            info_dfs.append(info)
            innings_dfs.append(innings)

        print('Done with {0:d}/{1:d}'.format(i+1, len(fnames)))
        if limit is not None and i > limit:
            break

    infodf = pd.concat(info_dfs)
    inningsdf = pd.concat(innings_dfs)

    for col in infodf.columns:
        if infodf[col].dtype == 'object':
            infodf.loc[:, col] = infodf[col].astype(str)
    for col in inningsdf.columns:
        if inningsdf[col].dtype == 'object':
            inningsdf.loc[:, col] = inningsdf[col].astype(str)

    feather.write_dataframe(infodf, get_info_dataframe_filename(date))
    feather.write_dataframe(inningsdf, get_innings_dataframe_filename(date))


def get_info_dataframe_filename(date):
    return os.path.join(get_data_feather_dir(), '{0:s}_info.feather'.format(date))
def get_info_dataframe(date):
    return feather.read_dataframe(get_info_dataframe_filename(date))
def get_innings_dataframe_filename(date):
    return os.path.join(get_data_feather_dir(), '{0:s}_innings.feather'.format(date))
def get_innings_dataframe(date):
    return feather.read_dataframe(get_innings_dataframe_filename(date))


def edit_feather_to_sql(date=None):
    if date is None:
        date = most_recent_date(get_data_feather_dir())

    info = edit_and_write_info(date)
    innings = edit_and_write_innings(date)

    # Write to SQL. Every column without a period will be in the main table
    # Columns with a period will be a new table (table name is what comes before the period)
    for df, dbname in [(info, 'matchinfo'), (innings, 'innings')]:
        period_df = df.filter(regex='\.')
        no_period_df = df.drop(df.filter(regex='\.').columns, axis=1)
        conn = sqlite3.connect(get_sql_db_fname(dbname))
        no_period_df.to_sql(dbname, con=conn, if_exists='replace')

        col_to_table = {}
        for col in period_df.columns:
            tname = col[:col.index('.')]
            if tname not in col_to_table:
                col_to_table[tname] = []
            col_to_table[tname].append(col)
        for tbl in col_to_table:
            tmp = period_df[col_to_table[tbl]]
            tmp.to_sql(tbl, con=conn, if_exists='replace')

        conn.commit()
        conn.close()

def edit_and_write_innings(date):
    return None

def edit_and_write_info(date):

    info = get_info_dataframe(date).rename(columns={'ID': 'MATCHID'}).set_index('MATCHID')
    list_cols_info = ['dates', 'player_of_match', 'teams', 'umpires']
    # For when I have lists, separate
    # E.g. Team is a list of 2--separate into team1 and team2
    # Apply to dates, player_of_match, teams, umpires
    # Rows with lists will also be written to different tables
    for col in list_cols_info:
        vals = info[col].values
        if col == 'dates':
            vals = [x[1:-1].replace("datetime.date", "") for x in vals]
            # Split on commas, make sure months and years have zero pads
            vals = [[re.sub(r'-(\d)-', r'-0\1-', re.sub(r'-(\d)$', r'-0\1', y.replace(', ', '-')))
                     for y in re.findall(r'\d{4}, \d{2}, \d{1,2}', x)] for x in vals]
        else:
            vals = [x[1:-1].replace("'", "").split(', ') for x in vals]

        max_length = max([len(x) for x in vals])
        newcols = {x: '{0:s}.{1:d}'.format(col[:-1], x+1) for x in range(max_length)}  # :-1 to e.g. turn dates into date
        for i, newcol in newcols.items():
            info.loc[:, newcol] = [x[i] if i < len(x) else None for x in vals]
        info = info.drop(col, axis=1)

    return info


def access_dict(dct, *keys):
    try:
        if keys is None or len(keys) == 0:
            return dct
        return access_dict(dct[keys[0]], keys[1:])
    except KeyError:
        return None
    except ValueError:
        return None
    except IndexError:
        return None

def download_new_data():
    """
    This method gets new data and creates the database in four steps:

    1) Get the new zip file from the Internet
    2) Unzip it to a folder
    3) Aggregate the unzipped files
    4) Edit the aggregated result for format, and write to database

    :return:
    """
    #save_new_zip()
    #unzip_to_folder()
    #aggregate_yaml_to_feather(limit=20)
    edit_feather_to_sql()


folder_setup()


if __name__ == '__main__':
    download_new_data()

