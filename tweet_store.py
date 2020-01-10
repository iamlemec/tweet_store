# twitter store

import os
import time
import json
import twitter
import sqlite3
import pandas as pd

RATE_LIMIT = 15
RATE_WINDOW = 15
MAX_COUNT = 200

default = {
    'include_rts': False,
    'exclude_replies': True,
    'trim_user': True
}

class TweetStore:
    def __init__(self, handle, db, table='tweet', auth=None):
        self.handle = handle
        self.table = table

        db_exists = os.path.exists(db)
        self.con = sqlite3.connect(db)
        if not db_exists:
            self.init_db()

        if auth is None:
            auth = f'creds/{handle}.json'
        with open(auth) as f:
            creds = json.load(f)
        self.api = twitter.Api(**creds)

    def __del__(self):
        self.con.close()

    def init_db(self):
        cur = self.con.cursor()
        cur.execute(f'create table if not exists {self.table} (id int, created int, handle text, body text)')
        cur.execute(f'create unique index if not exists tid on {self.table} (id)')
        self.con.commit()

    def sync_batch(self, when='newest', **kwargs):
        cur = self.con.cursor()

        min_id, max_id = cur.execute(f'select min(id),max(id) from {self.table}').fetchone()
        if when == 'newest':
            iargs = {'since_id': max_id}
        elif when == 'oldest':
            iargs = {'max_id': min_id - 1 if min_id is not None else None}
        else:
            iargs = {}

        args = {**default, **iargs, **kwargs}
        stats = self.api.GetUserTimeline(screen_name=self.handle, count=MAX_COUNT, **args)
        nrets = len(stats)

        print(f'Fetched {nrets} {when} tweets')
        if nrets == 0:
            return 0

        cur.executemany(
            f'insert or replace into {self.table} values (?,?,?,?)',
            [(st.id, st.created_at_in_seconds, self.handle, st.text) for st in stats]
        )
        self.con.commit()

        return nrets

    def sync_window(self, when=None, **kwargs):
        done_old = (when == 'newest')
        done_new = (when == 'oldest')
        for i in range(RATE_LIMIT):
            if not done_old:
                nrets = self.sync_batch(when='oldest', **kwargs)
                if nrets == 0:
                    done_old = True
            if not done_new:
                nrets = self.sync_batch(when='newest', **kwargs)
                if nrets == 0:
                    done_new = True
            if done_old and done_new:
                return True
        return False

    def sync(self, **kwargs):
        while True:
            if self.sync_window(**kwargs):
                break
            time.sleep(60*RATE_WINDOW)

class TweetView:
    def __init__(self, db, table='tweet'):
        self.table = table

        if not os.path.exists(db):
            raise('Database does not exist')

        self.con = sqlite3.connect(db)

    def __del__(self):
        self.con.close()

    def fetch(self, limit=None):
        ltxt = f'{limit}' if limit is not None else ''
        cur = self.con.cursor()
        cur.execute(f'select * from {self.table} {ltxt}')
        return cur

    def fetch_many(self, limit=10):
        return cur.fetch(limit=limit).fetchall()

    def fetch_all(self):
        return self.fetch().fetchall()

    def fetch_frame(self, limit=None):
        tweets = self.fetch_all()
        df = pd.DataFrame(tweets, columns=['id', 'ts', 'handle', 'text'])
        df['time'] = pd.to_datetime(df['ts'], unit='s')
        return df[['id', 'handle', 'time', 'text']].set_index('id')

def export(db, csv, table='tweet'):
    tv = TweetView(db, table=table)
    df = tv.fetch_frame()
    df.to_csv(csv)
