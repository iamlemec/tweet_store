# twitter store

import os
import time
import json
import twitter
import sqlite3

RATE_LIMIT = 15
RATE_WINDOW = 15

class TweetStore:
    def __init__(self, handle, db, table='tweet', auth=None):
        self.handle = handle
        self.table = table

        db_exists = os.path.exists(db)
        self.con = sqlite3.connect(db)
        if not db_exists:
            self.init_db()

        if auth is None:
            auth = os.path.join('creds', '%s.json' % handle)
        with open(auth) as f:
            creds = json.load(f)
        self.api = twitter.Api(**creds)

    def __del__(self):
        self.con.close()

    def init_db(self):
        cur = self.con.cursor()
        cur.execute('create table if not exists %s (id int, created int, handle text, body text)' % self.table)
        cur.execute('create unique index if not exists tid on %s (id)' % self.table)
        self.con.commit()

    def sync_batch(self, when='newest'):
        cur = self.con.cursor()

        (min_id, max_id) = cur.execute('select min(id),max(id) from %s' % self.table).fetchone()
        if when == 'newest':
            args = {'since_id': max_id}
        else:
            args = {'max_id': min_id - 1 if min_id is not None else None}
        stats = self.api.GetUserTimeline(screen_name=self.handle, include_rts=False, exclude_replies=True,
                                         trim_user=True, count=200, **args)
        nrets = len(stats)

        print('Fetched %d %s tweets' % (nrets, when))
        if nrets == 0:
            return 0

        cur.executemany('insert or replace into %s values (?,?,?,?)' % self.table,
            [(st.id, st.created_at_in_seconds, self.handle, st.text) for st in stats])
        self.con.commit()

        return nrets

    def sync_window(self, when=None):
        done_old = (when == 'newest')
        done_new = (when == 'oldest')
        for i in range(RATE_LIMIT):
            if not done_old:
                nrets = self.sync_batch(when='oldest')
                if nrets == 0:
                    done_old = True
            if not done_new:
                nrets = self.sync_batch(when='newest')
                if nrets == 0:
                    done_new = True
            if done_old and done_new:
                return True
        return False

    def sync(self):
        while True:
            if self.sync_window():
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

    def fetch(self):
        cur = self.con.cursor()
        cur.execute('select * from %s' % self.table)
        return cur

    def fetchmany(self, limit=10):
        cur = self.con.cursor()
        cur.execute('select * from %s limit %d' % (self.table, limit))
        return cur.fetchall()

    def fetchall(self):
        cur = self.con.cursor()
        cur.execute('select * from %s' % self.table)
        return cur.fetchall()
