import praw
from cryptography.fernet import Fernet
import base64
import json
from datetime import datetime
from typing import Tuple, Set
from joblib import Parallel, delayed
from itertools import chain
import os
import sqlite3

post_columns_global = ['Id', 'Title', 'Author', 'Author_flair', 'Created', 'Subreddit', 'Subreddit_id', 'Text',
                'Text_content', 'Num_comments', 'Score', 'Upvote_ratio', 'Stickied', 'Distinguished', 'URL']
comment_columns_global = ['Comment_id', 'Author', 'Created', 'Submission_id', 'Text_content', 'Num_replies', 'Score', 'Stickied', 'Distinguished']
reply_columns_global = ['Reply_id', 'Author', 'Created', 'Submission_id', 'Parent_id', 'Text_content', 'Score', 'Stickied', 'Distinguished']
subreddit_columns_global = ['Id', 'Name', 'Description', 'Public_description', 'Created', 'Subscribers']
table_names = ('Posts', 'Subreddits', 'Comments', 'Replies')

def reddit_object(current_path, password):
    c=Fernet(f'{password}=')
    private_json_path = os.path.join(current_path, "private.json")
    with open(private_json_path, "rb") as f:ec=json.loads(c.decrypt(f.read()).decode())
    ci,cs,rt=c.decrypt(base64.b64decode(ec["client_id"])).decode(),c.decrypt(base64.b64decode(ec["client_secret"])).decode(),c.decrypt(base64.b64decode(ec["refresh_token"])).decode()

    reddit = praw.Reddit(
        client_id = ci,
        client_secret = cs,
        refresh_token = rt,
        user_agent = "nlp_project:v0.1 (by u/Neoncodemaj)",
    )
    return reddit

def start_connection(database_path = 'test_database.db'):
    global conn
    if 'conn' in globals() and isinstance(conn, sqlite3.Connection):
        try:
            # Attempt to close the connection
            conn.close()
            print("Existing connection closed.")
        except sqlite3.ProgrammingError:
            # Catch the exception if the connection is already closed
            pass
        
    if os.path.exists(database_path):
        print(f"Database {database_path} already exists. Deleting it.")
        os.remove(database_path)
    else:
        print(f"Database '{database_path}' does not exist. Creating it")
        
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()
    print('Connection established, new cursor object created.')
    return conn, cursor


def prepare_database(conn=None, cursor=None, tables=table_names):
    if conn is None:
        conn = globals()['conn']
    if cursor is None:
        cursor = globals()['cursor']
        
    # not setting CHAR with character limit, since I do not know limitations yet.
    cursor.execute(f'''SELECT count(name) FROM sqlite_master WHERE type='table' AND name IN {tables}''')
    if cursor.fetchone()[0] > 0: # also assuming there already are indexes
        for table in tables:
            cursor.execute(f'''DELETE FROM {table}''')
    else:
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Posts (
                Id TEXT PRIMARY KEY,
                Title TEXT,
                Author TEXT,
                Author_flair TEXT,
                Created TEXT,
                Subreddit TEXT,
                Subreddit_id TEXT,
                Text TEXT,
                Text_content TEXT,
                Num_comments INTEGER,
                Score INTEGER,
                Upvote_ratio REAL,
                Stickied INTEGER,
                Distinguished TEXT,
                URL TEXT
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Subreddits (
                Id TEXT PRIMARY KEY,
                Name TEXT,
                Description TEXT,
                Public_description TEXT,
                Created TEXT,
                Subscribers INTEGER,
                FOREIGN KEY (Id) REFERENCES Posts(Subreddit_id) ON DELETE CASCADE
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Comments (
                Comment_id TEXT PRIMARY KEY,
                Author TEXT,
                Created TEXT,
                Submission_id TEXT NOT NULL ON CONFLICT IGNORE,
                Text_content TEXT NOT NULL ON CONFLICT IGNORE,
                Num_replies INTEGER,
                Score INTEGER,
                Stickied INTEGER,
                Distinguished TEXT,
                FOREIGN KEY (Submission_id) REFERENCES Posts(id) ON DELETE CASCADE
            )
        ''')


        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Replies (
                Reply_id TEXT PRIMARY KEY,
                Author TEXT,
                Created TEXT,
                Submission_id TEXT NOT NULL ON CONFLICT IGNORE,
                Parent_id TEXT NOT NULL ON CONFLICT IGNORE,
                Text_content TEXT NOT NULL ON CONFLICT IGNORE,
                Score INTEGER,
                Stickied INTEGER,
                Distinguished TEXT,
                FOREIGN KEY (Submission_id) REFERENCES Posts(id) ON DELETE CASCADE,
                FOREIGN KEY (Parent_id) REFERENCES Comments(Comment_id) ON DELETE CASCADE
            )
        ''')
        
        # Indexes. Might need to add more if I find out certain queries are repeatable
        cursor.execute('CREATE INDEX idx_Posts_Subreddit_id ON Posts(Subreddit_id);')
        cursor.execute('CREATE INDEX idx_Comments_Submission_id ON Comments(Submission_id);')
        cursor.execute('CREATE INDEX idx_Replies_Submission_id ON Replies(Submission_id);')
        cursor.execute('CREATE INDEX idx_Replies_Parent_id ON Replies(Parent_id);')
        
    conn.commit()

def process_post_batch(posts, comments_limit, replies_limit) -> Tuple[Set[Tuple], Set[Tuple], Set[Tuple], Set[Tuple]]:
    comment_data = set()
    reply_data = set()
    post_data = set()
    subreddit_data = set()

    for post in posts:
        subreddit = post.subreddit
        post_data.add((
            post.id,
            post.title,
            post.author.name if post.author else None,
            post.author_flair_text,
            datetime.fromtimestamp(post.created_utc),
            subreddit.display_name,
            subreddit.id,
            post.is_self,
            post.selftext if post.selftext else None,
            post.num_comments,
            post.score,
            post.upvote_ratio,
            post.stickied,
            post.distinguished,
            post.url
        ))
        subreddit_data.add((
            subreddit.id,
            subreddit.display_name,
            subreddit.description,
            subreddit.public_description,
            datetime.fromtimestamp(subreddit.created_utc),
            subreddit.subscribers
        ))
        comment_batch, reply_batch = process_comments(post, comments_limit, replies_limit)
        comment_data.update(comment_batch)
        reply_data.update(reply_batch)

    return post_data, subreddit_data, comment_data, reply_data

def process_comments(post, comments_limit, replies_limit) -> Tuple[Set[Tuple], Set[Tuple]]:
    comment_data = set()
    reply_data = set()
   
    for comment in post.comments[:comments_limit]:
        if isinstance(comment, praw.models.MoreComments):
            break
        
        batch_replies = set()
        num_replies = 0 
        for reply in comment.replies[:replies_limit]:
            # Stacking loop inside a loop and not separate function because of joblib.Parallel
            # Cannot pass unserializable objects as function parameters
            if isinstance(reply, praw.models.MoreComments):
                break
            
            batch_replies.add((
                reply.id,
                reply.author.name if reply.author else None,
                datetime.fromtimestamp(reply.created_utc),
                post.id,
                comment.id, # instead of: reply.parent_id
                reply.body,
                reply.score,
                reply.stickied,
                reply.distinguished
            ))
            num_replies += 1
        reply_data.update(batch_replies)
        
        comment_data.add((
            comment.id,
            comment.author.name if comment.author else None,
            datetime.fromtimestamp(comment.created_utc),
            post.id,
            comment.body,
            num_replies,
            comment.score,
            comment.stickied,
            comment.distinguished
        ))

    return comment_data, reply_data

def batch_generator(iterable, batch_size):
    """Generator to yield batches of items from an iterable."""
    batch = []
    for item in iterable:
        batch.append(item)
        if len(batch) == batch_size:
            yield batch
            batch = []
    if batch:
        yield batch

def fill_tables(query: str,
                posts_limit: int = 25,
                comments_limit: int = 25, # max 100
                replies_limit: int = 25, # max 100
                conn = None,
                cursor = None,
                reddit = None,
                table_names = table_names):
    if conn is None:
        conn = globals()['conn']
    if cursor is None:
        cursor = globals()['cursor']
    if reddit is None:
        reddit = globals()['reddit']
    
    native_limit = 100 # current limit in Reddit API
    post_data = set()
    subreddit_data = set()
    comment_data = set()
    reply_data = set()
    post_columns_local, subreddit_columns_local = post_columns_global, subreddit_columns_global
    comment_columns_local, reply_columns_local = comment_columns_global, reply_columns_global
    
    if posts_limit > native_limit:
        div, mod = divmod(posts_limit, native_limit)
        last_post_id = None
        after_param = None
        result_collection = chain()
        for i in range(div+1):
            if i == div and mod == 0:
                break
            if last_post_id:
                after_param = last_post_id  # Use last post ID from the previous batch

            search_results = list(reddit.subreddit('all').search(query,
                                                                 limit=native_limit if i != div else mod,
                                                                 params={'after': after_param}))
            
            result_collection = chain(result_collection, search_results)
            last_post_id = search_results[-1].id

    else:
        result_collection = reddit.subreddit('all').search(query, limit=posts_limit)

    # Process posts in batches
    batch_size = (posts_limit // os.cpu_count()) + 1
    results = Parallel(n_jobs=-1)(delayed(process_post_batch)(batch, comments_limit, replies_limit) for batch in batch_generator(result_collection, batch_size))
    for post_batch, subreddit_batch, comment_batch, reply_batch in results:
        post_data.update(post_batch)
        subreddit_data.update(subreddit_batch)
        comment_data.update(comment_batch)
        reply_data.update(reply_batch)
        
    insert_posts_query = f'INSERT OR IGNORE INTO {table_names[0]} ({", ".join(post_columns_local)}) VALUES ({", ".join(["?"] * len(post_columns_local))})'
    # Insert or replace/ignore, because data integrity issue and snapshoting of the same subreddit from different submissions.
    insert_subreddits_query = f'INSERT OR IGNORE INTO {table_names[1]} ({", ".join(subreddit_columns_local)}) VALUES ({", ".join(["?"] * len(subreddit_columns_local))})'
    insert_comments_query = f'INSERT OR IGNORE INTO {table_names[2]} ({", ".join(comment_columns_local)}) VALUES ({", ".join(["?"] * len(comment_columns_local))})'
    insert_replies_query = f'INSERT OR IGNORE INTO {table_names[3]} ({", ".join(reply_columns_local)}) VALUES ({", ".join(["?"] * len(reply_columns_local))})'
    
    cursor.executemany(insert_posts_query, post_data)
    cursor.executemany(insert_subreddits_query, subreddit_data)
    cursor.executemany(insert_comments_query, comment_data)
    cursor.executemany(insert_replies_query, reply_data)
    
    conn.commit()

def main():
    current_path = os.path.dirname(os.path.abspath(__file__))
    password = input("Provide a password do reddit API: ")
    database_path = f'{input("Provide database name: ")}.db'
    query = input("Provide query for search: ")
    reddit = reddit_object(current_path=current_path, password=password)
    conn, cursor = start_connection(database_path=os.path.join(current_path, database_path))
    prepare_database(conn=conn, cursor=cursor)
    print(f'Inserting values into database {database_path}...')
    fill_tables(query=query,
                posts_limit=300,
                comments_limit=50,
                replies_limit=20,
                conn=conn,
                cursor=cursor,
                reddit=reddit)
    print('Operation complete, database filled.')
    conn.close()
    
if __name__ == "__main__":
    main()