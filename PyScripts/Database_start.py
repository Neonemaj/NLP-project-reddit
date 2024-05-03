import praw
from cryptography.fernet import Fernet
import base64
import json
from datetime import datetime
from typing import Tuple, Set, List, Generator, Iterable
from joblib import Parallel, delayed
from itertools import chain
import os
import re
import sqlite3

post_columns_global = ['Id', 'Title', 'Author', 'Author_flair', 'Created', 'Subreddit', 'Subreddit_id', 'Text',
                'Text_content', 'Num_comments', 'Score', 'Upvote_ratio', 'Stickied', 'Distinguished', 'URL']
comment_columns_global = ['Id', 'Author', 'Created', 'Submission_id', 'Text_content', 'Num_replies', 'Score', 'Stickied', 'Distinguished']
reply_columns_global = ['Id', 'Author', 'Created', 'Submission_id', 'Parent_id', 'Text_content', 'Score', 'Stickied', 'Distinguished']
subreddit_columns_global = ['Id', 'Name', 'Description', 'Public_description', 'Created', 'Subscribers']
table_names = ('Posts', 'Subreddits', 'Comments', 'Replies')

def reddit_object(private_path: str, password: str) -> praw.reddit.Reddit:
    '''
    Creates Reddit object by connecting to Praw - Reddit API.
    
    Parameters:
        private_path (str): Path to the .json file containing encrypted data needed to connect to API.
        password (str): Password used to decrypt data.
    '''
    key=Fernet(f'{password}=')
    with open(private_path, "rb") as f:
        ec=json.loads(key.decrypt(f.read()).decode())
    ci = key.decrypt(base64.b64decode(ec["client_id"])).decode()
    cs = key.decrypt(base64.b64decode(ec["client_secret"])).decode()
    rt = key.decrypt(base64.b64decode(ec["refresh_token"])).decode()

    reddit = praw.Reddit(
        client_id = ci,
        client_secret = cs,
        refresh_token = rt,
        user_agent = "nlp_project:v0.1 (by u/Neoncodemaj)",
    )
    return reddit

def start_connection(database_path: str = 'test_database.db') -> Tuple[sqlite3.Connection, sqlite3.Cursor]:
    '''
    Creates a sqlite3 database and connects to it, creating connection and cursor objects.
    
    Parameters:
        database_path (str): Path to the database that will be created or overwritten.
        
    Returns:
        Tuple[sqlite3.Connection, sqlite3.Cursor]: Connection and Cursor objects needed for further interaction with new database.
        
    Notes:
        - If a database provided with database_path already exists, this function will delete it and create new in its place.
        - If there is already connection object (conn) in global scope, this function will attempt to close it before creating new connection object.
    '''
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


def prepare_database(conn: sqlite3.Connection=None, cursor: sqlite3.Cursor=None, table_names: Tuple[str, str, str, str]=table_names) -> None:
    '''
    Creates 4 tables and 4 additional indexes (beyond 4 default ones) in current connection.
    
    Parameters:
        conn (sqlite3.Connection): SQLite database connection.
        cursor (sqlite3.Cursor): SQLite database cursor.
        table_names (Tuple[str, str, str, str]): Tuple of 4 table names.
    
    Notes:
        - Function modifies database in-place, does not return anything.
        - Because of hard-coded column names, expected order of tables is: Posts, Subreddits, Comments, Replies.
        - Function will atempt to use global conn and cursor objects if none were provided within parameters.
        - If tables with table_names (at least one) already exist in this database, function will wipeout all tables before proceeding.
    '''
    if conn is None:
        conn = globals()['conn']
    if cursor is None:
        cursor = globals()['cursor']
        
    cursor.execute(f'''SELECT count(name) FROM sqlite_master WHERE type='table' AND name IN {table_names};''')
    if cursor.fetchone()[0] > 0: # also assuming there already are indexes
        for table in table_names:
            cursor.execute(f'''DELETE FROM {table};''')
    else:
        cursor.execute("BEGIN TRANSACTION;")
        try:
            cursor.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_names[0]} (
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
                );
            ''')

            cursor.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_names[1]} (
                    Id TEXT PRIMARY KEY,
                    Name TEXT,
                    Description TEXT,
                    Public_description TEXT,
                    Created TEXT,
                    Subscribers INTEGER,
                    FOREIGN KEY (Id) REFERENCES Posts(Subreddit_id) ON DELETE CASCADE
                );
            ''')

            cursor.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_names[2]} (
                    Id TEXT PRIMARY KEY,
                    Author TEXT,
                    Created TEXT,
                    Submission_id TEXT NOT NULL ON CONFLICT IGNORE,
                    Text_content TEXT NOT NULL ON CONFLICT IGNORE,
                    Num_replies INTEGER,
                    Score INTEGER,
                    Stickied INTEGER,
                    Distinguished TEXT,
                    FOREIGN KEY (Submission_id) REFERENCES Posts(id) ON DELETE CASCADE
                );
            ''')

            cursor.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_names[3]} (
                    Id TEXT PRIMARY KEY,
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
                );
            ''')
            # Indexes. Might need to add more if I find out certain queries are repeatable
            cursor.execute('CREATE INDEX idx_Posts_Subreddit_id ON Posts(Subreddit_id);')
            cursor.execute('CREATE INDEX idx_Comments_Submission_id ON Comments(Submission_id);')
            cursor.execute('CREATE INDEX idx_Replies_Submission_id ON Replies(Submission_id);')
            cursor.execute('CREATE INDEX idx_Replies_Parent_id ON Replies(Parent_id);')
            cursor.execute('COMMIT;')
            
        except sqlite3.Error as e:
            print("SQLite error:", e.__class__.__name__, "\n", e)
            cursor.execute("ROLLBACK;")
        except Exception as e:
            print("Error:", e.__class__.__name__, "\n", e)
            cursor.execute("ROLLBACK;")
            
def process_post_batch(posts: List[praw.models.Submission],
                       comments_limit: int,
                       replies_limit: int) -> Tuple[Set[Tuple], Set[Tuple], Set[Tuple], Set[Tuple]]:
    '''
    Processes a batch of Reddit posts extracting crucial data.
    Collects information from posts and subreddits,
    then sends related comments to these posts and replies to these comments to process_comments() where more data is gathered.
    
    Notes:
        - Function handles almost all duplicates with use of sets.
            There are cases where the same subreddit returns different information from different snapshots.
    '''
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

def process_comments(post: praw.models.Submission, comments_limit: int, replies_limit: int) -> Tuple[Set[Tuple], Set[Tuple]]:
    '''
    Processes comments and replies to these comments to gather data.
    
    Parameters:
        post (praw.models.Submission): Reddit post object from which comments and replies will be extracted.
        comments_limit (int): Limit of how many comments process in current post. Will return less if less are available.
        replies_limit (int): Limit of how many replies process to fetched comments. Will return less if less are available.
        
    Notes:
        - Some parent_id are created using methods on parent object, not method .parent_id which returns with prefixes.
    '''
    comment_data = set()
    reply_data = set()
   
    for comment in post.comments[:comments_limit]:
        if isinstance(comment, praw.models.MoreComments):
            break
        
        batch_replies = set()
        num_replies = 0 
        for reply in comment.replies[:replies_limit]:
            # Nesting loop inside a loop and not separate function because of joblib.Parallel:
            # Cannot pass unserializable objects as function parameters
            if isinstance(reply, praw.models.MoreComments):
                break
            
            batch_replies.add((
                reply.id,
                reply.author.name if reply.author else None,
                datetime.fromtimestamp(reply.created_utc),
                post.id,
                comment.id,
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

def batch_generator(iterable: Iterable, batch_size: int) -> Generator:
    '''
    Generator to yield batches of items from an iterable.
    '''
    batch = []
    for item in iterable:
        batch.append(item)
        if len(batch) == batch_size:
            yield batch
            batch = []
    if batch:
        yield batch

def fill_tables(query: str,
                posts_limit: int = 100,
                comments_limit: int = 50,
                replies_limit: int = 20,
                conn: sqlite3.Connection = None,
                cursor: sqlite3.Cursor = None,
                reddit: praw.reddit.Reddit = None,
                table_names: Tuple[str, str, str, str] = table_names) -> None:
    '''
    Gathers data from Reddit API by making requests and inserts it into SQL tables.
    
    Parameters:
        query (str): Search query used to extract posts sorted by highest relevance to this query.
            Can use boolean logic supported by Reddit in searches.
        posts_limit (int): Limit of posts retrieved. Default 100.
        comments_limit (int): Limit of comments retrieved from posts. Default 50.
        replies_limit (int): Limit of replies retrieved from comments. Default 20.
        conn (sqlite3.Connection): SQLite database connection.
        cursor (sqlite3.Cursor): SQLite database cursor.
        reddit (praw.reddit.Reddit): Reddit object needed to make requests to API.
        table_names (Tuple[str, str, str, str]): Table names, assumed order: Posts, Subreddits, Comments, Replies.
    
    Notes:
        - Uses INSERT OR IGNORE to fill data because of existance of different snapshots of the same object.
            Primary key constraint helps here to avoid duplicates.
        - Distributes processing to maximum number of available cores.
        - Function will try to use global objects if none provided: conn, cursor, reddit.
        - Higher values for limit parameters (especially posts_limit) will result in longer processing.
            Function won't try to return more than 10000 posts to avoid hitting rate limit of API.
    '''
    if conn is None:
        conn = globals()['conn']
    if cursor is None:
        cursor = globals()['cursor']
    if reddit is None:
        reddit = globals()['reddit']
    
    native_limit = 100 # current limit in Reddit API
    comments_limit = min(comments_limit, 100) # max 100
    replies_limit = min(replies_limit, 100) # max 100
    posts_limit = min(posts_limit, 10000)
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
                after_param = last_post_id

            search_results = list(reddit.subreddit('all').search(query,
                                                                 limit=native_limit if i != div else mod,
                                                                 params={'after': after_param}))
            
            result_collection = chain(result_collection, search_results)
            last_post_id = search_results[-1].id

    else:
        result_collection = reddit.subreddit('all').search(query, limit=posts_limit)

    results = process_post_batch(result_collection, comments_limit, replies_limit)
    # Process posts in batches
    #batch_size = (posts_limit // os.cpu_count()) + 1
    #results = Parallel(n_jobs=-1)(delayed(process_post_batch)(batch, comments_limit, replies_limit) for batch in batch_generator(result_collection, batch_size))
    post_data = results[0]
    subreddit_data = results[1]
    comment_data = results[2]
    reply_data = results[3]
        
    insert_posts_query = f'INSERT OR IGNORE INTO {table_names[0]} ({", ".join(post_columns_local)}) VALUES ({", ".join(["?"] * len(post_columns_local))})'
    insert_subreddits_query = f'INSERT OR IGNORE INTO {table_names[1]} ({", ".join(subreddit_columns_local)}) VALUES ({", ".join(["?"] * len(subreddit_columns_local))})'
    insert_comments_query = f'INSERT OR IGNORE INTO {table_names[2]} ({", ".join(comment_columns_local)}) VALUES ({", ".join(["?"] * len(comment_columns_local))})'
    insert_replies_query = f'INSERT OR IGNORE INTO {table_names[3]} ({", ".join(reply_columns_local)}) VALUES ({", ".join(["?"] * len(reply_columns_local))})'
    
    cursor.execute("BEGIN TRANSACTION;")
    try:
        cursor.executemany(insert_posts_query, post_data)
        cursor.executemany(insert_subreddits_query, subreddit_data)
        cursor.executemany(insert_comments_query, comment_data)
        cursor.executemany(insert_replies_query, reply_data)
        cursor.execute("COMMIT;")
        
    except sqlite3.Error as e:
        print("SQLite error:", e.__class__.__name__, "\n", e)
        cursor.execute("ROLLBACK;")
    except Exception as e:
        print("Error:", e.__class__.__name__, "\n", e)
        cursor.execute("ROLLBACK;")
        
def sanitize_file_name(filename: str, character_limit: int = 50) -> str:
    '''
    Cleans file name in order to assure file creation.
    
    Parameters:
        filename (str): String of file name.
        character_limit (int): Limiting the length of file name. Default limit: 50.
    '''
    if not filename:
        raise ValueError("File name cannot be empty")
    filename = re.sub(r'[^\w\.-]', '', filename)
    filename = filename[:character_limit]
    return filename

def main(): # for testing
    # Sorting out paths
    current_dir = os.path.dirname(os.path.realpath(__file__)) # PyScripts directory path
    data_directory_name = "Data"
    config_files_directory_name = "config" # where private.json is stored
    data_dir = os.path.join(current_dir, '..', data_directory_name) # assuming Data and PyScripts are both in main
    private_path = os.path.join(data_dir, config_files_directory_name, "private.json")
    
    # Inputs
    password = input("Provide a password to reddit API: ")
    database_name = f'{sanitize_file_name(input("Provide database name: "))}.db'
    query = input("Provide query for search: ")
    
    reddit = reddit_object(private_path=private_path, password=password)
    conn, cursor = start_connection(database_path=os.path.join(data_dir, database_name))
    prepare_database(conn=conn, cursor=cursor)
    print(f'Inserting values into database {database_name}...')
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
    
    # To do:
    #   set posts/comments/replies limits to be either as a presets in file inside 'config', or UI sliders
    #   Might need to cast information gathered from posts/subreddits/comments/replies to correct data type
    #       to avoid deserialization errors if they occur.
    #   Got rid of Parallel functionality from joblib for now. Causes unserializable errors. Possible solutions:
    #       Add logging
    #       debug with pdb
    #       Inspect objects (results of methods on reddit objects)
    #       Find different way to optimize loop of process_posts_batch()