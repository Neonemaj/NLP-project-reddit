import praw
from cryptography.fernet import Fernet
import base64
import json
import time # for timing experiments
from typing import Tuple, List, Any, Generator, Iterable
from itertools import chain
from joblib import Parallel, delayed
import os
import re
import sqlite3

post_columns_global = ['Id', 'Title', 'Author', 'Author_flair', 'Created', 'Text',
                'Text_content', 'Num_comments', 'Score', 'Upvote_ratio', 'Stickied', 'Distinguished', 'URL']
comment_columns_global = ['Id', 'Author', 'Created', 'Submission_id', 'Text_content', 'Num_replies', 'Score', 'Stickied', 'Distinguished']
reply_columns_global = ['Id', 'Author', 'Created', 'Submission_id', 'Parent_id', 'Text_content', 'Score', 'Stickied', 'Distinguished']
table_names = ('Posts', 'Comments', 'Replies')

#api limits
max_posts_per_request = 100 # current praw limit
api_requests_limit = 600 # current limit of api requests in one time frame.
praw_posts_limit = (api_requests_limit * max_posts_per_request + 1) // (max_posts_per_request + 1)

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
        user_agent = "nlp_project:v0.1 (by u/Neoncodemaj)"
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


def prepare_database(cursor: sqlite3.Cursor=None, table_names: Tuple[str, str, str]=table_names) -> None:
    '''
    Creates 3 tables and 3 additional indexes (beyond 3 default ones) in current connection.
    
    Parameters:
        conn (sqlite3.Connection): SQLite database connection.
        cursor (sqlite3.Cursor): SQLite database cursor.
        table_names (Tuple[str, str, str, str]): Tuple of 4 table names.
    
    Notes:
        - Function modifies database in-place, does not return anything.
        - Because of hard-coded column names, expected order of tables is: Posts, Comments, Replies.
        - Function will atempt to use global conn and cursor objects if none were provided within parameters.
        - If tables with table_names (at least one) already exist in this database, function will wipeout all tables before proceeding.
    '''   
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
                    Created INTEGER,
                    Text INTEGER,
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
                    Author TEXT,
                    Created INTEGER,
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
                CREATE TABLE IF NOT EXISTS {table_names[2]} (
                    Id TEXT PRIMARY KEY,
                    Author TEXT,
                    Created INTEGER,
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
        
def praw_get_data(query: str,
                  posts_limit: int = 100,
                  reddit: praw.reddit.Reddit = None) -> List:
    '''
    Searches all subreddits by relevance for submitted query and retrieves 'posts_limit' number of posts in a list of praw objects.
    '''
    if posts_limit > max_posts_per_request:
        div, mod = divmod(posts_limit, max_posts_per_request)
        last_post_id = None
        after_param = None
        result_collection = chain()
        for i in range(div+1):
            if i == div and mod == 0:
                break
            if last_post_id:
                after_param = last_post_id

            search_results = list(reddit.subreddit('all').search(query,
                                                                 limit=max_posts_per_request if i != div else mod,
                                                                 params={'after': after_param}))
            
            result_collection = chain(result_collection, search_results)
            last_post_id = search_results[-1].id

    else:
        result_collection = reddit.subreddit('all').search(query, limit=posts_limit)

    return list(result_collection)
        
def process_posts(posts_batch: List[praw.models.Submission],
                  comments_limit: int,
                  replies_limit: int) -> List[List[List[Any]]]:
    '''
    Processes a batch of Reddit posts extracting crucial data.
    Collects information from posts, comments to these posts, and replies to these comments (only 1st level of depth).
    '''
    comment_data = []
    reply_data = []
    post_data = []

    for post in posts_batch:
        post_data.append([
            post.id,
            post.title,
            post.author.name if post.author else None,
            post.author_flair_text,
            post.created_utc,
            int(post.is_self),
            post.selftext,
            post.num_comments,
            post.score,
            post.upvote_ratio,
            post.stickied,
            post.distinguished,
            post.url
        ])
        
        batch_comments = []
        for comment in post.comments[:comments_limit]:
            if isinstance(comment, praw.models.MoreComments):
                break
            
            batch_replies = []
            num_replies = 0 
            for reply in comment.replies[:replies_limit]:
                if isinstance(reply, praw.models.MoreComments):
                    break
                
                batch_replies.append([
                    reply.id,
                    reply.author.name if reply.author else None,
                    reply.created_utc,
                    post.id,
                    comment.id,
                    reply.body,
                    reply.score,
                    reply.stickied,
                    reply.distinguished
                ])
                num_replies += 1
            reply_data.extend(batch_replies)
            
            batch_comments.append([
                comment.id,
                comment.author.name if comment.author else None,
                comment.created_utc,
                post.id,
                comment.body,
                num_replies,
                comment.score,
                comment.stickied,
                comment.distinguished
            ])
        comment_data.extend(batch_comments)   

    return [post_data, comment_data, reply_data]
    
def fill_tables(cursor: sqlite3.Cursor,
                reddit: praw.reddit.Reddit,
                query: str,
                posts_limit: int,
                comments_limit: int,
                replies_limit: int,
                table_names: Tuple[str, str, str]) -> None:
    '''
    Gathers data from Reddit API by making requests and inserts it into SQL tables.
    
    Parameters:
        cursor (sqlite3.Cursor): SQLite database cursor.
        reddit (praw.reddit.Reddit): Reddit object needed to make requests to API.
        query (str): Search query used to extract posts sorted by highest relevance to this query.
            Can use boolean logic supported by Reddit in searches.
        posts_limit (int): Limit of posts retrieved. Won't exceed global variable praw_posts_limit.
        comments_limit (int): Limit of comments retrieved from posts. Maximum 100.
        replies_limit (int): Limit of replies retrieved from comments. Maximum 100.
        table_names (Tuple[str, str, str, str]): Table names, assumed order: Posts, Subreddits, Comments, Replies.
    
    Notes:
        - Uses INSERT OR IGNORE to fill data because of existance of different snapshots of the same object.
            Primary key constraint helps here to avoid duplicates.
        - Function will utilitize maximum number of available cpu cores.
    '''
    post_columns_local = post_columns_global
    comment_columns_local, reply_columns_local = comment_columns_global, reply_columns_global
    posts_limit = min(praw_posts_limit, posts_limit)
    result_collection = praw_get_data(query=query,
                                      posts_limit=posts_limit,
                                      reddit=reddit)
    
    batch_size = (posts_limit // (2 * os.cpu_count())) + 1
    results = Parallel(n_jobs=-1)(delayed(process_posts)(posts_batch, comments_limit, replies_limit) for posts_batch in batch_generator(result_collection, batch_size))
    aggregated_posts = []
    aggregated_comments = []
    aggregated_replies = []

    for post_list, comment_list, reply_list in results:
        aggregated_posts.extend(post_list)
        aggregated_comments.extend(comment_list)
        aggregated_replies.extend(reply_list)
    
    insert_posts_query = f'INSERT OR IGNORE INTO {table_names[0]} ({", ".join(post_columns_local)}) VALUES ({", ".join(["?"] * len(post_columns_local))})'
    insert_comments_query = f'INSERT OR IGNORE INTO {table_names[1]} ({", ".join(comment_columns_local)}) VALUES ({", ".join(["?"] * len(comment_columns_local))})'
    insert_replies_query = f'INSERT OR IGNORE INTO {table_names[2]} ({", ".join(reply_columns_local)}) VALUES ({", ".join(["?"] * len(reply_columns_local))})'
    
    cursor.execute("BEGIN TRANSACTION;")
    try:
        cursor.executemany(insert_posts_query, aggregated_posts)
        cursor.executemany(insert_comments_query, aggregated_comments)
        cursor.executemany(insert_replies_query, aggregated_replies)
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
    prepare_database(cursor=cursor)
    print(f'Inserting values into database {database_name}...')
    start_time = time.time()
    fill_tables(cursor=cursor,
                reddit=reddit,
                query=query,
                posts_limit=300,
                comments_limit=50,
                replies_limit=20,
                table_names=table_names)
    end_time = time.time()
    print(f'Operation complete, database filled.\nTime taken: {end_time - start_time:.4f} seconds.')
    conn.close()
    
if __name__ == "__main__":
    main()
    
    # To do:
    #   Implement Logging.