import sqlite3
import spacy
from spacy.lang.en.stop_words import STOP_WORDS
from string import punctuation
import os
import json
import re
from typing import Dict, List, Tuple

# config
# if table_names will be parameterized, ensure its sanitazed to prevent dangerous injection to sql
table_names = ('Posts', 'Comments', 'Replies')
text_content_column_name = 'Text_content'
table_columns_dict = {table:text_content_column_name for table in table_names}
new_columns_global = ['Raw_tokens', 'Lemma_lower_tokens', 'Lemma_lower_stop_tokens']
nlp_model = spacy.load('en_core_web_md')
STOP_WORDS_SET = set(STOP_WORDS) # for faster retrieval

def check_column_exist(cursor: sqlite3.Cursor, check_dict: Dict[str, str]) -> bool:
    '''
    Checks if a given column exist in the corresponding table.

    Parameters:
        cursor (sqlite3.Cursor): SQLite cursor object for executing SQL queries.
        check_dict (Dict[str, str]): Dictionary where keys are table names
            and values are column names to check.

    Returns:
        bool: True if all columns exist in the tables, False otherwise.

    Example:
        check_column_exist(cursor, {
            'table1': 'column1',
            'table2': 'column2',
        })
    '''
    for table_name, column_name in check_dict.items():
        cursor.execute(f"PRAGMA table_info({table_name})")
        if not any(column_name == col[1] for col in cursor.fetchall()):
            return False
    return True

def regex_replace(text: str, pattern: str, replacement: str) -> str:
    '''
    Replaces ALL text matching patterns with replacement using regular expression.
    
    Parameters:
        text (str): Text content, target replacement.
        pattern (str): RegEx pattern to match in 'text'.
        replacement (str): String replacing all matched characters in 'text'.
        
    Returns:
        str: Text content after replacing all matches with 'replacement' if found any.
        
    Notes:
        Intended to be used only as a function created in sqlite for text preprocessing with queries.
    '''
    if text is None:
        return ''
    else:
        result = re.sub(pattern, replacement, text)
        # order of parameters in regex_replace() is switched to better match REGEX_REPLACE or REGEXP_REPLACE
        # that function in other sql databases
        return result
    
def create_regex_replace(conn: sqlite3.Connection, cursor: sqlite3.Cursor) -> None:
    '''
    Checks if REGEX_REPLACE function exists and works correctly in current sqlite connection.
    
    Parameters:
        conn (sqlite3.Connection): SQLite database connection.
        cursor (sqlite3.Cursor): SQLite database cursor.
        
    Notes:
        - Function returns nothing
        - Raises AssertionError if REGEX_REPLACE works incorrectly (for example by having wrong order of parameters)
    '''
    try:
        cursor.execute("SELECT REGEX_REPLACE('test', 'est', 'ry')")
        result = cursor.fetchall()
        assert result == [('try',)], (
            "REGEX_REPLACE exists in this connection, but provided unexpected output.\n"
            "Correct order of parameters: 'text', 'pattern', 'replacement'")

    except sqlite3.OperationalError as e:
        if "no such function: REGEX_REPLACE" in str(e):
            conn.create_function('REGEX_REPLACE', 3, regex_replace)

def preprocess_tables_text(conn: sqlite3.Connection, cursor: sqlite3.Cursor) -> None:
    '''
    Cleans tables: Posts, Comments, Replies in the SQLite database.

    Steps:
    1. Creates a SQLite function 'REGEX_REPLACE' for further regex matching and replacing.
    2. Assigns NULL to any [removed], [deleted] or empty string text contents.
    3. Cleans text contents by removing URLs, HTML tags, unknown ASCII characters, emojis,
       and other characters that may not be suitable for NLP.
    4. Truncates multiple breaklines, whitespaces, punctuation marks to singular ones.
    5. Deletes entries if they do not have text information and are not necessary in relation to other tables.

    Parameters:
        conn (sqlite3.Connection): SQLite database connection.
        cursor (sqlite3.Cursor): SQLite database cursor.

    Notes:
        - Column names are not parameterized, but table names are linked to the global variable table_names.
        - The function modifies the database in-place and does not return any value.
    '''
    local_table_names = table_names # assuming order: Posts, Comments, Replies
    create_regex_replace(conn=conn, cursor=cursor)
    cursor.execute("BEGIN TRANSACTION;")
    try:
        for table_name in local_table_names:
            cursor.execute("""
                UPDATE {}
                SET Text_content = NULL
                WHERE Text_content IN ('[deleted]', '[removed]', '');
            """.format(table_name))

            cursor.execute(r"""
                UPDATE {}
                SET Text_content = TRIM(
                REGEX_REPLACE(
                    REGEX_REPLACE(
                        REGEX_REPLACE(
                            REGEX_REPLACE(
                                REGEX_REPLACE(
                                    REGEX_REPLACE(
                                        REGEX_REPLACE(
                                            Text_content,
                                            "http\S+|www\S+|\S*\.com\S*|<.*?>|(?![\u2019\n])[^ -~]|[;:](?:-)?(?:([BCDOPVXbcdopvx30\(\)\[\]/\\\\*><])\\1*)|x200B.", ""),
                                        "’", "'"),
                                    "(?<![\.\s])\n+", ". "),
                                "\n+", " "), 
                            "[^0-9A-Za-z()\\'?!:,. +\\-\\x22]", ""), 
                        "([.!?,-])\\1+", "\\1"), 
                    "\s{{2,}}", " ")
                );
            """.format(table_name))

            cursor.execute("""
                UPDATE {}
                SET Text_content = NULL
                WHERE Text_content = '';
            """.format(table_name))

        # Deletion outside the loop to check on fully processed text contents
        # Not updating Num_comments and Num_replies after deletion <- its information about raw state of post/comment
        cursor.execute("""
            DELETE FROM {}
            WHERE Num_comments = 0 AND Text_content IS NULL;
        """.format(local_table_names[0]))

        cursor.execute("""
            DELETE FROM {}
            WHERE Num_replies = 0 AND Text_content IS NULL;
        """.format(local_table_names[1]))

        cursor.execute("""
            DELETE FROM {}
            WHERE Text_content IS NULL;
        """.format(local_table_names[2]))
        
        cursor.execute("COMMIT;")
    
    except sqlite3.Error as e:
        print("SQLite error:", e.__class__.__name__, "\n", e)
        cursor.execute("ROLLBACK;")  
    except Exception as e:
        print("Error:", e.__class__.__name__, "\n", e)
        cursor.execute("ROLLBACK;")

def tokenize_and_json_serialize(nlp_model: spacy.lang.en.English, text: str) -> Tuple[str, str, str]:
    '''
    Tokenizes into 3 different lists and serializes it with json into string.
    
    Parameters:
        nlp_model (spacy.lang.en.English): Instance of SpaCy english model used for tokenizing text.
        text (str): Text that will be tokenized and serialized.
        
    Returns:
        Tuple[str, str, str]: 3 strings:
            1. Serialized raw tokens from the text.
            2. Serialized tokens that were lemmatized and lowercased.
            3. Serialized lemmatized and lowercased tokens without stop words.
    '''
    if text is None:
        return None, None, None
    doc = nlp_model(text)
    if doc.lang_ != 'en':
        return None, None, None
    raw_tokens = [token.text for token in doc]
    lemma_lower_tokens = [token.lemma_.lower() for token in doc]
    lemma_lower_stop_tokens = [token.lemma_.lower() for token in doc if token.text.lower() not in STOP_WORDS_SET and token.text not in punctuation]
    
    raw_serialized = json.dumps(raw_tokens)
    lemma_lower_serialized = json.dumps(lemma_lower_tokens)
    lemma_lower_stop_serialized = json.dumps(lemma_lower_stop_tokens)
    
    return raw_serialized, lemma_lower_serialized, lemma_lower_stop_serialized

def create_columns_insert_tokens(cursor: sqlite3.Cursor, table_name: str, serialized_values: List[Tuple[str, str, str]]) -> None:
    '''
    Creates 3 new columns for different versions of serialized tokens and populates them.
    
    Parameters:
        cursor (sqlite3.Cursor): Cursor object used for querying.
        table_name (str): Name of a table that will be altered and populated.
        serialized_values (List[Tuple[str, str, str]]): Values which will be inserted into a table.
        
    Notes:
        - Function modifies table in-place and does not return any value.
        - This function will only create missing columns <-> won't create any if there already are these 3 exact columns in a table.
        - Function overwrites values that already exist in those columns.
    '''
    new_columns = new_columns_global
    cursor.execute("BEGIN TRANSACTION;")
    try:
        cursor.execute(f"PRAGMA table_info({table_name})")
        current_column_list = [col[1] for col in cursor.fetchall()]
        for column_name in new_columns:
            if column_name in current_column_list:
                continue
            else:
                cursor.execute(f'''
                    ALTER TABLE {table_name}
                    ADD COLUMN {column_name} TEXT;
                ''')
        
        query = f'UPDATE {table_name} SET ({", ".join(new_columns)}) = ({", ".join(["?" for _ in range(len(new_columns))])}) WHERE Id = ?;'
        cursor.executemany(query, serialized_values)
        cursor.execute("COMMIT;")
        
    except sqlite3.Error as e:
        print("SQLite error:", e.__class__.__name__, "\n", e)
        cursor.execute("ROLLBACK;")
    except Exception as e:
        print("Error:", e.__class__.__name__, "\n", e)
        cursor.execute("ROLLBACK;")

def main_loop_for_tokenizing(cursor: sqlite3.Cursor,
                             nlp_model: spacy.lang.en.English,
                             table_columns_dict: Dict[str, List[str]]) -> None:
    '''
    Iterates over text columns and text blocks in given tables to unpack text for other functions.
    
    Parameters:
        cursor (sqlite3.Cursor): Sqlite Cursor object needed to retrieve text contents from sql tables.
        nlp_model (spacy.land.en.English): Instance of SpaCy english model used for inner functions to analyze text.
        table_columns_dict (Dict[str, List[str]]): Dictionary with keys as table names and values being a list of column names corresponding to this table.
                                   
    Notes:
        - Returns nothing. Gathers text blocks/columns to use for functions:
            tokenize_and_json_serialize() and create_columns_insert_tokens().
        - Raises AssertionError when the length of serialized_values do not match length of a table.
    '''
    
    for table_name, column_list in table_columns_dict.items():
        serialized_tokens = []
        column_name = column_list[0]
        cursor.execute(f"SELECT Id, {column_name} FROM {table_name};")
        fetched_results = cursor.fetchall()
        for id_key, text_block in fetched_results:
            serialized_tokens.append(tokenize_and_json_serialize(nlp_model=nlp_model, text=text_block) + (id_key,))
            
        assert len(serialized_tokens) == len(fetched_results), (
            'Mismatch of lengths between retrieved serialized tokens and table size.\n'
            f'{len(serialized_tokens) = }, {len(fetched_results) = }')
        create_columns_insert_tokens(cursor=cursor,
                                     table_name=table_name,
                                     serialized_values=serialized_tokens)
    
def main():
    # Connection and paths
    current_dir = os.path.dirname(os.path.realpath(__file__)) # PyScripts directory path
    data_directory_name = "Data"
    data_dir = os.path.join(current_dir, '..', data_directory_name) # assuming Data and PyScripts are both in main
    database_name = input('Input database name (with file extension) to start cleaning: ')
    database_path = os.path.join(data_dir, database_name)
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()

    assert check_column_exist(cursor=cursor, check_dict=table_columns_dict), (
        f'At least one table doesn\'t have text content column: {text_content_column_name}')
    
    # Text cleaning
    preprocess_tables_text(conn=conn, cursor=cursor)

    # Adding tokenized columns
    main_loop_for_tokenizing(cursor=cursor,
                             nlp_model=nlp_model,
                             table_columns_dict=table_columns_dict)

    conn.close()
    
if __name__ == "__main__":
    main()