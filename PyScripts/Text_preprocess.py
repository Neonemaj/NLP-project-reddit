import sqlite3
import spacy
import os
import re
from typing import Dict, List, Tuple
from collections import OrderedDict

# config
table_names = ('Posts', 'Subreddits', 'Comments', 'Replies')
nlp_model = spacy.load('en_core_web_md')

def check_column_exist(cursor: sqlite3.Cursor, check_dict: Dict[str, List[str]]) -> bool:
    '''
    Checks if all given columns exist in the corresponding tables.

    Parameters:
        cursor (sqlite3.Cursor): SQLite cursor object for executing SQL queries.
        check_dict (Dict[str, List[str]]): Dictionary where keys are table names
            and values are lists of column names to check.

    Returns:
        bool: True if all columns exist in the tables, False otherwise.

    Example:
        check_column_exist(cursor, {
            'table1': ['column1', 'column2'],
            'table2': ['column3'],
            'table3': []
        })
    '''
    for table_name, column_list in check_dict.items():
        for column_name in column_list:
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
        conn.execute("SELECT REGEX_REPLACE('test', 'est', 'ry')")
        result = cursor.fetchall()
        assert result == [('try',)], (
            "REGEX_REPLACE exists in this connection, but provided unexpected output.\n"
            "Correct order of parameters: 'text', 'pattern', 'replacement'")

    except sqlite3.OperationalError as e:
        if "no such function: REGEX_REPLACE" in str(e):
            conn.create_function('REGEX_REPLACE', 3, regex_replace)

def preprocess_tables_text(conn: sqlite3.Connection, cursor: sqlite3.Cursor) -> None:
    '''
    Cleans tables: Posts, Subreddits, Comments, Replies in the SQLite database.

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
    
    local_table_names = table_names # assuming order: Posts, Subreddits, Comments, Replies
    column_names_ordered = [['Text_content'], ['Description', 'Public_description'], ['Text_content'], ['Text_content']]
    table_columns_dict = {table_names[i]:column_names_ordered[i] for i in range(len(table_names))}
    assert check_column_exist(cursor, table_columns_dict), f'One or more columns were not found in tables\ntable:columns dict -> {table_columns_dict}'
    
    create_regex_replace(conn=conn, cursor=cursor)

    try:
        # Posts table
        cursor.execute("""
            UPDATE {}
            SET Text_content = NULL
            WHERE Text_content IN ('[deleted]', '[removed]', '');
        """.format(local_table_names[0]))

        cursor.execute("""
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
                                        "http\S+|www\S+|\S*\.com\S*|<.*?>|(?![\u2019\n])[^ -~]|[:;]-?(?:[a-zA-Z]+|[0-9]+)|x200B.", ""),
                                    "’", "'"),
                                "(?<![\.\s])\n+", ". "),
                            "\n+", " "), 
                        "[^0-9A-Za-z()\\'?!:,. +\\-\\x22]", ""), 
                    "([.!?,-])\\1+", "\\1"), 
                "\s{{2,}}", " ")
            );
        """.format(local_table_names[0])) # since I am formating this way, I have to escape curly brackets inside by doubling them

        cursor.execute("""
            UPDATE {}
            SET Text_content = NULL
            WHERE Text_content = '';
        """.format(local_table_names[0]))

        cursor.execute("""
            DELETE FROM {}
            WHERE Num_comments = 0 AND Text_content IS NULL;
        """.format(local_table_names[0]))
        
        # Subreddits
        cursor.execute("""
            UPDATE {}
            SET Description = NULL
            WHERE Description IN ('[deleted]', '[removed]', '');
        """.format(local_table_names[1]))

        cursor.execute("""
            UPDATE {}
            SET Public_description = NULL
            WHERE Public_description IN ('[deleted]', '[removed]', '');
        """.format(local_table_names[1]))

        cursor.execute("""
            UPDATE {}
            SET Description = TRIM(
            REGEX_REPLACE(
                REGEX_REPLACE(
                    REGEX_REPLACE(
                        REGEX_REPLACE(
                            REGEX_REPLACE(
                                REGEX_REPLACE(
                                    REGEX_REPLACE(
                                        Description,
                                        "http\S+|www\S+|\S*\.com\S*|<.*?>|(?![\u2019\n])[^ -~]|[:;]-?(?:[a-zA-Z]+|[0-9]+)|x200B.", ""),
                                    "’", "'"),
                                "(?<![\.\s])\n+", ". "),
                            "\n+", " "), 
                        "[^0-9A-Za-z()\\'?!:,. +\\-\\x22]", ""), 
                    "([.!?,-])\\1+", "\\1"), 
                "\s{{2,}}", " ")
                ),
            Public_description = TRIM(
            REGEX_REPLACE(
                REGEX_REPLACE(
                    REGEX_REPLACE(
                        REGEX_REPLACE(
                            REGEX_REPLACE(
                                REGEX_REPLACE(
                                    REGEX_REPLACE(
                                        Public_description,
                                        "http\S+|www\S+|\S*\.com\S*|<.*?>|(?![\u2019\n])[^ -~]|[:;]-?(?:[a-zA-Z]+|[0-9]+)|x200B.", ""),
                                    "’", "'"),
                                "(?<![\.\s])\n+", ". "),
                            "\n+", " "), 
                        "[^0-9A-Za-z()\\'?!:,. +\\-\\x22]", ""), 
                    "([.!?,-])\\1+", "\\1"), 
                "\s{{2,}}", " ")
                );
        """.format(local_table_names[1]))

        cursor.execute("""
            UPDATE {}
            SET Description = NULL
            WHERE Description = '';
        """.format(local_table_names[1]))

        cursor.execute("""
            UPDATE {}
            SET Public_description = NULL
            WHERE Public_description = '';
        """.format(local_table_names[1]))

        # Comments 
        cursor.execute("""
            UPDATE {}
            SET Text_content = NULL
            WHERE Text_content IN ('[deleted]', '[removed]', '');
        """.format(local_table_names[2]))

        cursor.execute("""
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
                                        "http\S+|www\S+|\S*\.com\S*|<.*?>|(?![\u2019\n])[^ -~]|[:;]-?(?:[a-zA-Z]+|[0-9]+)|x200B.", ""),
                                    "’", "'"),
                                "(?<![\.\s])\n+", ". "),
                            "\n+", " "), 
                        "[^0-9A-Za-z()\\'?!:,. +\\-\\x22]", ""), 
                    "([.!?,-])\\1+", "\\1"), 
                "\s{{2,}}", " ")
            );
        """.format(local_table_names[2]))

        cursor.execute("""
            UPDATE {}
            SET Text_content = NULL
            WHERE Text_content = '';
        """.format(local_table_names[2]))

        cursor.execute("""
            DELETE FROM {}
            WHERE Num_replies = 0 AND Text_content IS NULL;
        """.format(local_table_names[2]))
        
        # Replies
        cursor.execute("""
            UPDATE {}
            SET Text_content = NULL
            WHERE Text_content IN ('[deleted]', '[removed]', '');
        """.format(local_table_names[3]))

        cursor.execute("""
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
                                        "http\S+|www\S+|\S*\.com\S*|<.*?>|(?![\u2019\n])[^ -~]|[:;]-?(?:[a-zA-Z]+|[0-9]+)|x200B.", ""),
                                    "’", "'"),
                                "(?<![\.\s])\n+", ". "),
                            "\n+", " "), 
                        "[^0-9A-Za-z()\\'?!:,. +\\-\\x22]", ""), 
                    "([.!?,-])\\1+", "\\1"), 
                "\s{{2,}}", " ")
            );
        """.format(local_table_names[3]))

        cursor.execute("""
            UPDATE {}
            SET Text_content = NULL
            WHERE Text_content = '';
        """.format(local_table_names[3]))

        cursor.execute("""
            DELETE FROM {}
            WHERE Text_content IS NULL;
        """.format(local_table_names[3]))
        
        conn.commit()
    
    # Error handling and rollback is not necessary in sqlite, since it won't commit changes if there will be error in execution.
    # Writing error handling just for practice.
    except sqlite3.OperationalError as e:
        print("An operational error occurred:", e)
        conn.rollback()

    except sqlite3.IntegrityError as e:
        print("An integrity error occurred:", e)
        conn.rollback()

    except sqlite3.DatabaseError as e:
        print("A database error occurred:", e)
        conn.rollback()
        
    except Exception as e:
        print("An unexpected error occurred:", e)
        conn.rollback()
        
def count_entities(text_content: List[str],
                   nlp_model: spacy.lang.en.English,
                   entity_count: Dict[str, int],
                   text_content_count: int) -> Tuple[Dict[str, int], int]:
    '''
    Counts entities recognized by SpaCy in provided list of texts.
    
    Parameters:
        text_content (List[str]): List of texts to analyze. Usually contents of a column from sql table.
        nlp_model (spacy.lang.en.English): Instance of SpaCy english model used for detecting and labeling entities.
        entity_count (Dict[str, int]): Dictionary of entities: number of occurences, that will be updated adding new counts.
        text_content_count (int): Number of already analyzed text blocks that will be updated adding new values.
        
    Returns:
        Tuple[Dict[str, int], int]: Tuple of [0]: updated entity_count and [1]: updated number of analyzed blocks.
        
    Example:
        count_entities(text_content=['Bach', 'Mozart 42'],
                       nlp_model=nlp_model,
                       entity_count={'Bach': 3, 'Buxtehude': 1},
                       text_content_count=6) ->
        ({'Bach': 4, 'Buxtehude': 1, 'Mozart': 1}, 8)
        
    Notes:
        - SpaCy language model is supposed to omit entities with labels: ['DATE', 'TIME', 'MONEY', 'PERCENT', 'CARDINAL', 'ORDINAL', 'QUANTITY'].
        - Function is adding cumulatively lemmatized form of entities.
    '''
    for text in text_content:
        if text == None:
            continue
        text_content_count += 1
        doc = nlp_model(text)
        for entity in doc.ents:
            if entity.label_ in ['DATE', 'TIME', 'MONEY', 'PERCENT', 'CARDINAL', 'ORDINAL', 'QUANTITY']:
                continue
            elif entity.lemma_ not in entity_count.keys():
                entity_count[entity.lemma_] = 1
            else:
                entity_count[entity.lemma_] += 1
                
    return entity_count, text_content_count

def count_entities_from_tables(cursor: sqlite3.Cursor,
                               nlp_model: spacy.lang.en.English,
                               table_names: Tuple) -> Tuple[OrderedDict[str, int], int]:
    '''
    Counts valid entities from given tables and number of analyzed text blocks.
    
    Parameters:
        cursor (sqlite3.Cursor): Sqlite Cursor object needed to retrieve text contents from sql tables.
        nlp_model (spacy.land.en.English): Instance of SpaCy english model used for detecting and labeling entities.
        table_names (Tuple): Tuple of table names with text column to analyze.
        
    Returns:
        Tuple[Dict[str, int], int]: Tuple of a [0]: SortedDictionary (descending) with keys as entities and
            values - number of occurences of this entity, and [1]: number of analyzed text blocks throughout tables.
            
    Example:
        count_entities_from_tables(cursor=cursor,
                                   nlp_model=nlp_model,
                                   table_names=(Posts, Subreddits, Comments, Replies))
                                   
    Notes:
        - Assumed order of tables: Posts, Subreddits, Comments, Replies.
        - Assumed columns with text_contents with matching order: [['Text_content'], ['Public_description'], ['Text_content'], ['Text_content']]
    '''
    column_names_ordered = [['Text_content'], ['Public_description'], ['Text_content'], ['Text_content']]
    table_columns_dict = {table_names[i]:column_names_ordered[i] for i in range(len(table_names))}
    assert check_column_exist(cursor, table_columns_dict), f'One or more columns were not found in tables\ntable:columns dict: {table_columns_dict}'
    
    entity_count = dict()
    text_content_count = 0
    for table_name, column_list in table_columns_dict.items():
        column_name = column_list[0]
        cursor.execute(f"SELECT {column_name} FROM {table_name};")
        text_content = [text[0] for text in cursor.fetchall()]
        entity_count, text_content_count = count_entities(text_content=text_content,
                                                          nlp_model=nlp_model,
                                                          entity_count=entity_count,
                                                          text_content_count=text_content_count)
 
    return OrderedDict(sorted(entity_count.items(), key=lambda x: x[1], reverse=True)), text_content_count

def main(): # testing text cleaning and entity counting
    # Connection and paths
    current_dir = os.path.dirname(os.path.realpath(__file__)) # PyScripts directory path
    data_directory_name = "Data"
    data_dir = os.path.join(current_dir, '..', data_directory_name) # assuming Data and PyScripts are both in main
    database_name = 'iphone11.db'
    database_path = os.path.join(data_dir, database_name)
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()

    # Text cleaning
    preprocess_tables_text(conn=conn, cursor=cursor)

    # Count entities
    entity_count, text_blocks_count = count_entities_from_tables(cursor=cursor,
                                                                 nlp_model=nlp_model,
                                                                 table_names=table_names)
    num_showed: int = 10
    print(f'Analyzed {text_blocks_count} text blocks.')
    print(f'{num_showed} most common entities are: {[entity for entity in list(entity_count.keys())[:num_showed]]}')
    conn.close()
    
if __name__ == "__main__":
    main()
    
    # If I won't use SpaCy for any text cleaning/preprocessing in the future, then I might move spacy related functions out to different script,
    # to keep this one solely related to cleaning.
    # 
    # Consider addressing case sensitivity in entity counting.
