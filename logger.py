import sqlite3
import pickle
import os
from .progsnap import PS2

def get(json_obj, key, default=None):
    if key in json_obj:
        return json_obj[key]
    else:
        return default

CODE_STATES_TABLE = 'CodeStates'
MAIN_TABLE = 'MainTable'
METADATA_TABLE = 'DatasetMetadata'
PROBLEM_TABLE = 'LinkProblem'
SUBJECT_TABLE = 'LinkSubject'

CODE_STATES_TABLE_COLUMNS = {
    'CodeStateID': 'INTEGER PRIMARY KEY',
    'Code': 'TEXT',
}

MAIN_TABLE_COLUMNS = {
    'EventID': 'INTEGER PRIMARY KEY',
    'SubjectID': 'TEXT',
    'ProblemID': 'TEXT',
    'AssignmentID': 'TEXT',
    'EventType': 'TEXT',
    'CodeStateID': 'INTEGER',
    'ClientTimestamp': 'TEXT',
    'ServerTimestamp': 'TEXT',
    'Score': 'REAL',
}

METADATA_TABLE_COLUMNS = {
    'Property': 'TEXT',
    'Value': 'TEXT',
}

PROBLEM_TABLE_COLUMNS = {
    'ProblemID': 'TEXT PRIMARY KEY',
    'StarterCode': 'TEXT',
    'Subgoals': 'TEXT',
}

SUBJECT_TABLE_COLUMNS = {
    'SubjectID': 'TEXT PRIMARY KEY',
    'IsInterventionGroup': 'INTEGER',
}


class SQLiteLogger:
    """
    A work-in-progress SQLite logging class that creates a ProgSnap2-formatted
    database. Supports basic logging and updating of data. Does not support
    the whole PS2 specification yet.
    """

    def __init__(self, db_path):
        dirname = os.path.dirname(os.path.abspath(db_path))
        os.makedirs(dirname, exist_ok=True)
        self.db_path = db_path
        self.create_tables()

    # TODO: This should support batch operations but currently does not
    def _connect(self):
        return sqlite3.connect(self.db_path)

    def _create_table(self, table_name, column_map):
        column_text = [f"`{k}` {v}" for k, v in column_map.items()]
        with self._connect() as conn:
            c = conn.cursor()
            c.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({','.join(column_text)})")
            conn.commit()

    def create_tables(self):
        self._create_table(MAIN_TABLE, MAIN_TABLE_COLUMNS)
        self._create_table(CODE_STATES_TABLE, CODE_STATES_TABLE_COLUMNS)
        # Not actually used, but helpful to have for clean loading
        self._create_table(METADATA_TABLE, METADATA_TABLE_COLUMNS)
        self.__add_metadata()
        self._create_table(PROBLEM_TABLE, PROBLEM_TABLE_COLUMNS)
        self._create_table(SUBJECT_TABLE, SUBJECT_TABLE_COLUMNS)
        self.__add_code_index()

    def __add_code_index(self):
        with self._connect() as conn:
            c = conn.cursor()
            c.execute(f"CREATE INDEX IF NOT EXISTS idx_Code ON {CODE_STATES_TABLE} (Code)")
            conn.commit()

    def __add_metadata(self):
        # get the number of rows in the metadata table
        with self._connect() as conn:
            c = conn.cursor()
            c.execute(f"SELECT COUNT(*) FROM {METADATA_TABLE}")
            count = c.fetchone()[0]
            if count != 0:
                return

        metadata_map = {
            'Version': '8.0',
            'IsEventOrderingConsistent': 1,
            'EventOrderScope': 'Global',
            'EventOrderScopeColumns': '',
            'CodeStateRepresentation': 'Sqlite',
        }
        for key in metadata_map:
            self.__insert_map(METADATA_TABLE, {
                'Property': key,
                'Value': metadata_map[key]
            })

    def __insert_map(self, table_name, column_map):
        columns = '`' + '`,`'.join(column_map.keys()) + '`'
        values = ','.join(['?'] * len(column_map))
        id = None
        with self._connect() as conn:
            c = conn.cursor()
            query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
            # print(query)
            c.execute(query, tuple(column_map.values()))
            id = c.lastrowid
            conn.commit()
        return id

    def clear_table(self, table_name):
        with self._connect() as conn:
            c = conn.cursor()
            c.execute(f"DELETE FROM {table_name}")
            conn.commit()

    def execute_query(self, query, args) -> list[any]:
        with self._connect() as conn:
            c = conn.cursor()
            c.execute(query, args)
            conn.commit()
            return c.fetchall()

    def __get_codestate_id(self, code_state):
        # TODO: This could be more efficient and concurrency-safe
        # using INSERT OR IGNORE, with a UNIQUE Code column, but
        # I'm keeping it this way for now for backwards compatibility
        result = None
        with self._connect() as conn:
            c = conn.cursor()
            c.execute(f"SELECT CodeStateID FROM {CODE_STATES_TABLE} WHERE Code = ?", (code_state,))
            result = c.fetchone()
        if result is None:
            return self.__insert_map(CODE_STATES_TABLE, {'Code': code_state})
        return result[0]

    def log_event(self, event_type, row_dict):
        """Logs an event to the MainTable with column values given in the row_dict.
        """
        code_state = get(row_dict, 'CodeState')
        code_state_id = self.__get_codestate_id(code_state)
        main_table_map = {
            PS2.EventType: event_type,
            PS2.CodeStateID: code_state_id,
            # I haven't gotten order to work, but it's optional so ignoring
            # "Order": f"(SELECT IFNULL(MAX(`Order`), 0) + 1 FROM {MAIN_TABLE})"
        }
        for key in MAIN_TABLE_COLUMNS:
            if key in main_table_map:
                continue
            main_table_map[key] = get(row_dict, key)
        del main_table_map[PS2.EventID]
        # print (main_table_map)
        self.__insert_map(MAIN_TABLE, main_table_map)

    def get_starter_code(self, problem_id):
        with self._connect() as conn:
            c = conn.cursor()
            c.execute(f"SELECT StarterCode FROM {PROBLEM_TABLE} WHERE ProblemID = ?", (problem_id,))
            result = c.fetchone()
            if result is None:
                return None
            return result[0]

    def set_starter_code(self, problem_id, starter_code):
        with self._connect() as conn:
            c = conn.cursor()
            query = f"INSERT OR IGNORE INTO {PROBLEM_TABLE} (ProblemID) VALUES (?);"
            c.execute(query, (problem_id,))
            query = f"UPDATE {PROBLEM_TABLE} SET StarterCode = ? WHERE ProblemID = ?;"
            c.execute(query, (starter_code, problem_id))
            conn.commit()

    def get_or_set_subject_condition(self, subject_id, condition_to_set):
        if subject_id is None:
            return condition_to_set
        with self._connect() as conn:
            c = conn.cursor()
            c.execute(f"SELECT IsInterventionGroup FROM {SUBJECT_TABLE} WHERE SubjectID = ?", (subject_id,))
            result = c.fetchone()
            if result is None:
                self.__insert_map(SUBJECT_TABLE, {
                    'SubjectID': subject_id,
                    'IsInterventionGroup': condition_to_set,
                })
                return condition_to_set
            return result[0]
