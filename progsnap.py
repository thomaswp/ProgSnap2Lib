import pandas as pd
import os
from os import path


class PS2:
    """ A class holding constants used to get columns of a PS2 dataset
    """

    Order = 'Order'
    SubjectID = 'SubjectID'
    ToolInstances = 'ToolInstances'
    ServerTimestamp = 'ServerTimestamp'
    ServerTimezone = 'ServerTimezone'
    CourseID = 'CourseID'
    CourseSectionID = 'CourseSectionID'
    AssignmentID = 'AssignmentID'
    ProblemID = 'ProblemID'
    Attempt = 'Attempt'
    CodeStateID = 'CodeStateID'
    EventType = 'EventType'
    Score = 'Score'
    CompileResult = 'CompileResult'
    CompileMessageType = 'CompileMessageType'
    CompileMessageData = 'CompileMessageData'
    EventID = 'EventID'
    ParentEventID = 'ParentEventID'
    SourceLocation = 'SourceLocation'
    Code = 'Code'

class Metadata:
    """ A class holding constants for attributes of the metadata table
    """

    Version = 'Version'
    IsEventOrderingConsistent = 'IsEventOrderingConsistent'
    EventOrderScope = 'EventOrderScope'
    EventOrderScopeColumns = 'EventOrderScopeColumns'
    CodeStateRepresentation = 'CodeStateRepresentation'


class EventType:
    """ A class holding constants for event types
    """

    SessionStart = 'Session.Start'
    """Marks the start of a work session."""
    SessionEnd = 'Session.End'
    """Marks the end of a work session."""
    ProjectOpen = 'Project.Open'
    """Indicates that a project was opened."""
    ProjectClose = 'Project.Close'
    """Indicates that a project was closed due to an explicit user or system action.
    Data consumers should be prepared to handle cases where Project.
    Open is not terminated by an explicit Project.Close.
    """
    FileCreate = 'File.Create'
    """Indicates that a file was created."""
    FileDelete = 'File.Delete'
    """Indicates that a file was deleted."""
    FileOpen = 'File.Open'
    """Indicates that a file was opened."""
    FileClose = 'File.Close'
    """Indicates that a file was closed."""
    FileSave = 'File.Save'
    """Indicates that a file was saved."""
    FileRename = 'File.Rename'
    """Indicates that a file was renamed."""
    FileCopy = 'File.Copy'
    """Indicates that a file was copied."""
    FileEdit = 'File.Edit'
    """Indicates that the contents of a file were edited."""
    FileFocus ='File.Focus'
    """Indicates that a file was selected by the user within the user interface."""
    Compile = 'Compile'
    """Indicates an attempt to compile all or part of the code."""
    CompileError = 'Compile.Error'
    """Represents a compilation error and its associated diagnostic."""
    CompileWarning = 'Compile.Warning'
    """Represents a compilation warning and its associated diagnostic."""
    Submit = 'Submit'
    """Indicates that code was submitted to the system."""
    RunProgram = 'Run.Program'
    """Indicates a program execution and its associated input and/or output."""
    RunTest = 'Run.Test'
    """Indicates execution of a test and its associated input and/or output."""
    DebugProgram = 'Debug.Program'
    """Indicates a debug execution of the program and its associated input and/or output."""
    DebugTest = 'Debug.Test'
    """Indicates a debug execution of a test and its associated input and/or output."""
    ResourceView = 'Resource.View'
    """Indicates that an intervention such as a hint was done."""
    Intervention = 'Intervention'
    """Indicates that a resource (typically a learning resource of some type) was viewed."""


class ProgSnap2Dataset:

    MAIN_TABLE_FILE = 'MainTable.csv'
    METADATA_TABLE_FILE = 'DatasetMetadata.csv'
    LINK_TABLE_DIR = 'LinkTables'
    CODE_STATES_DIR = 'CodeStates'
    CODE_STATES_TABLE_FILE = os.path.join(CODE_STATES_DIR, 'CodeStates.csv')

    def __init__(self, directory):
        self.directory = directory
        self.main_table = None
        self.metadata_table = None
        self.code_states_table = None

    def path(self, local_path):
        return path.join(self.directory, local_path)

    def get_main_table(self):
        """ Returns a Pandas DataFrame with the main event table for this dataset
        """
        if self.main_table is None:
            self.main_table = pd.read_csv(self.path(ProgSnap2Dataset.MAIN_TABLE_FILE))
            if self.get_metadata_property(Metadata.IsEventOrderingConsistent):
                order_scope = self.get_metadata_property(Metadata.EventOrderScope)
                if order_scope == 'Global':
                    # If the table is globally ordered, sort it
                    self.main_table.sort_values(by=[PS2.Order], inplace=True)
                elif order_scope == 'Restricted':
                    # If restricted ordered, sort first by grouping columns, then by order
                    order_columns = self.get_metadata_property(Metadata.EventOrderScopeColumns)
                    if order_columns is None or len(order_columns) == 0:
                        raise Exception('EventOrderScope is restricted by no EventOrderScopeColumns given')
                    columns = order_columns.split(';')
                    columns.append('Order')
                    # The result is that _within_ these groups, events are ordered
                    self.main_table.sort_values(by=columns, inplace=True)
        return self.main_table.copy()

    def set_main_table(self, main_table):
        """ Overwrites the main table loaded from the file with the provided table.
        This this table will be used for future operations, including copying the dataset.
        """
        self.main_table = main_table.copy()

    def get_code_states_table(self):
        """ Returns a Pandas DataFrame with the code states table form this dataset
        """
        if self.code_states_table is None:
            self.code_states_table = pd.read_csv(self.path(ProgSnap2Dataset.CODE_STATES_TABLE_FILE))
        return self.code_states_table.copy()

    def get_metadata_property(self, property):
        """ Returns the value of a given metadata property in the metadata table
        """
        if self.metadata_table is None:
            self.metadata_table = pd.read_csv(self.path(ProgSnap2Dataset.METADATA_TABLE_FILE))

        values = self.metadata_table[self.metadata_table['Property'] == property]['Value']
        if len(values) == 1:
            return values.iloc[0]
        if len(values) > 1:
            raise Exception('Multiple values for property: ' + property)

        # Default return values as of V6
        if property == Metadata.IsEventOrderingConsistent:
            return False
        if property == Metadata.EventOrderScope:
            return 'None'
        if property == Metadata.EventOrderScopeColumns:
            return ''

        return None

    def __link_table_path(self):
        return self.path(ProgSnap2Dataset.LINK_TABLE_DIR)

    def list_link_tables(self):
        """ Returns a list of the link tables in this dataset, which can be loaded with load_link_table
        """
        path = self.__link_table_path()
        dirs = os.listdir(path)
        return [f for f in dirs if os.path.isfile(os.path.join(path, f)) and f.endswith('.csv')]

    def load_link_table(self, link_table):
        """ Returns a Pandas DataFrame with the link table with the given name
        :param link_table: The link table nme or file
        """
        if not link_table.endswith('.csv'):
            link_table += '.csv'
        return pd.read_csv(path.join(self.__link_table_path(), link_table))

    def drop_main_table_column(self, column):
        self.get_main_table()
        self.main_table.drop(column, axis=1, inplace=True)

    def save_subset(self, path, main_table_filterer, copy_link_tables=True):
        os.makedirs(os.path.join(path, ProgSnap2Dataset.CODE_STATES_DIR), exist_ok=True)
        main_table = main_table_filterer(self.get_main_table())
        main_table.to_csv(os.path.join(path, ProgSnap2Dataset.MAIN_TABLE_FILE), index=False)
        code_state_ids = main_table[PS2.CodeStateID].unique()
        code_states = self.get_code_states_table()
        code_states = code_states[code_states[PS2.CodeStateID].isin(code_state_ids)]
        code_states.to_csv(os.path.join(path, ProgSnap2Dataset.CODE_STATES_DIR, 'CodeStates.csv'), index=False)
        self.metadata_table.to_csv(os.path.join(path, ProgSnap2Dataset.METADATA_TABLE_FILE), index=False)

        if not copy_link_tables:
            return

        os.makedirs(os.path.join(path, ProgSnap2Dataset.LINK_TABLE_DIR), exist_ok=True)

        def indexify(x):
            return tuple(x) if len(x) > 1 else x[0]

        for link_table_name in self.list_link_tables():
            link_table = self.load_link_table(link_table_name)
            columns = [col for col in link_table.columns if col.endswith('ID') and col in main_table.columns]
            distinct_ids = main_table.groupby(columns).apply(lambda x: True)
            # TODO: Still need to test this with multi-ID link tables
            to_keep = [indexify(list(row)) in distinct_ids for index, row in link_table[columns].iterrows()]
            filtered_link_table = link_table[to_keep]
            filtered_link_table.to_csv(os.path.join(path, ProgSnap2Dataset.LINK_TABLE_DIR, link_table_name), index=False)



    @staticmethod
    def __to_one(lst, error):
        if len(lst) == 0:
            return None
        if len(lst) > 1:
            raise Exception(error or 'Should have only one result!')
        return lst.iloc[0]

    def get_code_for_id(self, code_state_id):
        if code_state_id is None:
            return None
        code_states = self.get_code_states_table()
        code = code_states[code_states[PS2.CodeStateID] == code_state_id][PS2.Code]
        return ProgSnap2Dataset.__to_one(code, 'Multiple code states match that ID.')

    def get_code_for_event_id(self, row_id):
        events = self.get_main_table()
        code_state_ids = events[events[PS2.EventID == row_id]][PS2.CodeStateID]
        code_state_id = ProgSnap2Dataset.__to_one(code_state_ids, 'Multiple rows match that ID.')
        return self.get_code_for_id(code_state_id)

    def get_subject_ids(self):
        events = self.get_main_table()
        return events[PS2.SubjectID].unique()

    def get_problem_ids(self):
        events = self.get_main_table()
        return events[PS2.ProblemID].unique()

    def get_trace(self, subject_id, problem_id):
        events = self.get_main_table()
        rows = events[(events[PS2.SubjectID] == subject_id) & (events[PS2.ProblemID] == problem_id)]
        ids = rows[PS2.CodeStateID].unique()
        return [self.get_code_for_id(code_state_id) for code_state_id in ids]


