"""
Generates db from one or multiple csv file(s)
"""
import sqlite3
import sys
import os

# Classes
class CSV:
    """
    Represents a CSV file that is to be written into a database
    """
    def __init__(self, path):
        """
        Initializes CSV object
        """
        self.path = path
        self.name = path[path.rfind("\\") + 1:path.rfind(".csv")]
        self.read_contents()

    def read_contents(self):
        """
        Reads csv file to extract column names/row contents and infers datatypes
        """
        infile = open(self.path)
        self.content = [line.strip().split(",") for line in infile.readlines()]
        infile.close()
        self.column_names = self.content.pop(0)
        self.infer_types()

    def infer_types(self):
        """
        Determine SQLite datatypes to use for each column
        """

        # Infer data type of every column
        self.column_types = []
        for j in range(len(self.column_names)):

            # Call function to check column contents
            if is_col_int(j, self.content):
                self.column_types.append("INTEGER")
            elif is_col_flt(j, self.content):
                self.column_types.append("REAL")
            else:
                self.column_types.append("CHAR")

                # Surround values in column with quotes and escape all other '
                for i, row in enumerate(self.content):
                    self.content[i][j] = "'%s'" % self.content[i][j].replace("'", "''")
                    

                
                
    def write_content(self, db_connection, db_cursor):
        """
        Writes data in self.content into SQL database
        Returns: None
        """

        # Create table
        db_cursor.execute("CREATE TABLE %s(%s);" % (self.name, "id INTEGER PRIMARY KEY AUTOINCREMENT, " + ", ".join(["%s %s" % (self.column_names[i], self.column_types[i]) for i in range(len(self.column_names))])))
        
        # Write content
        for i in range(len(self.content)):
            try:
                line = "INSERT INTO %s VALUES(%d, %s)" % (self.name, i, self.make_insert_string(i))
                db_cursor.execute(line)
            except sqlite3.OperationalError as oe:
                print(oe)
                print(line)
                return
        
        # Commit changes
        db_connection.commit()

    def make_insert_string(self, index):
        """
        Return SQL INSERT INTO statement for line i of self.content
        """

        # Get content line
        line = self.content[index]

        # Make and return line
        # return ", ".join([line[i] if self.column_types[i] != "CHAR" else '"%s"' % line[i] for i in range(len(line))])
        return ", ".join([line[i] for i in range(len(line))])


    def get_path(self):
        """
        Returns path to csv
        """
        return self.path
    
    def get_name(self):
        """
        Returns name of csv
        """
        return self.name

# Functions
def is_col_int(j, content):
    """
    Checks whether all values within column j of the 2D array "content" are integers
    Returns: True if all values within column j are int, False otherwise
    """
    for row in content:
        try:
            int(row[j])
        except ValueError:
            return False
        else:
            continue
    return True

def is_col_flt(j, content):
    """
    Checks whether all values within column j of the 2D array "content" are floating-point (real) numbers
    Returns: True if all values within column j are float, False otherwise
    """
    for row in content:
        try:
            float(row[j])
        except ValueError:
            return False
        else:
            continue
    return True


def get_paths():
    """
    Returns path to .csv and folder for .db
    Checks command line arguments first
    Otherwise, prompts user for input
    Returns: list of paths to .csv files, path to .db file, and name of database
    """
    csv_paths = []
    db_path = ""
    db_name = ""

    # Open and read input file
    infile = open("paths.txt")
    content = [line.strip() for line in infile.readlines()]
    infile.close()

    # Get inputs
    for line in content:

        # Check for csv path
        if len(line) > 4 and line[-4:] == ".csv" and os.path.isfile(line):
            csv_paths.append(line)

        # Check for database name
        elif os.path.isdir(line):
            db_path = line

        # Otherwise, assign database name
        else:
            db_name = line

    # Check for missing inputs
    if len(csv_paths) == 0:
        raise Exception("No valid path to a .csv file was provided in paths.txt")

    if db_path == "" or db_path == ".":
        db_path = ""
        print("Database will be created in working directory")
    else:
        db_path += "\\"

    if db_name == "":
        print('No database name was provided in paths.txt. Creating database file as "database.db"')
        db_name = "database"

    # Return paths
    return csv_paths, db_path, db_name

def read_csvs(csv_paths):
    """
    Opens file specified by file path and reads content and column names into CSV object
    Returns: 1-D list of column names, 2-D list of csv contents
    """

    # Create CSV object for each path and return list of objects
    return [CSV(path) for path in csv_paths]

def create_database(db_file_path):
    """
    Create database at desired destination
    Returns: connection to and cursor of database
    """
    
    # Create database file
    open(db_file_path, "w").close()

    # Open database with sqlite
    connection = sqlite3.connect(db_file_path)
    cursor = connection.cursor()

    # Return database connection and cursor
    return connection, cursor

def write_db(csv_list, connection, cursor):
    """
    Creates tables and writes content into db
    Returns: none
    """
    
    # Write content for each CSV object
    for csv in csv_list:
        csv.write_content(connection, cursor)

def main():
    """
    Main functionality
    """

    # Get file paths
    csv_paths, db_path, db_name = get_paths()

    # Get columns and content from csv
    csv_list = read_csvs(csv_paths)

    # Create database
    connection, cursor = create_database("%s%s.db" % (db_path, db_name))

    # Write CSV content into database
    write_db(csv_list, connection, cursor)

if __name__ == "__main__":
    main()