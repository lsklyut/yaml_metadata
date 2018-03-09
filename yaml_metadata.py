from sqlalchemy import MetaData, Table, Column, ForeignKey, Integer, types
import yaml


class TableDefinition(yaml.YAMLObject):
    """
    Used to distinguish nested tables from columns.  This class is
    instantiated by YAML when an object with the ``!Table`` tag is
    visited.
    """
    yaml_tag = u'!Table'

    def __init__(self, name, columns):
        self.name = name
        self.columns = columns


def construct_column(name, column_type):
    """
    Given the name of the type, return an SQLAlchemy Column.

    Todo
    ----
    Allow arguments to be passed to the type engine here.  The length
    of a String, for example.
    """
    try:
        return Column(name, types.__dict__[column_type]())
    except KeyError:
        raise KeyError('Unknown type "%s" in column "%s"' % (column_type, name))


def construct_foreign_key(one_table, many_name, key_metadata):
    """
    Create a foreign key column in a table with the name given by
    manName that references the first primary key of the table given
    by oneTable.  Called when constructing a 'nested' table.

    The arguments 'oneTable' and 'manyName' refer to the one-to-many
    relationship implied by the added foreign key.
    """

    # Use the first primary key from the 'one' table.
    print(one_table.primary_key.columns)
    first_primary_key = next(iter(one_table.primary_key.columns))

    # column name in the form of 'table_primaryId'
    column_name = '%s_%s' % (one_table.name, first_primary_key)

    # find the existing table and insert the column
    many_table = Table(many_name, key_metadata, autoload=True, mustexist=True)
    table_dict = dict(one_table.columns)

    many_table.append_column(
        Column(column_name, None, ForeignKey(table_dict[first_primary_key.name])))


def construct_table(table_def, table_metadata):
    """
    Adds a Table to the given MetaData, recursively adding any
    "sub-tables" it finds.
    """

    # Check if this table has already been constructed.
    # This can happen if the relation graph is cyclic (not a tree).
    if table_def.name in (name for name in table_metadata.tables):
        return

    # The primary key must be added before other columns in case this
    # table contains any cyclic references to itself.
    if hasattr(table_def, 'primary_key'):
        primary_key = construct_column(table_def.primary_key,
                                       table_def.columns[table_def.primary_key])
        # don't add this column again
        del table_def.columns[table_def.primary_key]
    else:  # create a primary key if none specified explicitly
        primary_key = Column('id', Integer, primary_key=True)

    # add a new table to the metadata
    # The 'meat' columns are added later.  This allows a recursive call to this
    # function to skip tables that have already been added here.
    table = Table(table_def.name, table_metadata, primary_key)

    # add the columns to the metadata
    for name, value in table_def.columns.items():
        if isinstance(value, TableDefinition):
            # recursively construct any "sub-tables"
            construct_table(value, table_metadata)
            construct_foreign_key(table, value.name, table_metadata)
        else:
            table.append_column(construct_column(name, value))


def generate_metadata(file_object):
    """
    Given a YAML file containing a list of table definitions, returns a
    SQLAlchemy MetaData object describing the database.
    """

    table_def = yaml.load(file_object)
    table_metadata = MetaData()

    # assumes the root element is a list of table definitions
    construct_table(table_def, table_metadata)

    return table_metadata


metadata = generate_metadata("""
!Table
name: people
columns:
  firstName: String
  lastName:  String
  inventory: !Table
    name: items
    columns:
      description: String
""")

print(metadata.tables)
