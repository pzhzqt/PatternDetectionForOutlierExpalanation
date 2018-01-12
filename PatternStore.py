from sqlalchemy.orm import sessionmaker
from sqlalchemy import MetaData, Table
from sqlalchemy import DECIMAL, Column, String, Integer
import PatternFinder as pf

Pattern = None
metadata = MetaData()


def create_table_object(tablename):
    table_name = tablename+'_patterns'
    table_object = Table(table_name, metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('fixed', String),
        Column('fixedvalue', String),
        Column('variable', String),
        Column('aggfunction', String),
        Column('aggvalue', String),
        Column('pattern', String),
        Column('metric', DECIMAL)
    )
    global Pattern
    Pattern = table_object


def addPattern(fixed, fixedvalue, variable, aggfunction, aggvalue,
               pattern, metric):

    metadata.create_all(pf.engine)

    Session = sessionmaker(bind=pf.engine)
    session = Session()
    insert = Pattern.insert().values(fixed=fixed, fixedvalue=fixedvalue,
                          variable=variable, aggfunction=aggfunction,
                          aggvalue=aggvalue, pattern=pattern, metric=metric)
    session.execute(insert)
    session.commit()
