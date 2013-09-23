import logging
import sys

import sqlalchemy
import sqlalchemy.ext.declarative
import sqlalchemy.dialects.mysql
import sqlalchemy.dialects.postgresql
import sqlalchemy.orm

mappings = {
    sqlalchemy.String: [ 
        sqlalchemy.dialects.mysql.MEDIUMTEXT,
        sqlalchemy.dialects.mysql.TINYTEXT
    ],
    sqlalchemy.Integer: [
        sqlalchemy.dialects.mysql.INTEGER
    ]
}

logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.WARN)
logger = logging.getLogger('sqltransmutate')
logger.setLevel(logging.INFO)

logger.info('Connecting to %s', sys.argv[1])

engine1 = sqlalchemy.create_engine(sys.argv[1])
Session1 = sqlalchemy.orm.sessionmaker(bind=engine1)
session1 = Session1()

logger.info('Introspecting')

metadata = sqlalchemy.MetaData()
metadata.reflect(bind=engine1)

tables = metadata.tables.values()

logger.info('Replacing dialect types with SQLAlchemy types')
for table in tables:
    for col in table.columns:
        for base_type, dialect_types in mappings.items():
            for dialect_type in dialect_types:
                if isinstance(col.type, dialect_type):
                    col.type = base_type()


logger.info('Metadata --')
for table in tables:
    logger.info('  Table %r --', table.name)
    for col in table.columns:
        logger.info('    %r', col)

tables = metadata.tables.values()

logger.info('Mapping entities --')
entities = { table.name: type(str(table.name) + '_entity', (object,), {'__table__': table})
             for table in tables }
for entity in entities.values():
    logger.info('  Mapping entity %r to table %r', entity, entity.__table__.name)
    sqlalchemy.orm.mapper(entity, entity.__table__)

def get_dependencies(table):
    return { foreign_key.target_fullname.split('.')[0]
             for col in table.columns
             for foreign_key in col.foreign_keys }

logger.info('Dependencies --')
for table in tables:
    logger.info('  Table %r: %r', table.name, get_dependencies(table))

i = 0
while i < len(tables):
    for dependency in get_dependencies(tables[i]):
        j = tables.index(metadata.tables[dependency])
        if j > i:
            logger.info('Switch %s and %s', tables[i].name, dependency)
            tables[j], tables[i] = tables[i], tables[j]
            i = 0
            break
    else:
        i += 1

logger.info('Dependencies --')
for table in tables:
    logger.info('  Table %r: %r', table.name, get_dependencies(table))


if 1:
    logger.info('Connecting to %s', sys.argv[2])
    engine2 = sqlalchemy.create_engine(sys.argv[2])
    Session2 = sqlalchemy.orm.sessionmaker(bind=engine2)
    session2 = Session2()

    logger.info('Creating tables and the rest')
    metadata.create_all(engine2)


logger.info('Old counts: %r', { table.name: session1.query(table).count()
                                for table in tables })

for table in tables:
    later = []
    logger.info('Copying items from table %s', table)
    for item in session1.query(entities[table.name]):
        new_item = entities[table.name]()
        for col in table.columns:
            value = getattr(item, col.name)
            setattr(new_item, col.name, value)
        try:
            session2.add(new_item)
            session2.commit()
        except sqlalchemy.exc.IntegrityError:
            session2.rollback()
            later.append(new_item)
    if later:
        logger.info('Retrying %d items', len(later))
    while later:
        new_item = later.pop(0)
        try:
            session2.add(new_item)
            session2.commit()
        except sqlalchemy.exc.IntegrityError:
            session2.rollback()
            later.append(new_item)

logger.info('New counts: %r', { table.name: session2.query(table).count()
                                for table in tables })

