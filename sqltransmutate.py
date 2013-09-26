#!/usr/bin/env python

import logging
import sys

import sqlalchemy
import sqlalchemy.ext.declarative
import sqlalchemy.dialects.mysql
import sqlalchemy.dialects.postgresql
import sqlalchemy.orm

logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.WARN)
logger = logging.getLogger('sqltransmutate')
logger.setLevel(logging.INFO)

mappings = {
    sqlalchemy.String: [ 
        sqlalchemy.dialects.mysql.MEDIUMTEXT,
        sqlalchemy.dialects.mysql.TINYTEXT
    ],
    sqlalchemy.Integer: [
        sqlalchemy.dialects.mysql.INTEGER
    ]
}

def connect(url):
    logger.info('Connecting to %s', url)
    engine =  sqlalchemy.create_engine(url)
    Session = sqlalchemy.orm.sessionmaker(bind=engine)
    session = Session()
    return (engine, Session, session)

def introspect_database(engine):
    logger.info('Introspecting')
    metadata = sqlalchemy.MetaData()
    metadata.reflect(bind=engine)
    return metadata

def replace_dialect_types(tables):
    logger.info('Replacing dialect types with SQLAlchemy types')
    for table in tables:
        for col in table.columns:
            for base_type, dialect_types in mappings.items():
                for dialect_type in dialect_types:
                    if isinstance(col.type, dialect_type):
                        col.type = base_type()

def log_metadata(tables):
    logger.info('Metadata --')
    for table in tables:
        logger.info('  Table %r --', table.name)
        for col in table.columns:
            logger.info('    %r', col)

def __repr__(self):
    return '<{0} {1}>'.format(type(self).__name__,
         ' '.join('{0}={1!r}'.format(col.name, getattr(self, col.name))
                  for col in self.__table__.columns))

def map_entities(tables):
    logger.info('Mapping entities --')
    entities = { table.name: type(str(table.name).title().replace('_', ''),
                                  (object,), 
                                  { '__table__': table, '__repr__': __repr__ })
                 for table in tables }
    for entity in entities.values():
        logger.info('  Mapping entity %r to table %r', entity, entity.__table__.name)
        sqlalchemy.orm.mapper(entity, entity.__table__)
    return entities

def get_dependencies(table):
    return { foreign_key.target_fullname.split('.')[0]
             for col in table.columns
             for foreign_key in col.foreign_keys }

def log_dependencies(tables):
    logger.info('Dependencies --')
    for table in tables:
        logger.info('  Table %r: %r', table.name, get_dependencies(table))

def reorder_tables(tables):
    i = 0
    while i < len(tables):
        for dependency in get_dependencies(tables[i]):
            j = tables.index(metadata.tables[dependency])
            if j > i:
                logger.info('Switch %s and %s', tables[i].name, dependency)
                tables[j], tables[i] = tables[i], tables[j]
                break
        else:
            i += 1

def create_tables(engine, metadata):
    logger.info('Creating tables and the rest')
    metadata.create_all(engine)

def log_counts(session, tables):
    logger.info('Counts: %r', { table.name: session.query(table).count()
                                for table in tables })

def cloner(objtype):
    def clone(obj):
        result = objtype()
        for col in objtype.__table__.columns:
            value = getattr(obj, col.name)
            setattr(result, col.name, value)
        return result
    return clone

def index_by_key(items, key_name, key_value):
    for (i, item) in enumerate(items):
        if getattr(item, key_name) == key_value:
            return i
    return -1

def reorder_items(table, items):
    foreign_keys = [ col for col in table.columns if col.foreign_keys ]
    logger.debug('Foreign keys: %r', foreign_keys)
    recursive_keys = [ (col.name, fk.target_fullname.split('.')[-1])
                       for col in foreign_keys
                       for fk in col.foreign_keys
                       if fk.target_fullname.split('.')[-2] == table.name ]
    logger.debug('Recursive keys: %r', recursive_keys)
    for (source_name, target_name) in recursive_keys:
        i = 0
        while i < len(items):
            source_value = getattr(items[i], source_name)
            if source_value is None:
                i += 1
            else:
                j = index_by_key(items, target_name, source_value)
                if j > i:
                    logger.debug('Switching items --')
                    logger.debug('  #%r %r', i, items[i])
                    logger.debug('  #%r %r', j, items[j])
                    items[i], items[j] = items[j], items[i]
                else:
                    i += 1
    return items

def copy_items(session1, session2, tables, entities):
    for table in tables:
        logger.info('Copying items from table %s', table)
        items = session1.query(entities[table.name])
        if table.name in get_dependencies(table):
            items = reorder_items(table, list(items))
        clone = cloner(entities[table.name])
        for item in items:
            new_item = clone(item)
            session2.add(new_item)
        session2.commit()

def fixup_target_database(url, engine, tables):
    if url.startswith('postgresql://'):
        for table in tables:
            primary_keys = [ col for col in table.columns if col.primary_key ]
            logger.info('Resetting PostgreSQL sequence for table %s', table.name)
            if len(primary_keys) != 1:
                continue
            sql = """
                SELECT pg_catalog.setval(pg_get_serial_sequence('{0}', '{1}'), 
                (SELECT MAX("{1}") FROM {0}))""".format(table.name, primary_keys[0].name)
            engine.execute(sql)

if __name__ == '__main__':
    SOURCE = sys.argv[1]
    TARGET = sys.argv[2]

    engine1, Session1, session1 = connect(SOURCE)
    engine2, Session2, session2 = connect(TARGET)

    metadata = introspect_database(engine1)
    tables = metadata.tables.values()

    log_metadata(tables)
    replace_dialect_types(tables)

    entities = map_entities(tables)
    globals().update({ entity.__name__: entity for entity in entities.values() })

    log_dependencies(tables)
    reorder_tables(tables)
    log_dependencies(tables)

    create_tables(engine2, metadata)

    log_counts(session1, tables)
    copy_items(session1, session2, tables, entities)
    log_counts(session2, tables)

    fixup_target_database(TARGET, engine2, tables)

