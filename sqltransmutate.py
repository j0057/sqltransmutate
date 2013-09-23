import sys

import sqlalchemy
import sqlalchemy.orm
import sqlalchemy.dialects.mysql
import sqlalchemy.dialects.postgresql

engine1 = sqlalchemy.create_engine(sys.argv[1])
engine2= sqlalchemy.create_engine(sys.argv[2])

Session1 = sqlalchemy.orm.sessionmaker(bind=engine1)
Session2 = sqlalchemy.orm.sessionmaker(bind=engine2)

metadata = sqlalchemy.MetaData()
metadata.reflect(bind=engine1)

#session1 = Session1()
#for artist in mysql.query(metadata.tables['artist']).all():
#    print artist

mappings = {
    sqlalchemy.String: [ 
        sqlalchemy.dialects.mysql.MEDIUMTEXT,
        sqlalchemy.dialects.mysql.TINYTEXT
    ],
    sqlalchemy.Integer: [
        sqlalchemy.dialects.mysql.INTEGER
    ]
}

for (table_name, table) in metadata.tables.items():
    for col in table.columns:
        for base_type, dialect_types in mappings.items():
            for dialect_type in dialect_types:
                if isinstance(col.type, dialect_type):
                    col.type = base_type()


for (table_name, table) in metadata.tables.items():
    print 'table', repr(table_name)
    for col in table.columns:
        #print '\tcolumn', col.name, ':', type(col.type)
        print '\t', repr(col)

#for table_name in metadata.tables.keys():
    #print repr(metadata.tables[table_name])

#metadata.create_all(pgsql_engine)

print

def get_dependencies(table):
    return { foreign_key.target_fullname.split('.')[0]
             for col in table.columns
             for foreign_key in col.foreign_keys }

tables = metadata.tables.values()

for table in tables:
    print repr(table.name), ':', repr(get_dependencies(table))

print

i = 0
while i < len(tables):
    for dependency in get_dependencies(tables[i]):
        j = tables.index(metadata.tables[dependency])
        if j > i:
            print 'switch', tables[i].name, 'and', dependency
            tables[j], tables[i] = tables[i], tables[j]
            i = 0
            break
    else:
        i += 1

print
    
for table in tables:
    print repr(table.name), ':', repr(get_dependencies(table))

