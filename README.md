SQLTransmutate
==============

What this thing does is it inspects a database, creates the same schema in another database 
with another engine, then copies over all the data. It has no knowledge whatsoever of which 
databases it's moving from and to. Whatever SQLAlchemy can handle, this script can handle.
Scratch that -- it has to do some post-processing to reset the sequence counters for PostgreSQL.
Oh well.

Why??
-----

Because I don't want to run Oracle's software any more, and PostgreSQL is pretty awesome. :-) 
Plus I get to hack around in SQLAlchemy, which turned out to be a lot of geeky fun.

  
Limitations
-----------

It's not perfect, but it works for me. This thing:

* Will suck badly on big datasets. Or even somewhat largish datasets. I don't suppose this is 
  something you'd want to do every day.
* Does not handle circular dependencies between tables (infinite loop will result).
* Does not handle circular dependencies within tables (infinite loop will result).
* Needs a primary key on every table. SQLAlchemy demands it. In my case, I have association tables 
  for many-to-many relations. I had those defined with a unique index over the two foreign keys
  and had to change those to be a primary key instead. Not a really big deal to me, but YMMV.
* Is not complete for many dialect-specific data types. This won't be a good way to move the data
  if SQLAlchemy doesn't have a generic data type for it. (I think The Eagles have a song about this
  one.) That said, any pull requests are welcome!
* Is not very careful with mapping MySQL's TINYTEXT and MEDIUMTEXT things.

So yeah, circular dependencies. Who does that anyway??

How to run it
-------------

1. First copy `requirements.txt.dist` to `requirements.txt` and add any needed Python database drivers.
   Right now it contains drivers for MySQL and PostgreSQL.
2. Then copy `Makefile.inc.dist` to `Makefile.inc`. Edit it to add the connection strings to the
   source and target database. If there's anything special that needs to happen before the fun can
   start, add the required commands to drop-database and create-database. Careful with those tabs!
3. Type `make`. Grab coffee.

That was fun, wasn't it?

Did I mention pull requests are welcome?
----------------------------------------

It's missing entries in the mappings dictionary defined somewhere in the beginning of the code.
Right now it only contains what I need it to contain.

You'll probably have to extend it to make it work on your database, but if you do, I'll merge it.

