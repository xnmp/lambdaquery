.. LambdaQuery documentation master file, created by
   sphinx-quickstart on Sat Oct  7 18:35:07 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.


Welcome to λQuery!
=======================================

.. toctree::
   :maxdepth: 2
   :caption: Contents
   
   motivation
   start
   query
   properties
   advanced
   faq

This is a library that enables one to write composable SQL in pure python. It is heavily functional, enabling one to chain query combinators in incidentally very similar to the `Rabbit query language <https://arxiv.org/abs/1702.08409>`_. 

The question that we solve - why is writing SQL so hard? How is it that something describable in two sentences in English becomes a monster of a query? How does one not become lost navigating the scoping rules and the throwaway names that become rampant when you have more than one layer of aggregation? LambdaQuery is the answer.

The main goals of LambdaQuery are to be a query API that:

* Removes as much syntactic noise as possible. 
    
    * Joining by foreign keys automatically.
    * Handles all of the red tape in SQL.
    * Automatically assigns the throaway names to subqueries and columns of subqueries. 
    * Knows which tables should be left joined and handles the null cases.
    * Knows which relationships are one to one, and choosing the group bys appropriately to preserve structure of the rows.

* Syntax that is very similar to manipulating in-memory Python lists. 
    
    * Being able to work with particular elements of a list or the list as a whole. 
    * Having CLEAN syntax just like actual Python. 

* Maximize code reusability. 
    
    * Define functions from rows to rows (a one-to-one relationship), from rows to lists (a one-to-many relationship), from lists to lists, or lists to rows (aggregation). 
    * Such functions can include data from other tables. 
    
* Minimize boilerplate - setting up tables should be as easy as it is in SQLAlchemy. 


Contributing
------------

If you have a question then please raise an issue on Github `here <https://github.com/xnmp/lambdaquery/issues/>`_. 


