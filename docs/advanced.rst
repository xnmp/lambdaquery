
Advanced
=========


Grouping By
-----------

Grouping by


Last Before
-----------




Using Kleisli and Lifted
------------------------

If you wrap these you get a few benefits:

* The resulting columns have more readable names
* You can use the resulting functions as methods
* They add on the group bys of their source.


How it Works
============

Basically the internal representation of a query is as a graph. Query combinators are just transformations on this graph. Before generating the SQL, LambdaQuery performs a reduction algorithm on this graph, and then the reduced graph is compiled to SQL. To see the SQL of the unreduced graph, use the ``reduce=False`` option for the ``Query.sql`` method. 