Write composable SQL in pure python.  

The question that we solve - why is writing SQL so hard? How is it that something describable in two sentences in English becomes a monster of a query? How does one not become lost navigating the scoping rules and the throwaway names that become rampant when you have more than one layer of aggregation? LambdaQuery is the answer. 

#### Features

- VERY intuitive syntax - just like PonyORM except way better. The abstraction is right, and the syntax is very clean. 
- FULL COMPOSABLE. This cannot be emphasized enough. It should work just as if the tables were in-memory lists, so if it works for lists then it should work in LambdaQuery.
- Figures out the right SQL, the right nesting of subqueries, the right ways to rereference out of scope columns, and the right columns to group by (which sometimes isn't so obvious when you have multiple layers of aggregation). 


The full documentation can be found [here] (http://lambdaquery.readthedocs.io). 