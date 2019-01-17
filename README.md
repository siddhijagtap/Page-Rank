Objective:
Design and implement a system that generates a subset of graphs using Hyperlink-Induced Topic Search (HITS)12 over currently 
available Wikipedia articles.

Steps followed:
Step 1. Given a query (e.g. “eclipse”, there are more than 100 Wikipedia pages with title
containing the text “eclipse”), get all pages containing “your-topic” in their title. Call this the
root set of pages.
Step 2. Build a base set of pages, to include the root set as well as any page that either links to a
page in the root set, or is linked to by a page in the root set.
Step 3. We then use the base set for computing hub and authority scores. To calculate your hub
and authority scores, the initial hub and authority scores of pages in the base set should be set
as 1 before the iterations. The iterations should be continued automatically until the values
converge. You should normalize the set of values after every iteration. The authority values
should be divided by the sum of all authority values after each step. Similarly, the hub values
should be divided by the sum of all hub values after each step.
The hubs and authorities scores can be calculated using eigenvectors and Singular Value
Decomposition (SVD). However, in programming assignment 1, you must implement the
algorithm with the iterative method. The final hubs and authority scores are determined only
after a large number of repetitions of the algorithm. 

