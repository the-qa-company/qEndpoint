PREFIX     : <http://example.org/> 
PREFIX foaf: <http://xmlns.com/foaf/0.1/> 

WITH <http://example.org/g3>
DELETE 
{
  GRAPH <http://example.org/g2> { ?s ?p ?o }
}
WHERE 
{
  GRAPH <http://example.org/g2> { ?s foaf:name "Chris" .
                                  ?s ?p ?o }
}
