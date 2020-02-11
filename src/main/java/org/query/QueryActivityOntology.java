package org.query;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.http.HTTPRepository;

/**
* <h1>Performs three queries on an ontology of activities measured by house sensors</h1>
* The QueryActivityOntology program implements an application that
* performs queries on a GraphDB repository where an ontology and sensors measurements
* have been loaded by IoTSemantic application.
* <p>
*
* @author  Zoe Vasileiou
* @version 1.0
* @since   2020-02-11
*/
public class QueryActivityOntology {
	
	private RepositoryConnection connection;
	private static final String NAMESPACE = "PREFIX a: <http://www.semanticweb.org/user/ontologies/2020/1/activity#> \n"; 

	public QueryActivityOntology(RepositoryConnection connection) {
		this.connection = connection;
	}

	public void listAllActivities(RepositoryConnection connection) {
		System.out.println("\n--------------------------------\n ");
		System.out.println("# Listing all activities ");
		System.out.println("\n--------------------------------\n ");
		
		String queryString = NAMESPACE;
		
		queryString += "SELECT ?a ?sd ?ed ?c\n";
		queryString += "WHERE { \n";
		queryString += "    ?a a:hasElement  ?e . \n";
		queryString += "    ?a a a:Activity . \n";
		queryString += "    ?e a:hasStartDate ?sd . \n";
		queryString += "    ?e a:hasEndDate ?ed . \n";
		queryString += "    ?e a:hasContentString ?c .";
		queryString += "}";
		
		TupleQuery query = connection.prepareTupleQuery(queryString);
		
		TupleQueryResult result = query.evaluate();
		while (result.hasNext()) {
			BindingSet bindingSet = result.next();
			
			IRI a = (IRI) bindingSet.getBinding("a").getValue();
			
			System.out.println(a.getLocalName() + "\n\t has start date " + bindingSet.getValue("sd") + "\n\t has end date " + bindingSet.getValue("ed") + "\n\t has content string  " + bindingSet.getValue("c"));
				
		}
		result.close();
	}
	
	public void listAllObservations(RepositoryConnection connection) {
		System.out.println("\n--------------------------------\n ");
		System.out.println("# Listing all Observation Types ");
		System.out.println("\n--------------------------------\n ");
		
		String queryString = NAMESPACE;
		
		queryString += "SELECT DISTINCT ?c\n";
		queryString += "WHERE { \n";
		queryString += "    ?o a:hasElement  ?e . \n";
		queryString += "    ?o a a:Observation . \n";
		queryString += "    ?e a:hasContentString ?c . \n";
		queryString += "}";
		
		TupleQuery query = connection.prepareTupleQuery(queryString);
		
		TupleQueryResult result = query.evaluate();
		while (result.hasNext()) {
			BindingSet bindingSet = result.next();
			
			System.out.println(bindingSet.getValue("c"));	
		}
		result.close();
	}
	
	public void listAllObservationswithinRange(RepositoryConnection connection) {
		System.out.println("\n-----------------------------------------------------\n ");
		System.out.println("# Listing all Observation within specific date range ");
		System.out.println("\n-----------------------------------------------------\n ");
		
		String queryString = NAMESPACE;
		
		queryString += "SELECT DISTINCT ?c\n";
		queryString += "WHERE { \n";
		queryString += "    ?o a:hasElement  ?e . \n";
		queryString += "    ?o a a:Observation . \n";
		queryString += "    ?e a:hasStartDate ?sd . \n";
		queryString += "    ?e a:hasEndDate ?ed . \n";
		queryString += "    ?e a:hasContentString ?c . \n";
		queryString += "    FILTER ((?sd >= \"2014-05-05T18:34:54.000\"^^xsd:dateTime) && (?sd <= \"2014-05-05T18:55:40.000\"^^xsd:dateTime) && (?ed >= \"2014-05-05T18:34:54.000\"^^xsd:dateTime) && (?ed <= \"2014-05-05T18:55:40.000\"^^xsd:dateTime))";
		queryString += "}";
				
		TupleQuery query = connection.prepareTupleQuery(queryString);
		
		TupleQueryResult result = query.evaluate();
		while (result.hasNext()) {
			BindingSet bindingSet = result.next();			
			
			System.out.println(bindingSet.getValue("c"));	
		}
		result.close();
	}
	
	public static void main(String[] args) {
		// Abstract representation of a remote repository accessible over HTTP
		HTTPRepository repository = new HTTPRepository("http://localhost:7200/repositories/activity");

        // Separate connection to a repository
        RepositoryConnection connection = repository.getConnection();
		
		QueryActivityOntology q = new QueryActivityOntology(connection);
		
		try {
			q.listAllActivities(connection);
			q.listAllObservations(connection);
			q.listAllObservationswithinRange(connection);
		} finally {
			connection.close();
		}
	}
}
