package com.the_qa_company.qendpoint.store.hand;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled("hand")
public class GeoTest {

	private static final String SMALL_QUERY = """
			PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
			PREFIX schema: <http://schema.org/>
			PREFIX lod: <https://linkedopendata.eu/entity/>
			PREFIX lodp: <https://linkedopendata.eu/prop/direct/>
			#small 61915 res
			SELECT DISTINCT ?s0 ?coordinates
			WHERE {
			  ?s0 <https://linkedopendata.eu/prop/direct/P35> <https://linkedopendata.eu/entity/Q9934> .
			  ?s0 <https://linkedopendata.eu/prop/direct/P127> ?coordinates .
			  FILTER (<http://www.opengis.net/def/function/geosparql/ehContains>("POLYGON((8.811035156250002 45.29227885197089,9.57183837890625 45.29227885197089,9.57183837890625 45.641407856435364,8.811035156250002 45.641407856435364,8.811035156250002 45.29227885197089))"^^<http://www.opengis.net/ont/geosparql#wktLiteral>, ?coordinates))
			}
			""";
	private static final String BIG_QUERY = """
			PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
			PREFIX schema: <http://schema.org/>
			PREFIX lod: <https://linkedopendata.eu/entity/>
			PREFIX lodp: <https://linkedopendata.eu/prop/direct/>
			# big
			SELECT DISTINCT ?s0 ?coordinates
			WHERE {
			  ?s0 <https://linkedopendata.eu/prop/direct/P35> <https://linkedopendata.eu/entity/Q9934> .
			  ?s0 <https://linkedopendata.eu/prop/direct/P127> ?coordinates .
			  FILTER (<http://www.opengis.net/def/function/geosparql/ehContains>("POLYGON((8.811035156250002 45.29227885197089,9.57183837890625 45.29227885197089,9.57183837890625 45.641407856435364,8.811035156250002 45.641407856435364,8.811035156250002 45.29227885197089))"^^<http://www.opengis.net/ont/geosparql#wktLiteral>, ?coordinates))
			}
			""";

	@Test
	public void test() {

	}
}
