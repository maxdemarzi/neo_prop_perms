package com.maxdemarzi;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.server.rest.RestRequest;
import org.neo4j.test.server.HTTP;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

public class GetAssociatesTest {

    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withFixture(MODEL_STATEMENT)
            .withExtension("/v1", Service.class);


    @Test
    public void shouldRespondToGetAssociatesMethodForMax() {
        HTTP.POST(neo4j.httpURI().resolve("/v1/schema/create").toString());
        RestRequest req = RestRequest.req();
        req.put(neo4j.httpURI().resolve("/v1/service/Max/associates/B/permissions/name").toString(), null);
        req.put(neo4j.httpURI().resolve("/v1/service/Max/associates/C/permissions/id").toString(), null);
        req.put(neo4j.httpURI().resolve("/v1/service/Max/associates/C/permissions/name").toString(), null);
        req.put(neo4j.httpURI().resolve("/v1/service/Max/associates/C/permissions/age").toString(), null);
        req.put(neo4j.httpURI().resolve("/v1/service/Max/associates/D/permissions/id").toString(), null);

        HTTP.Response response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/Max/associates/A").toString());
        List<HashMap<String, Object>> actual  = response.content();

        Assert.assertEquals(expected4max, new HashSet<>(actual));
    }

    @Test
    public void shouldRespondToGetAssociatesMethodForJunior() {
        HTTP.POST(neo4j.httpURI().resolve("/v1/schema/create").toString());
        RestRequest req = RestRequest.req();
        req.put(neo4j.httpURI().resolve("/v1/service/Junior/associates/C/permissions/name").toString(), null);
        req.put(neo4j.httpURI().resolve("/v1/service/Junior/associates/D/permissions/name").toString(), null);
        req.put(neo4j.httpURI().resolve("/v1/service/Junior/associates/D/permissions/age").toString(), null);
        req.delete(neo4j.httpURI().resolve("/v1/service/Junior/associates/D/permissions/age").toString());
        HTTP.Response response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/Junior/associates/A").toString());

        List<HashMap<String, Object>> actual  = response.content();
        Assert.assertEquals(expected4junior, new HashSet<>(actual));

    }

    private static final String MODEL_STATEMENT =
            "CREATE (u1:User {username: 'Max'})" +
            "CREATE (u2:User {username: 'Junior'})" +
            "CREATE (g1:Group {name: 'Administrators'})" +
            "CREATE (g2:Group {name: 'Junior Analysts'})" +
                    "CREATE (p1:Person {id: 'A', name: 'Tom', age: 35})" +
                    "CREATE (p2:Person {id: 'B', name: 'Tim', age: 36})" +
                    "CREATE (p3:Person {id: 'C', name: 'Tony', age: 37})" +
                    "CREATE (p4:Person {id: 'D', name: 'Todd', age: 38})" +
                    "CREATE (p1)-[:KNOWS {how:'friends'}]->(p2)" +
                    "CREATE (p1)-[:KNOWS {how:'friends'}]->(p3)" +
                    "CREATE (p1)-[:KNOWS {how:'friends'}]->(p4)" +
                    "CREATE (u1)-[:BELONGS_TO]->(g1)" +
                    "CREATE (u2)-[:BELONGS_TO]->(g2)";

    private static final HashSet<HashMap<String, Object>> expected4max =
            new HashSet<HashMap<String, Object>>() {{
                add(new HashMap<String, Object>() {{
                    //put("id", "B");
                    put("name", "Tim");
                    //put("age", 36);
                }});
                add(new HashMap<String, Object>() {{
                    put("id", "C");
                    put("name", "Tony");
                    put("age", 37);
                }});
                add(new HashMap<String, Object>() {{
                    put("id", "D");
                    //put("name", "Todd");
                    //put("age", 38);
                }});
            }};

    private static final  HashSet<HashMap<String, Object>> expected4junior =
            new HashSet<HashMap<String, Object>>() {{
                add(new HashMap<String, Object>() {{
//                    put("id", "C");
                    put("name", "Tony");
//                    put("age", 37);
                }});
                add(new HashMap<String, Object>() {{
//                    put("id", "D");
                    put("name", "Todd");
//                    put("age", 38);
                }});
            }};

}
