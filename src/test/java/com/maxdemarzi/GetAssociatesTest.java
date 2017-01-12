package com.maxdemarzi;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.server.rest.RestRequest;
import org.neo4j.test.server.HTTP;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

public class GetAssociatesTest {

    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withFixture(MODEL_STATEMENT)
            .withExtension("/v1", Service.class);



    @Test
    public void shouldRespondToSetAssociatesPermissionForMax() {
        HTTP.POST(neo4j.httpURI().resolve("/v1/schema/create").toString());
        RestRequest req = RestRequest.req();
        req.put(neo4j.httpURI().resolve("/v1/service/Max/associates/A/permissions/name").toString(), null);
    }

    @Test
    public void shouldRespondToGetAssociatesMethodForMax() {
        HTTP.POST(neo4j.httpURI().resolve("/v1/schema/create").toString());
        RestRequest req = RestRequest.req();
        req.put(neo4j.httpURI().resolve("/v1/service/Max/associates/A/permissions/name").toString(), null);

        HTTP.Response response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/Max/associates/A").toString());
        ArrayList<String> actual  = response.content();
        Collections.sort(actual);
        Assert.assertEquals(expected4max, actual);
    }

    @Test
    public void shouldRespondToGetAssociatesMethodForJunior() {
        HTTP.POST(neo4j.httpURI().resolve("/v1/schema/create").toString());
        HTTP.Response response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/Junior/associates/A").toString());
        ArrayList<String> actual  = response.content();
        Collections.sort(actual);
        Assert.assertEquals(expected4junior, actual);
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
                    "CREATE (u1)-[:BELONGS_TO]->(g1)" +
                    "CREATE (u2)-[:BELONGS_TO]->(g2)";

    private static final ArrayList<HashMap<String, Object>> expected4max =
            new ArrayList<HashMap<String, Object>>() {{
                add(new HashMap<String, Object>() {{
                    put("id", "B");
                    put("name", "Tim");
                    put("age", 36);
                }});
                add(new HashMap<String, Object>() {{
                    put("id", "C");
                    put("name", "Tony");
                    put("age", 37);
                }});
                add(new HashMap<String, Object>() {{
                    put("id", "D");
                    put("name", "Tim");
                    put("age", 36);
                }});
            }};

    private static final ArrayList<HashMap<String, Object>> expected4junior =
            new ArrayList<HashMap<String, Object>>() {{
                add(new HashMap<String, Object>() {{
                    put("id", "B");
                    put("name", "Tim");
                    put("age", 36);
                }});
                add(new HashMap<String, Object>() {{
                    put("id", "C");
                    put("name", "Tony");
                    put("age", 37);
                }});
                add(new HashMap<String, Object>() {{
                    put("id", "D");
                    put("name", "Tim");
                    put("age", 36);
                }});
            }};

}
