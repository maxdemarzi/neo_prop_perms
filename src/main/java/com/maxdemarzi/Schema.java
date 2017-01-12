package com.maxdemarzi;


import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

@Path("/schema")
public class Schema {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @POST
    @Path("/create")
    public Response create(@Context GraphDatabaseService db) throws IOException {
        ArrayList<String> results = new ArrayList<>();

        try (Transaction tx = db.beginTx()) {
            org.neo4j.graphdb.schema.Schema schema = db.schema();
            if (!schema.getConstraints(Label.label("User")).iterator().hasNext()) {
                schema.constraintFor(Label.label("User"))
                        .assertPropertyIsUnique("username")
                        .create();
                tx.success();
                results.add("(:User {username}) constraint created");
            }
        }

        try (Transaction tx = db.beginTx()) {
            org.neo4j.graphdb.schema.Schema schema = db.schema();
            if (!schema.getConstraints(Label.label("Person")).iterator().hasNext()) {
                schema.constraintFor(Label.label("Person"))
                        .assertPropertyIsUnique("id")
                        .create();
                tx.success();
                results.add("(:Person {id}) constraint created");
            }
        }

        try (Transaction tx = db.beginTx()) {
            org.neo4j.graphdb.schema.Schema schema = db.schema();
            schema.awaitIndexesOnline(30, TimeUnit.SECONDS);
            tx.success();
        }
        results.add("Schema Created");

        return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
    }
}
