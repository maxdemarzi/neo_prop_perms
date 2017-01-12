package com.maxdemarzi;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.cursor.Cursor;
import org.neo4j.graphdb.*;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.IndexBrokenKernelException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.RelationshipItem;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import javax.ws.rs.*;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.lang.Math.toIntExact;

@Path("/service")
public class Service {

    public static final ObjectMapper objectMapper = new ObjectMapper();
    public static GraphDatabaseAPI dbapi;
    public static int userLabelId;
    public static int usernamePropertyKeyId;
    public static int permissionsPropertyKeyId;
    public static int belongsToRelationshipTypeId;
    public static int personLabelId;
    public static int personIdPropertyKeyId;
    private static final HashMap<String, Integer> keys = new HashMap();

    private static final LoadingCache<String, MutableRoaringBitmap> permissions = Caffeine.newBuilder()
            .maximumSize(10_000)
            .expireAfterWrite(5, TimeUnit.MINUTES)
            .refreshAfterWrite(1, TimeUnit.MINUTES)
            .build(userId -> getPermissions(userId));

    private static MutableRoaringBitmap getPermissions(String username) throws SchemaRuleNotFoundException, IndexBrokenKernelException, IndexNotFoundKernelException, IOException, EntityNotFoundException {
        MutableRoaringBitmap permissions = new MutableRoaringBitmap();
        try (Transaction tx = dbapi.beginTx()) {
            ThreadToStatementContextBridge ctx = dbapi.getDependencyResolver().resolveDependency(ThreadToStatementContextBridge.class);
            ReadOperations ops = ctx.get().readOperations();
            IndexDescriptor descriptor = ops.indexGetForLabelAndPropertyKey(userLabelId, usernamePropertyKeyId);
            Cursor<NodeItem> users = ops.nodeCursorGetFromUniqueIndexSeek(descriptor, username);
            if (users.next()) {
                permissions = getRoaringBitmap(ops, users.get().id());
                RelationshipIterator relationshipIterator = ops.nodeGetRelationships(users.get().id(), Direction.OUTGOING, belongsToRelationshipTypeId );
                Cursor<RelationshipItem> c;
                while (relationshipIterator.hasNext()) {
                    c = ops.relationshipCursor(relationshipIterator.next());
                    if (c.next()) {
                        permissions.or(getRoaringBitmap(ops, c.get().endNode()));
                    }
                }
            }
            tx.success();
        }
        return permissions;
    }

    private static MutableRoaringBitmap getRoaringBitmap(ReadOperations ops, long userNodeId) throws IOException, EntityNotFoundException {
        MutableRoaringBitmap rb = new MutableRoaringBitmap();
        byte[] nodeIds;
        if (ops.nodeHasProperty(userNodeId, permissionsPropertyKeyId)) {
            nodeIds = (byte[]) ops.nodeGetProperty(userNodeId, permissionsPropertyKeyId);
            ByteArrayInputStream bais = new ByteArrayInputStream(nodeIds);
            rb.deserialize(new DataInputStream(bais));
        }
        return rb;

    }

    private static MutableRoaringBitmap getRoaringBitmap(Node node) throws IOException {
        MutableRoaringBitmap rb = new MutableRoaringBitmap();
        if(node.hasProperty("permissions")) {
            byte[] nodeIds = (byte[]) node.getProperty("permissions", new MutableRoaringBitmap());
            ByteArrayInputStream bais = new ByteArrayInputStream(nodeIds);
            rb.deserialize(new DataInputStream(bais));
        }
        return rb;
    }

    public Service(@Context GraphDatabaseService db) throws PropertyKeyIdNotFoundKernelException {
        this.dbapi = (GraphDatabaseAPI) db;
        try (Transaction tx = db.beginTx()) {
            ThreadToStatementContextBridge ctx = dbapi.getDependencyResolver().resolveDependency(ThreadToStatementContextBridge.class);
            ReadOperations ops = ctx.get().readOperations();
            userLabelId = ops.labelGetForName("User");
            usernamePropertyKeyId = ops.propertyKeyGetForName("username");
            permissionsPropertyKeyId = ops.propertyKeyGetForName("permissions");
            belongsToRelationshipTypeId = ops.relationshipTypeGetForName("BELONGS_TO");
            personLabelId = ops.labelGetForName("Person");
            personIdPropertyKeyId = ops.propertyKeyGetForName("id");

            for (String key :db.getAllPropertyKeys() ) {
                keys.put(key, ops.propertyKeyGetForName(key));
            }
            tx.success();
        }
    }

    @GET
    @Path("{username}/associates/{id}")
    public Response getAssociates(@PathParam("username") final String username,
                                  @PathParam("id") final String id,
                                  @Context GraphDatabaseService db) throws IOException, SchemaRuleNotFoundException, IndexBrokenKernelException, IndexNotFoundKernelException, EntityNotFoundException {
        ArrayList<Map<String,Object>> results = new ArrayList<>();
        MutableRoaringBitmap userPermissions = permissions.get(username);
        try (Transaction tx = db.beginTx()) {
            Node person = db.findNode(Labels.Person, "id", id);
            for (Relationship rel : person.getRelationships(Direction.BOTH, RelationshipType.withName("KNOWS"))) {
                Node other = rel.getOtherNode(person);
                Map<String, Object> properties = other.getAllProperties();
                Map<String, Object> filteredProperties = new HashMap<>();
                for (String key : properties.keySet()) {
                    Integer permission = toIntExact((other.getId() << 8) | (keys.get(key) & 0xF));
                    if(userPermissions.contains(permission)) {
                       filteredProperties.put(key, properties.get(key));
                    }
                }
                if(!filteredProperties.isEmpty()) {
                    results.add(filteredProperties);
                }
            }

            tx.success();
        }

        return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
    }

    @PUT
    @Path("{username}/associates/{id}/permissions/{property}")
    public Response addPermission(@PathParam("username") final String username,
                                  @PathParam("id") final String id,
                                  @PathParam("property") final String property,
                                  @Context GraphDatabaseService db) throws IOException {
        Integer keyId = keys.get(property);
        try (Transaction tx = db.beginTx()) {
            Node user = db.findNode(Labels.User, "username", username);
            tx.acquireWriteLock(user);
            byte[] bytes;
            MutableRoaringBitmap userPermissions = getRoaringBitmap(user);
            Node person = db.findNode(Labels.Person, "id", id);
            Integer permission = toIntExact((person.getId() << 8) | (keyId & 0xF));
            userPermissions.add(permission);
            userPermissions.runOptimize();
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            baos.reset();
            userPermissions.serialize(new DataOutputStream(baos));
            bytes = baos.toByteArray();
            user.setProperty("permissions", bytes);
            tx.success();
        }
        return Response.ok().build();
    }

    @DELETE
    @Path("{username}/associates/{id}/permissions/{property}")
    public Response removePermission(@PathParam("username") final String username,
                                  @PathParam("id") final String id,
                                  @PathParam("property") final String property,
                                  @Context GraphDatabaseService db) throws IOException {
        Integer keyId = keys.get(property);
        try (Transaction tx = db.beginTx()) {
            Node user = db.findNode(Labels.User, "username", username);
            tx.acquireWriteLock(user);
            MutableRoaringBitmap userPermissions = getRoaringBitmap(user);
            Node person = db.findNode(Label.label("Person"), "id", id);
            Integer permission = toIntExact((person.getId() << 8) | (keyId & 0xF));
            userPermissions.remove(permission);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            userPermissions.serialize(new DataOutputStream(baos));
            user.setProperty("permissions", baos.toByteArray());
            tx.success();
        }
        return Response.ok().build();
    }

    @PUT
    @Path("/group/{name}/associates/{id}/permissions/{property}")
    public Response addGroupPermission(@PathParam("name") final String name,
                                  @PathParam("id") final String id,
                                  @PathParam("property") final String property,
                                  @Context GraphDatabaseService db) throws IOException {
        Integer keyId = keys.get(property);
        try (Transaction tx = db.beginTx()) {
            Node group = db.findNode(Labels.Group, "name", name);
            tx.acquireWriteLock(group);
            byte[] bytes;
            MutableRoaringBitmap userPermissions = getRoaringBitmap(group);
            Node person = db.findNode(Labels.Person, "id", id);
            Integer permission = toIntExact((person.getId() << 8) | (keyId & 0xF));
            userPermissions.add(permission);
            userPermissions.runOptimize();
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            baos.reset();
            userPermissions.serialize(new DataOutputStream(baos));
            bytes = baos.toByteArray();
            group.setProperty("permissions", bytes);
            tx.success();
        }
        return Response.ok().build();
    }

    @DELETE
    @Path("/group/{name}/associates/{id}/permissions/{property}")
    public Response removeGroupPermission(@PathParam("name") final String name,
                                     @PathParam("id") final String id,
                                     @PathParam("property") final String property,
                                     @Context GraphDatabaseService db) throws IOException {
        Integer keyId = keys.get(property);
        try (Transaction tx = db.beginTx()) {
            Node group = db.findNode(Labels.Group, "name", name);
            tx.acquireWriteLock(group);
            MutableRoaringBitmap userPermissions = getRoaringBitmap(group);
            Node person = db.findNode(Label.label("Person"), "id", id);
            Integer permission = toIntExact((person.getId() << 8) | (keyId & 0xF));
            userPermissions.remove(permission);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            userPermissions.serialize(new DataOutputStream(baos));
            group.setProperty("permissions", baos.toByteArray());
            tx.success();
        }
        return Response.ok().build();
    }
}
