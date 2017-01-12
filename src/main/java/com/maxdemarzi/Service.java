package com.maxdemarzi;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.cursor.Cursor;
import org.neo4j.graphdb.*;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
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

    private static final LoadingCache<String, Integer> keys = Caffeine.newBuilder()
            .maximumSize(100)
            .build(propertyName -> getPropertyKey(propertyName));


    private static Integer getPropertyKey(String propertyName) {
        try (Transaction tx = dbapi.beginTx()) {
            ThreadToStatementContextBridge ctx = dbapi.getDependencyResolver().resolveDependency(ThreadToStatementContextBridge.class);
            ReadOperations ops = ctx.get().readOperations();
            return ops.propertyKeyGetForName(propertyName);
        }
    }

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
        } else {
            nodeIds = new byte[0];
        }

        try {
            rb.deserialize(new java.io.DataInputStream(new java.io.InputStream() {
                int c = 0;

                @Override
                public int read() {
                    return nodeIds[c++] & 0xff;
                }

                @Override
                public int read(byte b[]) {
                    return read(b, 0, b.length);
                }

                @Override
                public int read(byte[] b, int off, int l) {
                    System.arraycopy(nodeIds, c, b, off, l);
                    c += l;
                    return l;
                }
            }));
        } catch (IOException ioe) {
            // should never happen because we read from a byte array
            throw new RuntimeException("unexpected error while deserializing from a byte array");
        }


        //ByteArrayInputStream bais = new ByteArrayInputStream(nodeIds);
        //rb.deserialize(new DataInputStream(bais));
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

    public Service(@Context GraphDatabaseService db) {
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
            tx.success();
        }
    }

    @GET
    @Path("{username}/associates/{id}")
    public Response getAssociates(@PathParam("username") final String username,
                                  @PathParam("id") final String id,
                                  @Context GraphDatabaseService db) throws IOException, SchemaRuleNotFoundException, IndexBrokenKernelException, IndexNotFoundKernelException, EntityNotFoundException {
        ArrayList<Map<String,Object>> results = new ArrayList<>();

        try (Transaction tx = db.beginTx()) {
            MutableRoaringBitmap userPermissions = permissions.get(username);

            Node person = db.findNode(Label.label("Person"), "id", id);
            for (Relationship rel : person.getRelationships(Direction.BOTH, RelationshipType.withName("KNOWS"))) {
                Node other = rel.getOtherNode(person);
                Map<String, Object> properties = other.getAllProperties();
                for (String key : properties.keySet()) {
                    Integer permission = toIntExact((other.getId() * 100) + keys.get(key));
                    if(!userPermissions.contains(permission)) {
                        properties.remove(key);
                    }
                }
                if(!properties.isEmpty()) {
                    results.add(properties);
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
        try (Transaction tx = db.beginTx()) {
            Node user = db.findNode(Label.label("User"), "username", username);
            tx.acquireWriteLock(user);
            byte[] bytes = new byte[0];
            try {
                MutableRoaringBitmap userPermissions = getRoaringBitmap(user);
                Node person = db.findNode(Label.label("Person"), "id", id);
                Integer permission = toIntExact((person.getId() << 8) + keys.get(property));
                userPermissions.add(permission);
                userPermissions.runOptimize();
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                userPermissions.serialize(new DataOutputStream(baos));
                bytes = baos.toByteArray();
            } catch ( Exception e ) {
                System.out.println(e);
            }
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
        try (Transaction tx = db.beginTx()) {
            Node user = db.findNode(Label.label("User"), "username", username);
            tx.acquireWriteLock(user);
            MutableRoaringBitmap userPermissions = getRoaringBitmap(user);
            Node person = db.findNode(Label.label("Person"), "id", id);
            Integer permission = toIntExact((person.getId() << 8) + keys.get(property));
            userPermissions.remove(permission);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            userPermissions.serialize(new DataOutputStream(baos));
            user.setProperty("permissions", baos.toByteArray());
            tx.success();
        }
        return Response.ok().build();
    }
}
