package com.arangodb.tinkerpop.gremlin.client;

import com.arangodb.entity.EdgeDefinition;
import com.arangodb.tinkerpop.gremlin.utils.ArangoDBUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class ArngEdgeDefinitions implements EdgeDefinitions {

    private final GraphClient graphClient;

    public ArngEdgeDefinitions(GraphClient graphClient) {
        this.graphClient = graphClient;
    }

    @Override
    public List<EdgeDefinition> createEdgeDefinitions(
            List<String> vertexCols,
            List<String> edgeCols,
            Collection<String> relations) throws MalformedRelationException {

        final List<EdgeDefinition> edgeDefinitions;
        if (relations.isEmpty()) {
            // logger.info("No relations, creating default ones.");
            edgeDefinitions = createDefaultEdgeDefinitions(vertexCols, edgeCols);
        } else {
            edgeDefinitions = new ArrayList<>();
            for (String value : relations) {
                edgeDefinitions.add(relationPropertyToEdgeDefinition(value));
            }
        }
        return edgeDefinitions;
    }

    @Override
    public String edgeDefinitionToString(EdgeDefinition ed) {
        return String.format("[%s]: %s->%s", ed.getCollection(), ed.getFrom(), ed.getTo());
    }


    /**
     * Creates the default edge definitions. When no relations are provided, the graph schema is
     * assumed to be fully connected, i.e. there is an EdgeDefintion for each possible combination
     * of Vertex-Edge-Vertex triplets.
     *
     * @param verticesCollectionNames 	the vertex collection names
     * @param edgesCollectionNames 		the edge collection names
     * @return the list of edge definitions
     */

    private List<EdgeDefinition> createDefaultEdgeDefinitions(
        List<String> verticesCollectionNames,
        List<String> edgesCollectionNames) {
        List<EdgeDefinition> result = new ArrayList<>();
        for (String e : edgesCollectionNames) {
            for (String from : verticesCollectionNames) {
                for (String to : verticesCollectionNames) {
                    EdgeDefinition ed = new EdgeDefinition()
                            .collection(e)
                            .from(from)
                            .to(to);
                    result.add(ed);
                }
            }
        }
        return result;
    }

    /**
     * Create an EdgeDefinition from a relation in the Configuration. The format of a relation is:
     * <pre>
     * collection:from-&gt;to
     * </pre>
     * Where collection is the name of the Edge collection, and to and from are comma separated list of
     * node collection names.
     *
     * @param relation 				the relation
     * @return an EdgeDefinition that represents the relation.
     * @throws ArangoDBGraphException if the relation is malformed
     */

    private EdgeDefinition relationPropertyToEdgeDefinition(String relation) throws MalformedRelationException {
        // logger.info("Creating EdgeRelation from {}", relation);
        EdgeDefinition result = new EdgeDefinition();
        String[] info = relation.split(":");
        if (info.length != 2) {
            throw new MalformedRelationException("Error in configuration. Malformed relation " + relation);
        }
        result.collection(graphClient.getPrefixedCollectioName(info[0]));
        info = info[1].split("->");
        if (info.length != 2) {
            throw new MalformedRelationException("Error in configuration. Malformed relation> " + relation);
        }
        List<String> trimmed = Arrays.stream(info[0].split(","))
                .map(String::trim)
                .map(c -> graphClient.getPrefixedCollectioName(c))
                .collect(Collectors.toList());
        String[] from = new String[trimmed.size()];
        from = trimmed.toArray(from);

        trimmed = Arrays.stream(info[1].split(","))
                .map(String::trim)
                .map(c -> graphClient.getPrefixedCollectioName(c))
                .collect(Collectors.toList());
        String[] to = new String[trimmed.size()];
        to = trimmed.toArray(to);
        result.from(from).to(to);
        return result;
    }

}
