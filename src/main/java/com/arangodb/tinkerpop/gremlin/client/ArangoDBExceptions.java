package com.arangodb.tinkerpop.gremlin.client;

import com.arangodb.ArangoDBException;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Common exceptions to use with an ArangoDB. This class is intended to translate ArangoDB
 * error codes into meaningful exceptions with standard messages. ArangoDBException exception
 * is a RuntimeException intended to stop execution.
 */

public class ArangoDBExceptions {

    /** Regex to matches response messages from the ArangoDB client */

    public static Pattern ERROR_CODE = Pattern.compile("^Response:\\s\\d+,\\sError:\\s(\\d+)\\s-\\s([a-z\\s]+).+");

    /** "name to long" Message. */

    public static String NAME_TO_LONG = "Name is too long: {} bytes (max 64 bytes for labels, 256 for keys)";

/**
* Instantiates a new ArangoDB exceptions.
*/

private ArangoDBExceptions() { }

/**
* Translate ArangoDB Error code into exception (@see <a href="https://docs.arangodb.com/latest/Manual/Appendix/ErrorCodes.html">Error codes</a>)
*
* @param ex the ex
* @return The ArangoDBClientException
*/

public static ArangoDBGraphException getArangoDBException(ArangoDBException ex) {
        String errorMessage = ex.getMessage();
        Matcher m = ERROR_CODE.matcher(errorMessage);
        if (m.matches()) {
            int code = Integer.parseInt(m.group(1));
            String msg = m.group(2);
            switch (code/100) {
                case 10:	// Internal ArangoDB storage errors
                    return new ArangoDBGraphException(code, String.format("Internal ArangoDB storage error (%s): %s", code, msg), ex);
                case 11:
                    return new ArangoDBGraphException(code, String.format("External ArangoDB storage error (%s): %s", code, msg), ex);
                case 12:
                    return new ArangoDBGraphException(code, String.format("General ArangoDB storage error (%s): %s", code, msg), ex);
                case 13:
                    return new ArangoDBGraphException(code, String.format("Checked ArangoDB storage error (%s): %s", code, msg), ex);
                case 14:
                    return new ArangoDBGraphException(code, String.format("ArangoDB replication/cluster error (%s): %s", code, msg), ex);
                case 15:
                    return new ArangoDBGraphException(code, String.format("ArangoDB query error (%s): %s", code, msg), ex);
                case 19:
                    return new ArangoDBGraphException(code, String.format("Graph / traversal errors (%s): %s", code, msg), ex);
                default:
                    return new ArangoDBGraphException(String.format("General ArangoDB error (unkown error code - )", code), ex);
            }
        }
        return new ArangoDBGraphException("General ArangoDB error (unkown error code)", ex);
}

/**
* Gets the naming convention error.
*
* @param cause the cause
* @param details the details
* @return the naming convention error
*/

public static ArangoDBGraphException getNamingConventionError(String cause, String details) {
return new ArangoDBGraphException("The provided label or key name does not satisfy the naming conventions." +
String.format(cause, details));
}

/**
* Error persisting element property.
*
* @param ex the ex
* @return the arango DB graph exception
*/

public static ArangoDBGraphException errorPersistingElmenentProperty(ArangoDBGraphException ex) {
return new ArangoDBGraphException("Error persisting property in element. ", ex);
}
}
