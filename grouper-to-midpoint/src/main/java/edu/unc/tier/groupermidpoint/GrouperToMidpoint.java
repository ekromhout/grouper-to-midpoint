package edu.unc.tier.groupermidpoint;

import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import com.rabbitmq.client.*;
import java.util.List;
import java.util.Iterator;
import java.io.IOException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import java.util.ArrayList;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.apache.http.entity.StringEntity;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.auth.AuthenticationException; 
import java.io.BufferedReader;
import java.io.InputStreamReader;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.ParserConfigurationException;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;
import java.io.StringReader;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.w3c.dom.bootstrap.DOMImplementationRegistry;
import org.w3c.dom.ls.DOMImplementationLS;
import org.w3c.dom.ls.LSSerializer;
import java.io.StringWriter;
import java.io.Writer;
import org.w3c.dom.ls.LSOutput;
import java.lang.InstantiationException;



public class GrouperToMidpoint {

  /* Exchange to which you have configured the grouper messages to route */

  private static final String EXCHANGE_NAME = "amq.topic";
  private static final String EXCHANGE_HOST = "localhost";
  private static final String EXCHANGE_USER = "Guest";
  private static final String EXCHANGE_SECRET = "Guest";
  private static final String MIDPOINT_REST_URL = "http://localhost:8080/midpoint/ws/rest/";
  private static final String MIDPOINT_REST_USER = "administrator";
  private static final String MIDPOINT_REST_SECRET = "5ecr3t";
  private static final boolean DEBUG = true;

  public static void main(String[] argv) throws Exception {

    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost(EXCHANGE_HOST);
    factory.setUsername(EXCHANGE_USER);
    factory.setPassword(EXCHANGE_SECRET);
    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();
    boolean durable = true;
    channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC, durable);
    String queueName = channel.queueDeclare().getQueue();

    /* Create hashmaps for the resources and roles you
       want to sync over to midpoint. The keys here are 
       Grouper fully qualified group names. Those are also
       the routing keys in the Unicon rabbitmq implementation. */

    final HashMap<String, String> roleGroups = new HashMap<String, String>();
    final HashMap<String, String> resourceGroups = new HashMap<String, String>();
    //resourceGroups.put("stem1:group5","Test CSV: username");
    resourceGroups.put("app:drupal_authorized","Drupal");
    roleGroups.put("ref:employee","employee");
   

    /* TODO The above static strings and HashMaps should be fed from a property file.  */

    /* Bind to each of the desired group names / routing keys. */

    for ( Map.Entry<String, String> entry  :  resourceGroups.entrySet()) {
      String bindingKey = entry.getKey();
      channel.queueBind(queueName, EXCHANGE_NAME, bindingKey);
    }
    for ( Map.Entry<String, String> entry   : roleGroups.entrySet()) {
      String bindingKey = entry.getKey();
      channel.queueBind(queueName, EXCHANGE_NAME, bindingKey);
    }

    /* The below implements a listener for messages by subsription
       also known as the "Push API" so this class has to be running
       when the messages you are interested in are published 
       or within the messages TTL.
     */

    if (DEBUG) {
       System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
    }

    Consumer consumer = new DefaultConsumer(channel) {
      @Override
      public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
        String message = new String(body, "UTF-8");
        if (DEBUG) {
          System.out.println(" [x] Received '" + envelope.getRoutingKey() + "':'" + message + "'");
        }
        String[] messageDetails = parseMessage(message);
        String action = messageDetails[0];
        String group = messageDetails[1];
        String subjectId = messageDetails[2];

        /* The method getOidForName is just a helper method
           so you don't have to put OIDs in your configuration
           it was most needed for getting user OIDs by name
           but is handy for resources and roles as well.
         */

        String subjectOid = getOidForName(subjectId,"users");
        //System.out.println("--" + subjectOid + "--");
        String type = "";
        String resourceOrRoleOid = "";
        String patchStatus = "";
        if ( resourceGroups.containsKey(group) ) {
           type = "resources";
           resourceOrRoleOid = getOidForName(resourceGroups.get(group), type);  
           patchStatus = assignRoleOrResource ( subjectOid, resourceOrRoleOid, type, action);
        }
        if ( roleGroups.containsKey(group) ) {
           type = "roles";
           resourceOrRoleOid = getOidForName(roleGroups.get(group), type);  
           patchStatus = assignRoleOrResource ( subjectOid, resourceOrRoleOid, type, action);
        }

        if ( DEBUG ) {
          System.out.println("Sent action " + action + " for subject " + subjectId + "(" + subjectOid + ")" + " to " + type + " OID " + resourceOrRoleOid + " because of group " + group  + " with HTTP status result of " + patchStatus + ".");
        }

        }
    };
    channel.basicConsume(queueName, true, consumer);
  }
  private static String[] parseMessage(String message) {
 
  /* We are just intested in a few components of the payload
     of the AMQP messages, so this extracts the event type
     (MEMBERSHIP_ADD or MEMBERSHIP_DELETE) the fully qualified
     group name, and the Grouper subject ID.
   */

    String eventType = "";
    String groupName = "";
    String subjectId = "";
    try {
          JSONParser jsonParser = new JSONParser();
          JSONObject jsonObject = (JSONObject) jsonParser.parse(message);
          JSONArray esbEvent = (JSONArray) jsonObject.get("esbEvent");
          Iterator<?> i = esbEvent.iterator();
          while (i.hasNext()) 
          {
            JSONObject innerObj = (JSONObject) i.next();
            eventType = (String) innerObj.get("eventType");
            groupName = (String) innerObj.get("groupName");
            subjectId = (String) innerObj.get("subjectId");
          }
        }
        catch (Exception e) {
           System.err.println(e.getMessage());
        }
    return new String[] { eventType, groupName, subjectId }; 
  } 
  private static String getOidForName(String name, String type) {

    /* In Midpoint, most objects need to be referenced by OID in
       the rest API. In order to avoid hard coding references to
       specific OIDs in configuration, this function allows for 
       searching by name to retrieve an OID. This also avoids 
       having to  change configuration in case you are changing
       Midpoint instances, OIDs are generated and therefore 
       unique between installs.
     */

    String oid = "";
    try {

      /* Build a HTTP POST client, search operations in the Midpoint REST API are POSTS */

      CloseableHttpClient client = HttpClients.createDefault();
      HttpPost httpPost = new HttpPost( MIDPOINT_REST_URL + type + "/search");
      httpPost.setHeader("Content-Type", "application/xml");
      UsernamePasswordCredentials creds = new UsernamePasswordCredentials(MIDPOINT_REST_USER,MIDPOINT_REST_SECRET);
      httpPost.addHeader(new BasicScheme().authenticate(creds, httpPost, null));

      /* The Midpoint examples I found for the REST API were XML 
         so this builds a document based on the examples published
         at https://github.com/Evolveum/midpoint/tree/master/samples/rest */

      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      DocumentBuilder builder = factory.newDocumentBuilder();
      Document doc = builder.newDocument();
      Element query = doc.createElement("q:query");
      query.setAttribute("xmlns:q", "http://prism.evolveum.com/xml/ns/public/query-3");
      query.setAttribute("xmlns:c", "http://midpoint.evolveum.com/xml/ns/public/common/common-3");
      doc.appendChild(query);
      Element queryFilter = doc.createElement("q:filter");
      query.appendChild(queryFilter);
      Element queryCompare = doc.createElement("q:equal");
      queryFilter.appendChild(queryCompare);
      Element queryPath = doc.createElement("q:path");
      queryPath.insertBefore(doc.createTextNode("c:name"), queryPath.getLastChild());
      queryCompare.appendChild(queryPath);
      Element queryValue = doc.createElement("q:value");
      queryValue.insertBefore(doc.createTextNode(name), queryValue.getLastChild());
      queryCompare.appendChild(queryValue);
      final DOMImplementationRegistry registry = DOMImplementationRegistry.newInstance();
      final DOMImplementationLS impl = (DOMImplementationLS) registry.getDOMImplementation("LS");
      final LSSerializer writer = impl.createLSSerializer();
      writer.getDomConfig().setParameter("format-pretty-print", Boolean.TRUE);
      writer.getDomConfig().setParameter("xml-declaration", Boolean.TRUE);

      /* The below nonsense is to get a UTF-8 XML declaration,
         the declaration defaulted to UTF-16 in my testing, 
         and sending a UTF-16 declaration to Midpoint threw
         a 500 for me. This could just be a configuration issue.
       */

      LSOutput lsOutPut = impl.createLSOutput();
      lsOutPut.setEncoding("UTF-8");
      Writer stringWriter = new StringWriter();
      lsOutPut.setCharacterStream(stringWriter);
      writer.write(doc,lsOutPut);
      StringEntity stringEntity = new StringEntity(stringWriter.toString());
      httpPost.setEntity(stringEntity);
      CloseableHttpResponse response = client.execute(httpPost);
      BufferedReader rd = new BufferedReader( new InputStreamReader(response.getEntity().getContent()));
      StringBuffer result = new StringBuffer();
      String line = "";
       while ((line = rd.readLine()) != null) {
         result.append(line);
      } 
      doc = builder.parse( new InputSource(new StringReader(result.toString())));
      doc.getDocumentElement().normalize();

      /* Parse the document to get the OID, these had the form
         <apti:object oid="02c1c332-7074-4d92-9f16-bc4da9924306" version="0" xsi:type="c:RoleType">
         That example is from a Role search in my testing,
         but were similar for Users and Resources.
       */

      NodeList nList = doc.getElementsByTagName("apti:object");
      for (int temp = 0; temp < nList.getLength(); temp++) {
        Node nNode = nList.item(temp);
        if (nNode.getNodeType() == Node.ELEMENT_NODE) {
          Element eElement = (Element) nNode;
          oid = eElement.getAttribute("oid");
        }
      }

    } catch (ClassNotFoundException CNFE) {
       System.err.println(CNFE.getMessage());
       CNFE.printStackTrace();

    } catch (IllegalAccessException IAE) {
       System.err.println(IAE.getMessage());
       IAE.printStackTrace();

    } catch (InstantiationException IE) {
       System.err.println(IE.getMessage());
       IE.printStackTrace();

    } catch (IOException IOE) {
       System.err.println(IOE.getMessage());
       IOE.printStackTrace();

    } catch (SAXException SAXE) {
       System.err.println(SAXE.getMessage());
       SAXE.printStackTrace();

    } catch (AuthenticationException AE) {
       System.err.println(AE.getMessage());
       AE.printStackTrace();

    } catch (ParserConfigurationException PCE) {
       System.err.println(PCE.getMessage());
       PCE.printStackTrace();
    
    } catch (Exception e) {
      /* Shouldn't get here... */
      e.printStackTrace();
    }

    return new String(oid);
  }
  private static String assignRoleOrResource(String subjectOid, String resourceOrRoleOid, String type, String action) {

    /* In Midpoint, roles and resources are added to user objects
       so the user OID gets built into the rest URL, and then
       the role or resource OID is included in the XML document
       with the action to take.
     */

    String patchStatus = "";
    try {

      /* Build a HTTP PATCH client, update operations in the Midpoint REST API are PATCHES */

      String midpointAction = "";
      if (action.equals("MEMBERSHIP_ADD")) {
        midpointAction = "add";
      } else if (action.equals("MEMBERSHIP_DELETE"))  {
        midpointAction = "delete";
      } else {
        return "Error, not defined action for " + action + "aborting patch.";
      }
      CloseableHttpClient client = HttpClients.createDefault();
      HttpPatch httpPatch = new HttpPatch( MIDPOINT_REST_URL +  "/users/" + subjectOid);
      httpPatch.setHeader("Content-Type", "application/xml");
      UsernamePasswordCredentials creds = new UsernamePasswordCredentials(MIDPOINT_REST_USER,MIDPOINT_REST_SECRET);
      httpPatch.addHeader(new BasicScheme().authenticate(creds, httpPatch, null));

      /* The Midpoint examples I found for the REST API were XML 
         so this builds a document based on the examples published
         at https://github.com/Evolveum/midpoint/tree/master/samples/rest */

      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      DocumentBuilder builder = factory.newDocumentBuilder();
      Document doc = builder.newDocument();
      Element objectModification = doc.createElement("objectModification");
      doc.appendChild(objectModification);
      objectModification.setAttribute("xmlns:c", "http://midpoint.evolveum.com/xml/ns/public/common/common-3");
      objectModification.setAttribute("xmlns", "http://midpoint.evolveum.com/xml/ns/public/common/api-types-3");
      objectModification.setAttribute("xmlns:t", "http://prism.evolveum.com/xml/ns/public/types-3");
      Element itemDelta = doc.createElement("itemDelta");
      objectModification.appendChild(itemDelta);
      Element modificationType = doc.createElement("t:modificationType");
      modificationType.insertBefore(doc.createTextNode(midpointAction),modificationType.getLastChild());
      itemDelta.appendChild(modificationType);
      Element modificationPath = doc.createElement("t:path");
      modificationPath.insertBefore(doc.createTextNode("c:assignment"),modificationPath.getLastChild());
      itemDelta.appendChild(modificationPath);
      Element modificationValue = doc.createElement("t:value");
 
      /* There are small differences between the XML
         for a resource and a role. The two specific 
         examples I used to build these are:
         https://github.com/Evolveum/midpoint/blob/master/samples/rest/modification-assign-account.xml
         for the resource and:
         https://github.com/Evolveum/midpoint/blob/master/samples/rest/modify-user-assign-role.xml
         for the role.
       */

      if ( type.equals("resources")) {
        Element modificationConstruction = doc.createElement("c:construction");
        modificationConstruction.setAttribute("xmlns:icfs", "http://midpoint.evolveum.com/xml/ns/public/connector/icf-1/resource-schema-3");
        Element modificationResource = doc.createElement("c:resourceRef");
        modificationResource.setAttribute("oid",resourceOrRoleOid);
        modificationConstruction.appendChild(modificationResource);
        modificationValue.appendChild(modificationConstruction);
      }
      else {
        Element modificationRole = doc.createElement("c:targetRef");
        modificationRole.setAttribute("oid",resourceOrRoleOid);
        modificationRole.setAttribute("type","c:RoleType");
        modificationValue.appendChild(modificationRole);
      }
      itemDelta.appendChild(modificationValue);

      final DOMImplementationRegistry registry = DOMImplementationRegistry.newInstance();
      final DOMImplementationLS impl = (DOMImplementationLS) registry.getDOMImplementation("LS");
      final LSSerializer writer = impl.createLSSerializer();
      writer.getDomConfig().setParameter("format-pretty-print", Boolean.TRUE);
      writer.getDomConfig().setParameter("xml-declaration", Boolean.TRUE);

      /* The below nonsense is to get a UTF-8 XML declaration,
         the declaration defaulted to UTF-16 in my testing, 
         and sending a UTF-16 declaration to Midpoint threw
         a 500 for me. This could just be a configuration issue.
       */

      LSOutput lsOutPut = impl.createLSOutput();
      lsOutPut.setEncoding("UTF-8");
      Writer stringWriter = new StringWriter();
      lsOutPut.setCharacterStream(stringWriter);
      writer.write(doc,lsOutPut);
      StringEntity stringEntity = new StringEntity(stringWriter.toString());
      httpPatch.setEntity(stringEntity);
      CloseableHttpResponse response = client.execute(httpPatch);
      if ( response.getStatusLine().getStatusCode() >= 200 && response.getStatusLine().getStatusCode() < 300 ) {
        patchStatus = "Success " + response.getStatusLine().getStatusCode();
      }
      else {
        patchStatus = "Error " + response.getStatusLine().getStatusCode();
      }

    } catch (ClassNotFoundException CNFE) {
       System.err.println(CNFE.getMessage());
       CNFE.printStackTrace();

    } catch (IllegalAccessException IAE) {
       System.err.println(IAE.getMessage());
       IAE.printStackTrace();

    } catch (InstantiationException IE) {
       System.err.println(IE.getMessage());
       IE.printStackTrace();

    } catch (IOException IOE) {
       System.err.println(IOE.getMessage());
       IOE.printStackTrace();

    } catch (AuthenticationException AE) {
       System.err.println(AE.getMessage());
       AE.printStackTrace();

    } catch (ParserConfigurationException PCE) {
       System.err.println(PCE.getMessage());
       PCE.printStackTrace();
    
    } catch (Exception e) {
      /* Shouldn't get here... */
      e.printStackTrace();
    }

    return new String(patchStatus);
  }
}

