package com.netflix.exhibitor.core.importandexport;

import com.google.common.base.Strings;
import com.netflix.exhibitor.core.Exhibitor;
import com.netflix.exhibitor.core.rest.UIContext;
import com.sun.jersey.core.util.Base64;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.data.ACL;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

import java.util.Iterator;
import java.util.List;

public class Exporter {
    private String startPath;
    private Exhibitor exhibitor;
    private UIContext context;

    public Exporter(UIContext context, String startPath)
    {
        this.context = context;
        this.exhibitor = context.getExhibitor();

        if (Strings.isNullOrEmpty(startPath)) {
            this.startPath = "/";
        } else {
            if (startPath.startsWith("/")) {
                this.startPath = startPath;
            } else {
                this.startPath = "/" + startPath;
            }
        }
    }

    public String generate() throws Exception
    {
        ArrayNode jsonArray = JsonNodeFactory.instance.arrayNode();

        return convertToExportFormat(getChildren(startPath, jsonArray));
    }

    private String convertToExportFormat(ArrayNode jsonArray)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("[\r\n");

        Iterator<JsonNode> iterator = jsonArray.iterator();
        while (iterator.hasNext()) {
            JsonNode jsonNode = iterator.next();

            sb.append(convertToFlatJsonAsString(jsonNode.get("path").getTextValue(), jsonNode.get("data").getTextValue(),
                    jsonNode.get("acls").getElements()));

            if (iterator.hasNext()) {
                sb.append(",\r\n");
            }
        }

        sb.append("\r\n]");

        return sb.toString();
    }

    private String convertToFlatJsonAsString(String path, String data, Iterator<JsonNode> acls)
    {
        StringBuilder node = new StringBuilder();
        node.append("{\"path\": \"");
        node.append(path);
        node.append("\", \"data\": \"");
        node.append(data);
        node.append("\", \"acls\": [");


        while(acls.hasNext()) {
            JsonNode aclNode = acls.next();
            node.append("{\"scheme\": \"");
            node.append(aclNode.get("scheme").getTextValue());
            node.append("\", \"id\": \"");
            node.append(aclNode.get("id").getTextValue());
            node.append("\", \"perms\": ");
            node.append(aclNode.get("perms").getIntValue());
            node.append("}");

            if (acls.hasNext()) {
                node.append(", ");
            }
        }

        node.append("]}");

        return node.toString();
    }

    /**
     * Gets the list of children for a specific path. Then for each result it calls this method again. Eventually
     * we will have traversed the entire tree.
     *
     * @param path
     * @return ArrayNode
     * @throws Exception
     */
    private ArrayNode getChildren(String path, ArrayNode jsonArray) throws Exception
    {
        List<String> children = exhibitor.getLocalConnection().getChildren().forPath(path);
        jsonArray.add(getNodeDetails(path));

        for (String child : children) {
            getChildren(ZKPaths.makePath(path, child), jsonArray);
        }

        return jsonArray;
    }

    private JsonNode getNodeDetails(String path) throws Exception
    {
        byte[] data = exhibitor.getLocalConnection().getData().forPath(path);
        List<ACL> acls = exhibitor.getLocalConnection().getACL().forPath(path);

        ObjectNode node = JsonNodeFactory.instance.objectNode();
        node.put("path", path);
        node.put("data", new String(Base64.encode(data)));

        ArrayNode aclsArray = JsonNodeFactory.instance.arrayNode();
        for (ACL acl : acls) {
            ObjectNode aclNode = JsonNodeFactory.instance.objectNode();

            aclNode.put("scheme", acl.getId().getScheme());
            aclNode.put("id", acl.getId().getId());
            aclNode.put("perms", acl.getPerms());

            aclsArray.add(aclNode);
        }

        node.put("acls", aclsArray);

        return node;
   }
}
