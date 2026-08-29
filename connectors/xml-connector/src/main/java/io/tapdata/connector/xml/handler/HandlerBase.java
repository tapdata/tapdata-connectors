package io.tapdata.connector.xml.handler;

import org.dom4j.Element;
import org.dom4j.Node;
import org.dom4j.tree.DefaultElement;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public interface HandlerBase {

    default Object afterAnalyzeElement(List<Node> newNodes) {
        if (newNodes.stream().map(Node::getPath).distinct().count() > 1) {
            Map<String, Object> subMap = new LinkedHashMap<>();
            newNodes.forEach(v -> subMap.put(v.getName(), analyzeElement((DefaultElement) v)));
            return subMap;
        } else {
            List<Object> subList = new ArrayList<>();
            newNodes.forEach(v -> subList.add(analyzeElement((DefaultElement) v)));
            return subList;
        }
    }

    Object analyzeElement(Element element);
}
