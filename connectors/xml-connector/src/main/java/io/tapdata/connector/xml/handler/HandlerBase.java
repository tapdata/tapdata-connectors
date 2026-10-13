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
            Map<String, List<Object>> repeatedValues = new LinkedHashMap<>();
            newNodes.forEach(v -> {
                String name = v.getName();
                Object value = analyzeElement((DefaultElement) v);
                if (!subMap.containsKey(name)) {
                    subMap.put(name, value);
                    return;
                }
                List<Object> values = repeatedValues.computeIfAbsent(name, ignored -> {
                    List<Object> initialValues = new ArrayList<>();
                    initialValues.add(subMap.get(name));
                    subMap.put(name, initialValues);
                    return initialValues;
                });
                values.add(value);
            });
            return subMap;
        } else {
            List<Object> subList = new ArrayList<>();
            newNodes.forEach(v -> subList.add(analyzeElement((DefaultElement) v)));
            return subList;
        }
    }

    Object analyzeElement(Element element);
}
