package io.tapdata.connector.paimon.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Explicit source-field dependencies for connector-generated target columns. */
final class PaimonGeneratedFieldDependencies {

    private static final PaimonGeneratedFieldDependencies NONE =
            new PaimonGeneratedFieldDependencies(Collections.emptyMap());

    private final Map<String, List<String>> dependencies;

    private PaimonGeneratedFieldDependencies(
            Map<String, ? extends Collection<String>> dependencies) {
        LinkedHashMap<String, List<String>> copy = new LinkedHashMap<>();
        for (Map.Entry<String, ? extends Collection<String>> entry :
                Objects.requireNonNull(dependencies, "dependencies").entrySet()) {
            String targetField = Objects.requireNonNull(entry.getKey(), "generatedTargetField");
            Collection<String> sourceFields =
                    Objects.requireNonNull(entry.getValue(), "generatedSourceFields");
            if (sourceFields.isEmpty()) {
                throw new IllegalArgumentException(
                        "Generated target field must declare source dependencies: " + targetField);
            }
            ArrayList<String> sourceCopy = new ArrayList<>();
            for (String sourceField : sourceFields) {
                sourceCopy.add(Objects.requireNonNull(sourceField, "generatedSourceField"));
            }
            copy.put(targetField, Collections.unmodifiableList(sourceCopy));
        }
        this.dependencies = Collections.unmodifiableMap(copy);
    }

    static PaimonGeneratedFieldDependencies none() {
        return NONE;
    }

    static PaimonGeneratedFieldDependencies of(
            Map<String, ? extends Collection<String>> dependencies) {
        Objects.requireNonNull(dependencies, "dependencies");
        return dependencies.isEmpty() ? NONE : new PaimonGeneratedFieldDependencies(dependencies);
    }

    Set<String> generatedTargetFields() {
        return dependencies.keySet();
    }

    List<String> sourceDependencies(String generatedTargetField) {
        List<String> sourceFields = dependencies.get(generatedTargetField);
        return sourceFields == null ? Collections.emptyList() : sourceFields;
    }
}
