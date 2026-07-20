package io.tapdata.connector.paimon.service;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.tapdata.entity.utils.cache.KVMap;
import org.apache.paimon.Snapshot;
import org.apache.paimon.fs.Path;
import org.apache.paimon.table.FileStoreTable;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

/** Durable, versioned Paimon commit-user and identifier state for one physical table. */
final class PaimonCommitStateStore implements PaimonTableWriteContext.CommitStateStore {

    private static final int VERSION = 1;
    private static final Gson GSON = new Gson();
    private static final Pattern NON_NEGATIVE_LONG = Pattern.compile("0|[1-9][0-9]*");
    private static final Set<String> FIELDS =
            new HashSet<>(Arrays.asList("version", "commitUser", "nextCommitIdentifier"));

    private final KVMap<Object> stateMap;
    private final String stateKey;
    private final String commitUser;

    private PaimonCommitStateStore(
            KVMap<Object> stateMap, String stateKey, String commitUser) {
        this.stateMap = stateMap;
        this.stateKey = stateKey;
        this.commitUser = commitUser;
    }

    static Binding bind(
            KVMap<Object> stateMap,
            String warehouse,
            FileStoreTable table) throws Exception {
        if (stateMap == null) {
            throw new IllegalStateException("Tap task state map is required for stable Paimon commits");
        }

        String key = stateKey(warehouse, table.location().toUri().toString());
        State candidate =
                new State(
                        VERSION,
                        "tapdata-paimon-" + UUID.randomUUID().toString().replace("-", ""),
                        0L);
        Object stored = stateMap.get(key);
        if (stored == null) {
            String candidateJson = encode(candidate);
            Object raced = stateMap.putIfAbsent(key, candidateJson);
            stored = raced == null ? stateMap.get(key) : raced;
            if (stored == null) {
                throw new IllegalStateException("Paimon commit state was not visible after putIfAbsent");
            }
        }

        State winner = parse(stored);
        PaimonCommitStateStore store =
                new PaimonCommitStateStore(stateMap, key, winner.commitUser);

        Optional<Snapshot> latest =
                table.snapshotManager()
                        .latestSnapshotOfUserFromFilesystem(winner.commitUser);
        long snapshotNext = latest.map(PaimonCommitStateStore::nextIdentifier).orElse(0L);
        long reconciledNext = Math.max(winner.nextCommitIdentifier, snapshotNext);
        if (reconciledNext != winner.nextCommitIdentifier) {
            // Persist reconciliation before any writer/router using this state can be published.
            store.save(reconciledNext);
        }
        return new Binding(winner.commitUser, reconciledNext, store, key);
    }

    @Override
    public synchronized void save(long nextCommitIdentifier) {
        if (nextCommitIdentifier < 0L) {
            throw new IllegalArgumentException("Negative Paimon next commit identifier");
        }

        State current = parse(stateMap.get(stateKey));
        if (!commitUser.equals(current.commitUser)) {
            throw new IllegalStateException(
                    "Paimon commit user changed in task state; another writer may own the table");
        }
        if (nextCommitIdentifier < current.nextCommitIdentifier) {
            throw new IllegalStateException("Paimon commit identifier state moved backwards");
        }
        if (nextCommitIdentifier == current.nextCommitIdentifier) {
            return;
        }

        stateMap.put(
                stateKey,
                encode(new State(VERSION, commitUser, nextCommitIdentifier)));
        State verified = parse(stateMap.get(stateKey));
        if (!commitUser.equals(verified.commitUser)
                || verified.nextCommitIdentifier != nextCommitIdentifier) {
            throw new IllegalStateException("Paimon commit state update was not durably observable");
        }
    }

    static String stateKey(String warehouse, String physicalLocation) {
        return "paimon.commit-state-v1."
                + sha256(normalizeIdentity(warehouse))
                + "."
                + physicalTableHash(physicalLocation);
    }

    static String physicalTableHash(String physicalLocation) {
        return sha256(normalizeIdentity(physicalLocation));
    }

    /** Remove credentials/query/fragment and normalize path semantics before hashing an identity. */
    static String normalizeIdentity(String value) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("Paimon state identity must not be blank");
        }
        try {
            URI parsed;
            try {
                parsed = new URI(value.trim()).normalize();
            } catch (URISyntaxException invalidEscaping) {
                // Paimon Path explicitly accepts unescaped local paths (spaces, CJK, etc.) and
                // creates a normalized encoded URI. Catalog initialization uses the same model.
                parsed = new Path(value.trim()).toUri().normalize();
            }
            parsed = URI.create(parsed.toASCIIString()).normalize();
            String scheme = parsed.getScheme();
            if (scheme != null) {
                scheme = scheme.toLowerCase(java.util.Locale.ROOT);
            }
            String authority = parsed.getRawAuthority();
            if (authority != null) {
                int userInfoEnd = authority.lastIndexOf('@');
                if (userInfoEnd >= 0) {
                    authority = authority.substring(userInfoEnd + 1);
                }
                authority = authority.toLowerCase(java.util.Locale.ROOT);
            }
            String path = parsed.getRawPath();
            StringBuilder identity = new StringBuilder();
            if (scheme != null) {
                identity.append(scheme).append(':');
            }
            if (authority != null) {
                identity.append("//").append(authority);
            }
            if (path != null) {
                identity.append(path);
            }
            String normalized = identity.toString();
            while (normalized.endsWith("/") && !"/".equals(path)) {
                normalized = normalized.substring(0, normalized.length() - 1);
            }
            return normalized;
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("Invalid Paimon state identity URI", e);
        }
    }

    private static long nextIdentifier(Snapshot snapshot) {
        long identifier = snapshot.commitIdentifier();
        if (identifier == Long.MAX_VALUE) {
            throw new IllegalStateException("Paimon commit identifier is exhausted");
        }
        if (identifier < 0L) {
            throw new IllegalStateException("Paimon snapshot contains a negative commit identifier");
        }
        return identifier + 1L;
    }

    private static String sha256(String value) {
        try {
            byte[] digest =
                    MessageDigest.getInstance("SHA-256")
                            .digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder result = new StringBuilder(digest.length * 2);
            for (byte b : digest) {
                result.append(String.format("%02x", b & 0xff));
            }
            return result.toString();
        } catch (Exception e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    private static String encode(State state) {
        JsonObject json = new JsonObject();
        json.addProperty("version", state.version);
        json.addProperty("commitUser", state.commitUser);
        json.addProperty("nextCommitIdentifier", state.nextCommitIdentifier);
        return GSON.toJson(json);
    }

    static State parse(Object stored) {
        if (!(stored instanceof String)) {
            throw new IllegalStateException("Paimon commit state must be a JSON string");
        }
        final JsonElement element;
        try {
            element = new JsonParser().parse((String) stored);
        } catch (RuntimeException e) {
            throw new IllegalStateException("Malformed Paimon commit state JSON", e);
        }
        if (!element.isJsonObject()) {
            throw new IllegalStateException("Paimon commit state must be a JSON object");
        }
        JsonObject json = element.getAsJsonObject();
        if (!json.keySet().equals(FIELDS)) {
            throw new IllegalStateException("Paimon commit state has missing or unknown fields");
        }

        int version = strictInt(json.get("version"), "version");
        if (version != VERSION) {
            throw new IllegalStateException("Unsupported Paimon commit state version " + version);
        }
        JsonElement userElement = json.get("commitUser");
        if (!userElement.isJsonPrimitive()
                || !userElement.getAsJsonPrimitive().isString()
                || userElement.getAsString().trim().isEmpty()) {
            throw new IllegalStateException("Paimon commitUser must be a non-blank string");
        }
        long next = strictLong(json.get("nextCommitIdentifier"), "nextCommitIdentifier");
        return new State(version, userElement.getAsString(), next);
    }

    private static int strictInt(JsonElement value, String field) {
        long parsed = strictLong(value, field);
        if (parsed > Integer.MAX_VALUE) {
            throw new IllegalStateException("Paimon commit state " + field + " is out of range");
        }
        return (int) parsed;
    }

    private static long strictLong(JsonElement value, String field) {
        if (value == null
                || !value.isJsonPrimitive()
                || !value.getAsJsonPrimitive().isNumber()) {
            throw new IllegalStateException("Paimon commit state " + field + " must be an integer");
        }
        String raw = value.getAsString();
        if (!NON_NEGATIVE_LONG.matcher(raw).matches()) {
            throw new IllegalStateException(
                    "Paimon commit state " + field + " must be a non-negative integer");
        }
        try {
            return Long.parseLong(raw);
        } catch (NumberFormatException e) {
            throw new IllegalStateException("Paimon commit state " + field + " is out of range", e);
        }
    }

    static final class Binding {
        private final String commitUser;
        private final long nextCommitIdentifier;
        private final PaimonCommitStateStore store;
        private final String stateKey;

        private Binding(
                String commitUser,
                long nextCommitIdentifier,
                PaimonCommitStateStore store,
                String stateKey) {
            this.commitUser = commitUser;
            this.nextCommitIdentifier = nextCommitIdentifier;
            this.store = store;
            this.stateKey = stateKey;
        }

        String commitUser() {
            return commitUser;
        }

        long nextCommitIdentifier() {
            return nextCommitIdentifier;
        }

        PaimonCommitStateStore store() {
            return store;
        }

        String stateKey() {
            return stateKey;
        }
    }

    static final class State {
        private final int version;
        private final String commitUser;
        private final long nextCommitIdentifier;

        private State(int version, String commitUser, long nextCommitIdentifier) {
            this.version = version;
            this.commitUser = commitUser;
            this.nextCommitIdentifier = nextCommitIdentifier;
        }

        String commitUser() {
            return commitUser;
        }

        long nextCommitIdentifier() {
            return nextCommitIdentifier;
        }
    }
}
