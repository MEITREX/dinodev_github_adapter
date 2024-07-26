package de.unistuttgart.iste.meitrex.scrumgame.crs.github;

import com.fasterxml.jackson.databind.JsonNode;
import de.unistuttgart.iste.meitrex.generated.dto.AllowedDataType;
import de.unistuttgart.iste.meitrex.generated.dto.CreateEventInput;
import de.unistuttgart.iste.meitrex.generated.dto.EventVisibility;
import de.unistuttgart.iste.meitrex.generated.dto.DataFieldInput;
import de.unistuttgart.iste.meitrex.scrumgame.crs.CrsEventTypes;
import de.unistuttgart.iste.meitrex.scrumgame.external.ExternalSystemAdapter;
import jakarta.validation.constraints.NotNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.OffsetDateTime;
import java.util.*;
import java.util.function.*;

/**
 * Adapter for GitHub events.
 */
@Slf4j
@RequiredArgsConstructor
public class GitHubAdapter implements ExternalSystemAdapter  {

    private final Function<String, Optional<UUID>> githubUsernameToUserId;

    @Override
    public List<CreateEventInput> mapToDinoDevEvents(JsonNode jsonNode, Map<String, String> headers, UUID projectId) {
        return mapToSingleEvent(jsonNode, headers, projectId)
                .map(Collections::singletonList)
                .orElse(Collections.emptyList());
    }

    private Optional<CreateEventInput> mapToSingleEvent(JsonNode jsonNode, Map<String, String> headers, UUID projectId) {
        Optional<String> eventType = getEventType(headers);

        if (eventType.isEmpty()) {
            log.warn("No event type found in headers");
            return Optional.empty();
        }

        return switch (eventType.get()) {
            case "push" -> mapPushEvent(jsonNode, projectId);
            case "pull_request" -> mapPullRequestEvent(jsonNode, projectId);
            case "pull_request_review" -> mapReviewEvent(jsonNode, projectId);
            default -> {
                log.warn("Unsupported event type {}", eventType.get());
                yield Optional.empty();
            }
        };
    }

    private Optional<CreateEventInput> mapPushEvent(JsonNode jsonNode, UUID projectId) {
        CreateEventInput baseEvent = createBaseEvent(jsonNode, projectId);

        baseEvent.setEventTypeIdentifier(CrsEventTypes.PUSH.getIdentifier());

        JsonNode refNode = jsonNode.get("ref");
        if (refNode != null) {
            String branch = refNode.asText();
            if (branch.startsWith("refs/heads/")) {
                branch = branch.substring("refs/heads/".length());
            }
            if ("master".equals(branch) || "main".equals(branch)) {
                baseEvent.setVisibility(EventVisibility.PUBLIC);
            } else {
                baseEvent.setVisibility(EventVisibility.PRIVATE);
            }

            addStringField(baseEvent.getEventData(), "branch", branch);
        }

        List<DataFieldInput> fields = baseEvent.getEventData();

        JsonNode valueNode = jsonNode.get("commits");
        if (valueNode != null && valueNode.isArray()) {
            addIntField(fields, "commitCount", valueNode.size());
        }

        findStringValue(jsonNode, "compare").ifPresent(value -> addStringField(fields, "branchUrl", value));

        return Optional.of(baseEvent);
    }

    private Optional<CreateEventInput> mapPullRequestEvent(JsonNode jsonNode, UUID projectId) {
        CreateEventInput baseEvent = createBaseEvent(jsonNode, projectId);

        findStringValue(jsonNode, "action").ifPresent(value -> {
            if ("opened".equals(value)) {
                baseEvent.setEventTypeIdentifier(CrsEventTypes.OPEN_PULL_REQUEST.getIdentifier());
            } else if ("closed".equals(value)) {
                baseEvent.setEventTypeIdentifier(CrsEventTypes.CLOSE_PULL_REQUEST.getIdentifier());
            }
        });

        JsonNode pullRequestNode = jsonNode.get("pull_request");
        if (pullRequestNode == null) {
            return Optional.empty();
        }

        addPullRequestData(jsonNode, baseEvent);

        return Optional.of(baseEvent);
    }

    private Optional<CreateEventInput> mapReviewEvent(JsonNode jsonNode, UUID projectId) {
        String action = findStringValue(jsonNode, "action").orElse("");
        if (!"submitted".equals(action)) {
            return Optional.empty();
        }

        JsonNode reviewNode = jsonNode.get("review");
        if (reviewNode == null) {
            return Optional.empty();
        }

        String reviewState = findStringValue(reviewNode, "state").orElse("");
        if ("approved".equals(reviewState)) {
            return mapReviewEvent(jsonNode, CrsEventTypes.REVIEW_ACCEPT.getIdentifier(), projectId);
        } else if ("changes_requested".equals(reviewState)) {
            return mapReviewEvent(jsonNode, CrsEventTypes.REVIEW_CHANGE_REQUEST.getIdentifier(), projectId);
        }

        return Optional.empty();
    }

    private Optional<CreateEventInput> mapReviewEvent(
            JsonNode jsonNode,
            @NotNull String eventTypeIdentifier,
            UUID projectId
    ) {
        JsonNode reviewNode = jsonNode.get("review");
        if (reviewNode == null) {
            return Optional.empty();
        }
        CreateEventInput baseEvent = createBaseEvent(jsonNode, projectId);

        baseEvent.setEventTypeIdentifier(eventTypeIdentifier);

        setReviewTimestamp(reviewNode, baseEvent);
        addPullRequestData(jsonNode, baseEvent);

        return Optional.of(baseEvent);
    }

    private static void setReviewTimestamp(JsonNode reviewNode, CreateEventInput baseEvent) {
        OffsetDateTime timestamp = OffsetDateTime.now();
        try {
            timestamp = OffsetDateTime.parse(reviewNode.get("submitted_at").asText());
        } catch (Exception e) {
            log.warn("Failed to parse timestamp", e);
        }
        baseEvent.setTimestamp(timestamp);
    }

    private static void addPullRequestData(JsonNode jsonNode, CreateEventInput baseEvent) {
        JsonNode pullRequestNode = jsonNode.get("pull_request");
        if (pullRequestNode == null) {
            return;
        }

        findStringValue(pullRequestNode, "title")
                .ifPresent(value -> addStringField(baseEvent.getEventData(), "pullRequestTitle", value));
        findStringValue(pullRequestNode, "html_url")
                .ifPresent(value -> addStringField(baseEvent.getEventData(), "pullRequestUrl", value));

        findIntValue(pullRequestNode, "commits")
                .ifPresent(value -> addIntField(baseEvent.getEventData(), "commitCount", value));
        findIntValue(pullRequestNode, "additions")
                .ifPresent(value -> addIntField(baseEvent.getEventData(), "additions", value));
        findIntValue(pullRequestNode, "deletions")
                .ifPresent(value -> addIntField(baseEvent.getEventData(), "deletions", value));
    }

    private static Optional<String> findStringValue(JsonNode node, String key) {
        JsonNode valueNode = node.get(key);
        if (valueNode != null && valueNode.isTextual()) {
            return Optional.of(valueNode.asText());
        }
        return Optional.empty();
    }

    private static OptionalInt findIntValue(JsonNode node, String key) {
        JsonNode valueNode = node.get(key);
        if (valueNode != null && valueNode.isInt()) {
            return OptionalInt.of(valueNode.asInt());
        }
        return OptionalInt.empty();
    }

    private static void addStringField(List<DataFieldInput> fields, String key, String value) {
        fields.add(DataFieldInput.builder()
                .setKey(key)
                .setType(AllowedDataType.STRING)
                .setValue(value)
                .build());
    }

    private static void addIntField(List<DataFieldInput> fields, String key, Integer value) {
        fields.add(DataFieldInput.builder()
                .setKey(key)
                .setType(AllowedDataType.INTEGER)
                .setValue(Integer.toString(value))
                .build());
    }

    private CreateEventInput createBaseEvent(JsonNode jsonNode, UUID projectId) {
        OffsetDateTime timestamp = OffsetDateTime.now();
        List<DataFieldInput> baseData = createBaseData(jsonNode);

        JsonNode userNameNode = jsonNode.get("sender").get("login");

        return CreateEventInput.builder()
                .setTimestamp(timestamp)
                .setEventData(baseData)
                .setUserId(githubUsernameToUserId.apply(userNameNode.asText()).orElse(null))
                .setProjectId(projectId)
                .build();
    }

    private List<DataFieldInput> createBaseData(JsonNode jsonNode) {
        List<DataFieldInput> baseData = new ArrayList<>();

        JsonNode repositoryNode = jsonNode.get("repository");
        addRepositoryBaseData(repositoryNode, baseData);

        JsonNode senderNode = jsonNode.get("sender");
        addSenderBaseData(senderNode, baseData);

        return baseData;
    }

    private static void addSenderBaseData(JsonNode senderNode, List<DataFieldInput> baseData) {
        if (senderNode == null) {
            return;
        }
        findStringValue(senderNode, "login").ifPresent(value -> addStringField(baseData, "vcsUsername", value));
        findStringValue(senderNode, "avatar_url").ifPresent(value -> addStringField(baseData, "vcsAvatarUrl", value));
        findStringValue(senderNode, "html_url").ifPresent(value -> addStringField(baseData, "vcsProfileUrl", value));
    }

    private static void addRepositoryBaseData(JsonNode repositoryNode, List<DataFieldInput> baseData) {
        if (repositoryNode == null) {
            return;
        }
        findStringValue(repositoryNode, "name").ifPresent(value -> addStringField(baseData, "repositoryName", value));
        findStringValue(repositoryNode, "html_url").ifPresent(value -> addStringField(baseData,
                "repositoryUrl",
                value));
    }

    private Optional<String> getEventType(Map<String, String> headers) {
        return Optional.ofNullable(headers.get("x-github-event"));
    }
}
