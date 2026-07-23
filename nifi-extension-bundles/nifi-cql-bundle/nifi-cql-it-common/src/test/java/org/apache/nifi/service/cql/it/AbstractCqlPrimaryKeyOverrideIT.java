/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.service.cql.it;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.record.path.util.RecordPathCache;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.service.cql.api.CQLExecutionService;
import org.apache.nifi.service.cql.api.WriteOverrides;
import org.apache.nifi.service.cql.api.metadata.PrimaryKeyIdentifier;
import org.apache.nifi.service.cql.api.metadata.QualifiedTableName;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises primary key {@link RecordPath} overrides directly against {@link CQLExecutionService} - the
 * "session provider side" of the mechanism {@code PutCQLRecord} otherwise drives - using the {@code chats}
 * / {@code chats_by_user} / {@code messages_by_chat} schema examined earlier for this feature (see class
 * javadoc below for the DDL). Assumes the subclass has already bootstrapped a keyspace and called
 * {@link #initializeSessionProvider(CqlConnectionInfo)}, which also creates the three tables.
 *
 * <h2>Scenario</h2>
 * Simulates 5 chats with 2-4 participants apiece and 100 messages per chat (500 total). Each chat's
 * messages are spread over a window of 2-5 days, and every chat's window - despite starting and ending at
 * a different point - falls entirely inside the same shared one-week period ({@link #WEEK_START} through
 * {@link #WEEK_END}), matching "each chat can begin and end at different points in the same week."
 *
 * <h2>Why {@code messages_by_chat} needs overrides at all</h2>
 * {@code messages_by_chat}'s primary key is {@code ((chat_id, msg_date), msg_hour, sent_at, message_id)},
 * but a record shaped like the {@code MessageByChat} Avro schema only carries {@code chat_id}, {@code sent_at},
 * {@code message_id}, {@code sender_id}, and {@code body} - there is no {@code msg_date}/{@code msg_hour}
 * field for plain field-name matching to find. The primary key overrides map is what lets
 * {@code messages_by_chat.msg_date} and {@code messages_by_chat.msg_hour} be resolved from a {@link RecordPath}
 * evaluated against {@code sent_at} instead.
 *
 * <h2>Both {@code msg_date} and {@code msg_hour} are derived live from {@code sent_at}</h2>
 * Neither column has a matching-named field anywhere in the message record - matching the real
 * {@code MessageByChat} Avro schema, which has no {@code msg_date}/{@code msg_hour} field at all. Both
 * overrides are plain {@code format()} calls against {@code sent_at}: {@code format(/sent_at, 'H', 'UTC')}
 * for {@code msg_hour}, {@code format(/sent_at, 'yyyy-MM-dd', 'UTC')} for {@code msg_date}. The RecordPath
 * {@code format()} function always produces a {@link String}, which reaches
 * {@code CassandraCQLExecutionService}'s {@code convertForCqlType} unconverted (it has no branch for either
 * {@code DataTypes.TINYINT} or {@code DataTypes.DATE}), so both bindings depend entirely on the driver's own
 * codec registry accepting a {@link String} for that CQL type: {@code FlexibleTinyIntCodec} for
 * {@code msg_hour} (via {@code DataTypeUtils.toByte}), and {@code FlexibleDateCodec} for {@code msg_date}
 * (parsed with the fixed {@code yyyy-MM-dd} pattern, matching what {@code format()} produces here) - both
 * registered as {@code Object}-typed fallback codecs the driver only reaches once its own exact-type codec
 * fails to match a raw {@link String}.
 * <p>
 * Getting this far also depends on a fix to {@code CassandraCQLExecutionService.generateInsert}/
 * {@code generateUpdate}: since neither column has a same-named record field, the statement's column list -
 * originally built purely from {@code schema.getFieldNames()} - would never have included {@code msg_date}/
 * {@code msg_hour} as bind markers at all, regardless of what any codec could do with the override's value.
 * Both methods now also include every primary key override's target column for the table being written,
 * even when no record field shares that name.
 *
 * <h2>Why {@code sent_at} is a {@code java.sql.Timestamp}, not an {@code Instant}</h2>
 * Every other {@code timestamp} column written elsewhere in this test suite (e.g. {@code message.when_sent}
 * in {@link AbstractCqlCrudIT}) uses a raw {@link Instant} value, which binds fine on its own via the
 * driver's native {@code Instant}-typed {@code timestamp} codec. That would break {@code msg_hour}'s
 * override here, though: the RecordPath {@code format()} function (see {@code Format.evaluate}) only
 * recognizes a {@code java.util.Date} or a {@link Number} as its input value - not an {@link Instant} - and
 * silently passes anything else through unformatted. {@code java.sql.Timestamp} (a {@code java.util.Date}
 * subtype) satisfies both sides at once: {@code format()} recognizes it directly, and
 * {@code CassandraCQLExecutionService} separately registers {@code JavaSQLTimestampCodec}, which binds
 * {@code java.sql.Timestamp} to CQL {@code timestamp} for the plain, non-overridden {@code sent_at} column
 * write.
 *
 * <h2>DDL under test</h2>
 * <pre>{@code
 * CREATE TABLE chats (
 *     chat_id      uuid,
 *     created_at   timestamp,
 *     chat_name    text,
 *     participants set<uuid>,
 *     PRIMARY KEY (chat_id)
 * );
 *
 * CREATE TABLE chats_by_user (
 *     user_id   uuid,
 *     chat_id   uuid,
 *     joined_at timestamp,
 *     PRIMARY KEY (user_id, chat_id)
 * );
 *
 * CREATE TABLE messages_by_chat (
 *     chat_id    uuid,
 *     msg_date   date,
 *     msg_hour   tinyint,
 *     sent_at    timestamp,
 *     message_id timeuuid,
 *     sender_id  uuid,
 *     body       text,
 *     PRIMARY KEY ((chat_id, msg_date), msg_hour, sent_at, message_id)
 * ) WITH CLUSTERING ORDER BY (msg_hour ASC, sent_at ASC, message_id ASC);
 * }</pre>
 * <p>
 * Setup is a plain protected method rather than a JUnit lifecycle callback for the same reason documented
 * on {@link AbstractCqlCrudIT}.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractCqlPrimaryKeyOverrideIT {

    private static final int DDL_MAX_ATTEMPTS = 3;

    private static final long SEED = 42L;

    private static final int CHAT_COUNT = 5;

    private static final int MESSAGES_PER_CHAT = 100;

    private static final int MIN_PARTICIPANTS = 2;

    private static final int MAX_PARTICIPANTS = 4;

    private static final int MIN_CHAT_WINDOW_DAYS = 2;

    private static final int MAX_CHAT_WINDOW_DAYS = 5;

    private static final Instant WEEK_START = Instant.parse("2026-08-01T00:00:00Z");

    private static final Instant WEEK_END = WEEK_START.plus(7, ChronoUnit.DAYS);

    protected CQLExecutionService sessionProvider;

    private CqlSession session;

    private String keyspace;

    private RecordPathCache recordPathCache;

    /**
     * @return a fresh, unconfigured instance of the backend implementation under test
     */
    protected abstract CQLExecutionService newSessionProvider();

    protected void initializeSessionProvider(final CqlConnectionInfo connectionInfo) throws Exception {
        this.session = connectionInfo.session();
        this.keyspace = connectionInfo.keyspace();
        this.sessionProvider = newSessionProvider();
        this.recordPathCache = new RecordPathCache(10);

        final TestRunner runner = TestRunners.newTestRunner(new MockCqlProcessor());
        runner.addControllerService("cql-session-provider", sessionProvider);
        runner.setProperty(sessionProvider, CQLExecutionService.CONTACT_POINTS, connectionInfo.contactPoint());
        runner.setProperty(sessionProvider, CQLExecutionService.DATACENTER, connectionInfo.datacenter());
        runner.setProperty(sessionProvider, CQLExecutionService.KEYSPACE, connectionInfo.keyspace());
        runner.enableControllerService(sessionProvider);

        createSchema();
    }

    /**
     * Retries a schema-modifying statement on {@link DriverTimeoutException}, for the same reason (and in
     * the same idempotent-DDL-only shape) as the identically-named helper in {@link AbstractCqlRecordFieldTypeIT}.
     */
    private void executeDdlWithRetry(final String cql) {
        DriverTimeoutException lastFailure = null;
        for (int attempt = 1; attempt <= DDL_MAX_ATTEMPTS; attempt++) {
            try {
                session.execute(cql);
                return;
            } catch (final DriverTimeoutException e) {
                lastFailure = e;
            }
        }
        throw lastFailure;
    }

    private void createSchema() {
        executeDdlWithRetry(String.format(
                "create table if not exists %s.chats (chat_id uuid, created_at timestamp, chat_name text, "
                        + "participants set<uuid>, primary key (chat_id))", keyspace));
        executeDdlWithRetry(String.format(
                "create table if not exists %s.chats_by_user (user_id uuid, chat_id uuid, joined_at timestamp, "
                        + "primary key (user_id, chat_id))", keyspace));
        executeDdlWithRetry(String.format(
                "create table if not exists %s.messages_by_chat (chat_id uuid, msg_date date, msg_hour tinyint, "
                        + "sent_at timestamp, message_id timeuuid, sender_id uuid, body text, "
                        + "primary key ((chat_id, msg_date), msg_hour, sent_at, message_id)) "
                        + "with clustering order by (msg_hour asc, sent_at asc, message_id asc)", keyspace));
    }

    private record GeneratedChat(UUID chatId, String chatName, Set<UUID> participants, Instant windowStart, Instant windowEnd) {
    }

    private record GeneratedMessage(UUID chatId, Instant sentAt, UUID messageId, UUID senderId, String body,
                                     LocalDate expectedMsgDate, int expectedMsgHour) {
    }

    /**
     * Builds {@link #CHAT_COUNT} chats, each with a 2-5 day message window placed at a random offset within
     * the shared one-week period - so different chats begin and end at different points in the week, but
     * none ever escapes it.
     */
    private List<GeneratedChat> generateChats(final Random random) {
        final List<GeneratedChat> chats = new ArrayList<>();

        for (int i = 0; i < CHAT_COUNT; i++) {
            final int windowDays = MIN_CHAT_WINDOW_DAYS + random.nextInt(MAX_CHAT_WINDOW_DAYS - MIN_CHAT_WINDOW_DAYS + 1);
            final int maxStartOffsetDays = 7 - windowDays;
            final int startOffsetDays = random.nextInt(maxStartOffsetDays + 1);
            final Instant windowStart = WEEK_START.plus(startOffsetDays, ChronoUnit.DAYS);
            final Instant windowEnd = windowStart.plus(windowDays, ChronoUnit.DAYS);

            final int participantCount = MIN_PARTICIPANTS + random.nextInt(MAX_PARTICIPANTS - MIN_PARTICIPANTS + 1);
            final Set<UUID> participants = new LinkedHashSet<>();
            while (participants.size() < participantCount) {
                participants.add(UUID.randomUUID());
            }

            chats.add(new GeneratedChat(UUID.randomUUID(), "Chat " + i, participants, windowStart, windowEnd));
        }

        return chats;
    }

    /**
     * Builds {@link #MESSAGES_PER_CHAT} messages for a chat, with {@code sent_at} uniformly distributed
     * across the chat's own window (evenly splitting the message count across chats keeps the scenario
     * reproducible; the interesting variation is in the per-chat time windows, not the counts). The upper
     * bound is exclusive so a generated timestamp never lands exactly on the window's end instant.
     */
    private List<GeneratedMessage> generateMessages(final GeneratedChat chat, final Random random) {
        final List<GeneratedMessage> messages = new ArrayList<>();
        final List<UUID> participantList = new ArrayList<>(chat.participants());
        final long windowStartMillis = chat.windowStart().toEpochMilli();
        final long windowEndMillis = chat.windowEnd().toEpochMilli();

        for (int i = 0; i < MESSAGES_PER_CHAT; i++) {
            final long sentAtMillis = windowStartMillis + (long) (random.nextDouble() * (windowEndMillis - windowStartMillis));
            final Instant sentAt = Instant.ofEpochMilli(sentAtMillis);
            final UUID senderId = participantList.get(random.nextInt(participantList.size()));
            final UUID messageId = Uuids.timeBased();
            final String body = "Message " + i + " in chat " + chat.chatId();

            final LocalDate expectedMsgDate = sentAt.atZone(ZoneOffset.UTC).toLocalDate();
            final int expectedMsgHour = sentAt.atZone(ZoneOffset.UTC).getHour();

            messages.add(new GeneratedMessage(chat.chatId(), sentAt, messageId, senderId, body, expectedMsgDate, expectedMsgHour));
        }

        return messages;
    }

    private RecordSchema chatSchema() {
        return new SimpleRecordSchema(List.of(
                new RecordField("chat_id", RecordFieldType.UUID.getDataType()),
                new RecordField("created_at", RecordFieldType.TIMESTAMP.getDataType()),
                new RecordField("chat_name", RecordFieldType.STRING.getDataType()),
                new RecordField("participants", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.UUID.getDataType()))
        ));
    }

    private RecordSchema chatMembershipSchema() {
        return new SimpleRecordSchema(List.of(
                new RecordField("user_id", RecordFieldType.UUID.getDataType()),
                new RecordField("chat_id", RecordFieldType.UUID.getDataType()),
                new RecordField("joined_at", RecordFieldType.TIMESTAMP.getDataType())
        ));
    }

    private RecordSchema messageSchema() {
        // Exactly the real MessageByChat Avro schema's fields - no msg_date/msg_hour placeholder needed,
        // since generateInsert now includes primary key override target columns even without a matching
        // schema field (see the class javadoc).
        return new SimpleRecordSchema(List.of(
                new RecordField("chat_id", RecordFieldType.UUID.getDataType()),
                new RecordField("sent_at", RecordFieldType.TIMESTAMP.getDataType()),
                new RecordField("message_id", RecordFieldType.UUID.getDataType()),
                new RecordField("sender_id", RecordFieldType.UUID.getDataType()),
                new RecordField("body", RecordFieldType.STRING.getDataType())
        ));
    }

    private void insertChat(final GeneratedChat chat) {
        final Map<String, Object> values = new HashMap<>();
        values.put("chat_id", chat.chatId());
        values.put("created_at", chat.windowStart());
        values.put("chat_name", chat.chatName());
        values.put("participants", chat.participants());

        sessionProvider.insert(new QualifiedTableName(keyspace, "chats"), new MapRecord(chatSchema(), values), Map.of(), WriteOverrides.NONE);
    }

    private void insertChatMemberships(final GeneratedChat chat) {
        for (final UUID userId : chat.participants()) {
            final Map<String, Object> values = new HashMap<>();
            values.put("user_id", userId);
            values.put("chat_id", chat.chatId());
            values.put("joined_at", chat.windowStart());

            sessionProvider.insert(new QualifiedTableName(keyspace, "chats_by_user"), new MapRecord(chatMembershipSchema(), values), Map.of(), WriteOverrides.NONE);
        }
    }

    private void insertMessage(final GeneratedMessage message, final Map<PrimaryKeyIdentifier, RecordPath> overrides) {
        final Map<String, Object> values = new HashMap<>();
        values.put("chat_id", message.chatId());
        // java.sql.Timestamp, not Instant - see the class javadoc; this is what makes the msg_hour
        // override's format() call actually format the value instead of silently passing it through.
        values.put("sent_at", Timestamp.from(message.sentAt()));
        values.put("message_id", message.messageId());
        values.put("sender_id", message.senderId());
        values.put("body", message.body());

        final Record record = new MapRecord(messageSchema(), values);
        sessionProvider.insert(new QualifiedTableName(keyspace, "messages_by_chat"), record, overrides, WriteOverrides.NONE);
    }

    @Test
    @DisplayName("Primary key overrides derive msg_date and msg_hour from sent_at for 500 messages spread across 5 chats sharing one week")
    void testPrimaryKeyOverridesDeriveMessagePartitionAndClusteringColumns() {
        final Random random = new Random(SEED);
        final List<GeneratedChat> chats = generateChats(random);

        // Sanity-check the generator itself before trusting it to drive the rest of the test: every chat's
        // window is 2-5 days long and falls entirely within the shared one-week period, even though each
        // chat's own start/end point within that week differs.
        for (final GeneratedChat chat : chats) {
            final long windowDays = ChronoUnit.DAYS.between(chat.windowStart(), chat.windowEnd());
            assertTrue(windowDays >= MIN_CHAT_WINDOW_DAYS && windowDays <= MAX_CHAT_WINDOW_DAYS,
                    () -> "Chat window was " + windowDays + " days");
            assertTrue(!chat.windowStart().isBefore(WEEK_START) && !chat.windowEnd().isAfter(WEEK_END),
                    () -> "Chat window " + chat.windowStart() + " - " + chat.windowEnd() + " escaped the shared one-week period");
            assertTrue(chat.participants().size() >= MIN_PARTICIPANTS && chat.participants().size() <= MAX_PARTICIPANTS,
                    () -> "Chat had " + chat.participants().size() + " participants");
        }

        final Map<PrimaryKeyIdentifier, RecordPath> messageOverrides = Map.of(
                new PrimaryKeyIdentifier(keyspace, "messages_by_chat", "msg_date"), recordPathCache.getCompiled("format(/sent_at, 'yyyy-MM-dd', 'UTC')"),
                new PrimaryKeyIdentifier(keyspace, "messages_by_chat", "msg_hour"), recordPathCache.getCompiled("format(/sent_at, 'H', 'UTC')")
        );

        final List<GeneratedMessage> allMessages = new ArrayList<>();
        for (final GeneratedChat chat : chats) {
            insertChat(chat);
            insertChatMemberships(chat);

            final List<GeneratedMessage> messages = generateMessages(chat, random);
            allMessages.addAll(messages);
            for (final GeneratedMessage message : messages) {
                insertMessage(message, messageOverrides);
            }
        }

        assertEquals(CHAT_COUNT * MESSAGES_PER_CHAT, allMessages.size());

        final long totalRowCount = session.execute(String.format("select count(*) as row_count from %s.messages_by_chat", keyspace))
                .one().getLong("row_count");
        assertEquals(allMessages.size(), totalRowCount);

        for (final GeneratedChat chat : chats) {
            final Set<UUID> readBackParticipants = new LinkedHashSet<>(session.execute(
                    String.format("select participants from %s.chats where chat_id = %s", keyspace, chat.chatId()))
                    .one().getSet("participants", UUID.class));
            assertEquals(chat.participants(), readBackParticipants);
        }

        // Every message's msg_date/msg_hour - never supplied directly under those column names by the
        // caller - must have been correctly derived by the primary key overrides for the row to be found
        // at all at the primary key computed independently here from sent_at.
        for (final GeneratedMessage message : allMessages) {
            final String cql = String.format(
                    "select sender_id, body from %s.messages_by_chat where chat_id = %s and msg_date = '%s' "
                            + "and msg_hour = %d and sent_at = '%s' and message_id = %s",
                    keyspace, message.chatId(), message.expectedMsgDate(), message.expectedMsgHour(), message.sentAt(), message.messageId());
            final Row row = session.execute(cql).one();

            assertNotNull(row, () -> "Expected a row at the primary key derived from sent_at=" + message.sentAt() + " but found none: " + cql);
            assertEquals(message.senderId(), row.getUuid("sender_id"));
            assertEquals(message.body(), row.getString("body"));
        }
    }
}
