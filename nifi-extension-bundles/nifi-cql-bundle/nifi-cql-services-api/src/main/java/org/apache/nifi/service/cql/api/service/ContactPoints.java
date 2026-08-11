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
package org.apache.nifi.service.cql.api.service;

import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;

import java.util.ArrayList;
import java.util.List;

/**
 * Parses the <em>Cassandra Contact Points</em> property value into host/port pairs. The one set of rules
 * lives here and is applied twice: {@link #VALIDATOR} runs it at configuration time, and a session provider
 * runs {@link #parse} on the (possibly Expression-Language-produced) runtime value - so what validation
 * accepts and what the connection attempts cannot drift apart.
 *
 * <p>Accepted entry forms, comma-separated:
 * <ul>
 *   <li>{@code host:port} and bare {@code host} (which gets {@link #DEFAULT_PORT}), where the host is a
 *   hostname or IPv4 literal</li>
 *   <li>a bracketed IPv6 literal with an optional port: {@code [::1]:9042}, {@code [::1]}</li>
 *   <li>an unbracketed IPv6 literal - recognized by containing more than one colon - taken whole as the
 *   host, since without brackets there is no way to tell a port apart from the address's own final group</li>
 * </ul>
 *
 * <p>Parsing produces bare host/port pairs rather than resolved addresses: no name resolution happens here,
 * so the validator never blocks the framework on DNS.
 */
public final class ContactPoints {

    /** The default CQL client port, used for any entry that names no port of its own. */
    public static final int DEFAULT_PORT = 9042;

    /**
     * Validates a Contact Points value by parsing it with the same rules the connection will use. A value
     * containing Expression Language is passed through unvalidated, as is conventional - it can only be
     * checked once evaluated, which {@link #parse} does at connection time with the same error messages.
     */
    public static final Validator VALIDATOR = (subject, input, context) -> {
        if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(input)) {
            return new ValidationResult.Builder()
                    .subject(subject).input(input).explanation("Expression Language present").valid(true).build();
        }

        try {
            parse(input);
            return new ValidationResult.Builder().subject(subject).input(input).valid(true).build();
        } catch (final IllegalArgumentException e) {
            return new ValidationResult.Builder()
                    .subject(subject).input(input).explanation(e.getMessage()).valid(false).build();
        }
    };

    /** One parsed contact point. The host is never blank; the port is always in {@code 1-65535}. */
    public record HostPort(String host, int port) {
    }

    private ContactPoints() {
    }

    /**
     * @param contactPointList the property value: comma-separated entries in the forms listed on this class
     * @return one {@link HostPort} per entry, in the order given
     * @throws IllegalArgumentException if the value is null or blank, or any entry is malformed; the message
     * names the offending entry
     */
    public static List<HostPort> parse(final String contactPointList) {
        if (contactPointList == null || contactPointList.isBlank()) {
            throw new IllegalArgumentException("At least one contact point is required");
        }

        final List<HostPort> contactPoints = new ArrayList<>();
        for (final String entry : contactPointList.split(",")) {
            contactPoints.add(toContactPoint(entry.trim()));
        }

        return contactPoints;
    }

    private static HostPort toContactPoint(final String entry) {
        final String host;
        final String portText;

        if (entry.startsWith("[")) {
            final int closingBracket = entry.indexOf(']');
            if (closingBracket < 0) {
                throw new IllegalArgumentException(String.format(
                        "Contact point '%s' has an unterminated '[': a bracketed IPv6 literal looks like [::1] or [::1]:9042", entry));
            }

            host = entry.substring(1, closingBracket);
            final String remainder = entry.substring(closingBracket + 1);
            if (remainder.isEmpty()) {
                portText = null;
            } else if (remainder.startsWith(":")) {
                portText = remainder.substring(1);
            } else {
                throw new IllegalArgumentException(String.format(
                        "Contact point '%s' has trailing text after ']': only an optional :port may follow", entry));
            }
        } else {
            final int firstColon = entry.indexOf(':');
            if (firstColon >= 0 && firstColon == entry.lastIndexOf(':')) {
                host = entry.substring(0, firstColon);
                portText = entry.substring(firstColon + 1);
            } else {
                // No colon (a bare host), or several (an unbracketed IPv6 literal): either way, no port.
                host = entry;
                portText = null;
            }
        }

        if (host.isBlank()) {
            throw new IllegalArgumentException(String.format("Contact point '%s' has no host", entry));
        }

        return new HostPort(host.trim(), toPort(entry, portText));
    }

    private static int toPort(final String entry, final String portText) {
        if (portText == null) {
            return DEFAULT_PORT;
        }

        final int port;
        try {
            port = Integer.parseInt(portText.trim());
        } catch (final NumberFormatException e) {
            throw new IllegalArgumentException(String.format(
                    "Contact point '%s' has an invalid port '%s'", entry, portText.trim()), e);
        }

        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException(String.format(
                    "Contact point '%s' has out-of-range port %d: expected 1-65535", entry, port));
        }

        return port;
    }
}
