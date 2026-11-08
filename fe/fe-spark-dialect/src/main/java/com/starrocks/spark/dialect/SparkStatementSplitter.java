// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.spark.dialect;

import java.util.ArrayList;
import java.util.List;

/**
 * Splits a script into statements at top-level semicolons, following Spark SQL's lexical rules: '...' and "..."
 * are strings with backslash escapes, `...` are identifiers, and -- / C-style comments are skipped.
 */
public final class SparkStatementSplitter {
    private SparkStatementSplitter() {
    }

    public static List<String> split(String script) {
        List<String> statements = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        int i = 0;
        int n = script.length();
        while (i < n) {
            char c = script.charAt(i);
            if (c == '\'' || c == '"' || c == '`') {
                int end = skipQuoted(script, i, c);
                current.append(script, i, end);
                i = end;
            } else if (c == '-' && i + 1 < n && script.charAt(i + 1) == '-') {
                int end = script.indexOf('\n', i);
                end = end < 0 ? n : end;
                current.append(script, i, end);
                i = end;
            } else if (c == '/' && i + 1 < n && script.charAt(i + 1) == '*') {
                int end = script.indexOf("*/", i + 2);
                end = end < 0 ? n : end + 2;
                current.append(script, i, end);
                i = end;
            } else if (c == ';') {
                addIfNotBlank(statements, current);
                current.setLength(0);
                i++;
            } else {
                current.append(c);
                i++;
            }
        }
        addIfNotBlank(statements, current);
        return statements;
    }

    private static int skipQuoted(String script, int start, char quote) {
        int i = start + 1;
        while (i < script.length()) {
            char c = script.charAt(i);
            if (c == '\\' && quote != '`') {
                i += 2;
            } else if (c == quote) {
                // `a``b` escapes the backtick by doubling it.
                if (quote == '`' && i + 1 < script.length() && script.charAt(i + 1) == '`') {
                    i += 2;
                } else {
                    return i + 1;
                }
            } else {
                i++;
            }
        }
        return script.length();
    }

    private static void addIfNotBlank(List<String> statements, StringBuilder statement) {
        String text = statement.toString().trim();
        if (!text.isEmpty() && !isOnlyComments(text)) {
            statements.add(text);
        }
    }

    private static boolean isOnlyComments(String text) {
        String stripped = text.replaceAll("(?s)/\\*.*?\\*/", "").replaceAll("--[^\\n]*", "").trim();
        return stripped.isEmpty();
    }
}
