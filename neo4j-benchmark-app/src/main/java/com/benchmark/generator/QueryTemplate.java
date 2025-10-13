package com.benchmark.generator;

import java.util.Collections;
import java.util.Map;

public final class QueryTemplate {

    private QueryTemplate() {}

    public static final class PreparedQuery {
        public final QueryLanguage lang;
        public final String text;                     // Cypher or Gremlin
        public final Map<String,Object> params;       // bound variables

        /** Back-compat: default to CYPHER if lang not specified */
        public PreparedQuery(String text, Map<String,Object> params) {
            this(QueryLanguage.CYPHER, text, params);
        }

        public PreparedQuery(QueryLanguage lang, String text, Map<String,Object> params) {
            this.lang   = lang;
            this.text   = text;
            this.params = params == null ? Map.of() : Collections.unmodifiableMap(params);
        }

        @Override public String toString() {
            return "PreparedQuery[" + lang + "] " + text;
        }
    }
}
