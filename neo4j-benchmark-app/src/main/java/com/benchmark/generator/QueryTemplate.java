package com.benchmark.generator;

import java.util.Collections;
import java.util.Map;

public final class QueryTemplate {

    private QueryTemplate() {}

    public static final class PreparedQuery {
        public final QueryLanguage lang;              // CYPHER or GREMLIN
        public final String text;                     // query text
        public final Map<String,Object> params;       // bound variables
        public final String name;                     // template name
        public final String category;                 // OLTP / GRAPH / OLAP

        /** Back-compat: default to CYPHER and unknown name/category */
        public PreparedQuery(String text, Map<String,Object> params) {
            this(QueryLanguage.CYPHER, text, params, "<unnamed>", "OLTP");
        }

        public PreparedQuery(QueryLanguage lang, String text, Map<String,Object> params,
                             String name, String category) {
            this.lang      = lang;
            this.text      = text;
            this.params    = params == null ? Map.of() : Collections.unmodifiableMap(params);
            this.name      = name == null ? "<unnamed>" : name;
            this.category  = category == null ? "OLTP" : category.toUpperCase();
        }

        @Override public String toString() {
            return "PreparedQuery[" + lang + "] " + name + " {" + category + "}";
        }
    }
}
