/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mcubes.query;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author A.A.MAMUN
 */
public class DeleteQuery {

    private Map<String, Object> map;

    private DeleteQuery(Map<String, Object> map) {
        this.map = map;
    }

    public Map<String, Object> getQueryValueWithColumnName() {
        return this.map;
    }

    public static class DeleteQueryBuilder {

        public static Map<String, Object> map;

        public DeleteQueryBuilder() {
            map = new HashMap<>();
        }

        public DeleteQueryBuilder where(String columnName, Object value) {
            map.put(columnName, value);
            return this;
        }

        public DeleteQuery build() {
            return (new DeleteQuery(DeleteQueryBuilder.map));
        }
    }

}
