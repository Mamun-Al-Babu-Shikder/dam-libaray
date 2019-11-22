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
public class UpdateQuery {

    private Map<String, Object> map;
    private Map<String, Object> map2;

    private UpdateQuery(Map<String, Object> map, Map<String, Object> map2) {
        this.map = map;
        this.map2 = map2;
    }
    
    public Map<String, Object> getUpdateColumnNameWithValue(){
        return map;
    }
    
    public Map<String, Object> getConditionalColumnNameWithValue(){
        return map2;
    }

    public static class UpdateQueryBuilder {

        private Map<String, Object> map;
        private Map<String, Object> map2;

        public UpdateQueryBuilder() {
            map = new HashMap<>();
            map2 = new HashMap<>();
        }

        public UpdateQueryBuilder set(String columnName, Object value) {
            map.put(columnName, value);
            return this;
        }

        public UpdateQueryBuilder where(String columnName, Object value) {
            map2.put(columnName, value);
            return this;
        }

        
         public UpdateQuery build(){
         return new UpdateQuery(this.map, this.map2);
         }
         
    }

}
