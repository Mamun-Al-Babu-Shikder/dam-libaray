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
public class SelectQuery {
    
    private Map<String, Object> map;
    
    private SelectQuery(Map<String, Object> map){
        this.map = map;
    }
    
    public Map<String, Object> getQueryValueWithColumnName(){
        return this.map;
    }
    
    public static class SelectQueryBuilder{
        
        public static Map<String, Object> map;
        
        public SelectQueryBuilder(){
            map = new HashMap<>();
        }     
        
        public SelectQueryBuilder where(String columnName, Object value){
            map.put(columnName, value);
            return this;
        }
        
        public SelectQuery build(){
            return (new SelectQuery(SelectQueryBuilder.map));
        }
    }
    
}
