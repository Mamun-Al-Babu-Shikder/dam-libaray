/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mcubes.exception;

/**
 *
 * @author A.A.MAMUN
 */
public class TableNotFoundException extends Exception{
    public TableNotFoundException(String message){
        super(message);
    }
}
