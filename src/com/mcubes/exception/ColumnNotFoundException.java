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
public class ColumnNotFoundException extends Exception {
    public ColumnNotFoundException(String message){
        super(message);
    }
}
