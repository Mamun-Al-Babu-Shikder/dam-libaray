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
public class MultiplePrimaryKeyException extends Exception{
    
    public MultiplePrimaryKeyException(String message){
        super(message);
    }
}
