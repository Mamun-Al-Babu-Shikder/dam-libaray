/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mcubes.anotations;

import java.lang.annotation.*;

/**
 *
 * @author A.A.MAMUN
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface PrimaryKey {
    boolean autoIncrement() default false;
}
