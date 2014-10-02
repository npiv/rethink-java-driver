package com.rethinkdb.model;

/**
 * The durability setting used in various options like insert or table create
 */
public enum ConflictStrategy{
    error,   // Do not insert the new document and record the conflict as an error (default)
    replace, // replace the old document in its entirety with the new one
    update   // update fields of the old document with fields from the new one
}