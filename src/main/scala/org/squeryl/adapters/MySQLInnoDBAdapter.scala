
package org.squeryl.adapters

/**
*   Since MySQL 5.5 InnoDB has replaced MyISAM as the default storage engine.
*   Thus, to take full advantage of the database abilities, new MySQL installs
*   should use this Adapter. 
*   see: http://dev.mysql.com/doc/refman/5.5/en/innodb-default-se.html
*/
class MySQLInnoDBAdapter extends MySQLAdapter {
    
    /**
    *   InnoDB MySQL tables support foreign key constraints,
    *   see http://dev.mysql.com/doc/refman/5.5/en/innodb-foreign-key-constraints.html
    */
    override def supportsForeignKeyConstraints = true
    
}