const mysql = require('mysql2/promise');
require('dotenv').config();

const fixUsersTable = async () => {
  let connection;
  
  try {
    console.log('🔧 Fixing users table...\n');
    
    connection = await mysql.createConnection({
      host: process.env.MYSQL_HOST || 'localhost',
      port: process.env.MYSQL_PORT || 3306,
      user: process.env.MYSQL_USER || 'root',
      password: process.env.MYSQL_PASSWORD || 'root',
      database: process.env.MYSQL_DATABASE || 'research_sql'
    });

    // Check if last_login column exists
    const [columns] = await connection.query(`
      SELECT COLUMN_NAME 
      FROM INFORMATION_SCHEMA.COLUMNS 
      WHERE TABLE_SCHEMA = ? 
      AND TABLE_NAME = 'users' 
      AND COLUMN_NAME = 'last_login'
    `, [process.env.MYSQL_DATABASE || 'research_sql']);

    if (columns.length === 0) {
      console.log('Adding last_login column...');
      await connection.query(`
        ALTER TABLE users 
        ADD COLUMN last_login TIMESTAMP NULL AFTER password_hash
      `);
      console.log('✅ last_login column added successfully\n');
    } else {
      console.log('✅ last_login column already exists\n');
    }

    // Show current table structure
    const [structure] = await connection.query('DESCRIBE users');
    console.log('Current users table structure:');
    console.table(structure);
    
    console.log('\n✅ Users table is now fixed!\n');
    
  } catch (error) {
    console.error('❌ Error:', error.message);
    process.exit(1);
  } finally {
    if (connection) await connection.end();
    process.exit(0);
  }
};

fixUsersTable();