// userModel manages backend MySQL user records.
const { getMySQL } = require('../../config/database');

class UserModel {
  
  static async create(user) {
    const { name, email, password, role = 'researcher' } = user;
    const query = 'INSERT INTO users (name, email, password, role) VALUES (?, ?, ?, ?)';
    const [result] = await (await getMySQL()).execute(query, [name, email, password, role]);
    return result;
  }

  
  static async findByEmail(email) {
    const query = 'SELECT * FROM users WHERE email = ?';
    const [rows] = await (await getMySQL()).execute(query, [email]);
    return rows[0];
  }

  
  static async findByUsername(username) {
    const query = 'SELECT * FROM users WHERE username = ?';
    const [rows] = await (await getMySQL()).execute(query, [username]);
    return rows[0];
  }

  
  static async findById(user_id) {
    const query = 'SELECT user_id, username, email, created_at FROM users WHERE user_id = ?';
    const [rows] = await (await getMySQL()).execute(query, [user_id]);
    return rows[0];
  }

  
  static async updateLastLogin(user_id) {
    const query = 'UPDATE users SET last_login = NOW() WHERE user_id = ?';
    const [result] = await (await getMySQL()).execute(query, [user_id]);
    return result;
  }
}

module.exports = UserModel;
