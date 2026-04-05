// userModel manages backend MySQL user records.
const { getMySQL } = require('../../config/database');

const normaliseUserRow = (row) => {
  if (!row) return null;

  return {
    user_id: row.user_id,
    name: row.name ?? row.username ?? '',
    email: row.email ?? '',
    role: row.role ?? 'researcher',
    created_at: row.created_at ?? null,
    last_login: row.last_login ?? null,
  };
};

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
    const query = 'SELECT * FROM users WHERE user_id = ?';
    const [rows] = await (await getMySQL()).execute(query, [user_id]);
    return normaliseUserRow(rows[0]);
  }

  static async updateProfile(user_id, updates) {
    const fields = [];
    const values = [];

    if (updates.name !== undefined) {
      fields.push('name = ?');
      values.push(updates.name);
    }

    if (updates.email !== undefined) {
      fields.push('email = ?');
      values.push(updates.email);
    }

    if (fields.length === 0) return { affectedRows: 0 };

    values.push(user_id);
    const query = `UPDATE users SET ${fields.join(', ')} WHERE user_id = ?`;
    const [result] = await (await getMySQL()).execute(query, values);
    return result;
  }

  
  static async updateLastLogin(user_id) {
    const query = 'UPDATE users SET last_login = NOW() WHERE user_id = ?';
    const [result] = await (await getMySQL()).execute(query, [user_id]);
    return result;
  }
}

module.exports = UserModel;
