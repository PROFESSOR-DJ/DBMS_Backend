const { getMongoDB } = require('../config/database');

class MongoDBService {
  constructor() {
    this.db = getMongoDB();
    this.isConnected = this.db !== null;
  }

  async getCollection(collectionName) {
    if (!this.isConnected) {
      throw new Error('MongoDB is not connected');
    }
    return this.db.collection(collectionName);
  }

  async healthCheck() {
    if (!this.isConnected) {
      return { status: 'disconnected', message: 'MongoDB not available' };
    }
    
    try {
      await this.db.command({ ping: 1 });
      return { status: 'connected', message: 'MongoDB is healthy' };
    } catch (error) {
      return { status: 'error', message: error.message };
    }
  }
}

module.exports = new MongoDBService();