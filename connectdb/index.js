const { getConnector } = require('./db');

class DatabaseConnectSimplifier {
  constructor(config) {
    this.config = config;
    this.connector = getConnector(config);
  }

  async connect() {
    if (!this.connector) {
      throw new Error('Connector not initialized');
    }
    await this.connector.connect();
  }

  async createCollection(collectionName) {
    if (!this.connector) {
      throw new Error('Connector not initialized');
    }
    await this.connector.createCollection(collectionName);
  }
  
  async createDocument(collectionName,documentData){
    if (!this.connector) {
      throw new Error('Connector not initialized');
    }
    console.log(collectionName,documentData);
    await this.connector.createDocument(collectionName,documentData);
  }

  async readCollection(collectionName, query = {}) {
    if (!this.connector) {
      throw new Error('Connector not initialized');
    }
    return this.connector.readCollection(collectionName, query);
  }

  async updateCollection(collectionName, query, update) {
    if (!this.connector) {
      throw new Error('Connector not initialized');
    }
    return this.connector.updateCollection(collectionName, query, update);
  }

  async deleteCollection(collectionName) {
    if (!this.connector) {
      throw new Error('Connector not initialized');
    }
    await this.connector.deleteCollection(collectionName);
  }


  async disconnect() {
    if (this.connector) {
      await this.connector.disconnect();
    }
  }
}

module.exports = DatabaseConnectSimplifier;
