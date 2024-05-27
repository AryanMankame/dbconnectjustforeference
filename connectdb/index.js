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

  async createStore(collectionName, schema) {
    if (!this.connector) {
      throw new Error('Connector not initialized');
    }
    await this.connector.createStore(collectionName, schema);
  }
  async insertItem(collectionName, documentData) {
    if(!this.connector) {
      throw new Error('Database not connected problem');
    }
    await this.connector.insertItem(collectionName, documentData);
  }
  async readStore(collectionName, query = {}) {
    if (!this.connector) {
      throw new Error('Connector not initialized');
    }
    return this.connector.readCollection(collectionName, query);
  }

  async updateItems(collectionName, query, update) {
    if (!this.connector) {
      throw new Error('Connector not initialized');
    }
    return this.connector.updateItems(collectionName, query, update);
  }

  async deleteStore(collectionName) {
    if (!this.connector) {
      throw new Error('Connector not initialized');
    }
    await this.connector.deleteStore(collectionName);
  }
  async alterStore(storeName, newSchema) {
    if(!this.connector) {
      throw new Error('Connector not initialized');
    }
    await this.connector.alterStore(storeName, newSchema);
  }
  async getmydb() {
    if (!this.connector) {
      throw new Error('Connector not initialized');
    }
    return this.connector;
  }
  async dropItem(storeName, itemInfo) {
    if (!this.connector) {
      throw new Error('Connector not initialized');
    }
    await this.connector.dropItem(storeName, itemInfo);
    console.log('Item successfully dropped')
  }
  
  async dropItems(storeName, itemInfo) {
    if (!this.connector) {
      throw new Error('Connector not initialized');
    }
    await this.connector.dropItems(storeName, itemInfo);
    console.log('Items successfully dropped')
  }
  
  async truncateStore(storeName) {
    if(!this.connector) {
      throw new Error('Connector not initialized');
    }
    await this.connector.truncateStore(storeName);
  }

  async alterStore(storeName, newSchema, changeValues) {
    if(!this.connector) {
      throw new Error('Connector not initialized');
    }
    await this.connector.alterStore(storeName , newSchema, changeValues);
  }

  async createTrigger(firststoreName,operationsToBePerformed) {
    if(!this.connector) {
      throw new Error('Connector not initialized');
    }
    await this.connector.createTrigger(firststoreName, operationsToBePerformed );
  }

  async findAndPopulateItem(storeName, operationsToBePerformed, query) {
    if(!this.connector) {
      throw new Error('Connector not initialized');
    }
    return await this.connector.findAndPopulateItem(storeName, operationsToBePerformed, query);
  } 


  async disconnect() {
    if (this.connector) {
      await this.connector.disconnect();
    }
  }
}

module.exports = DatabaseConnectSimplifier;


