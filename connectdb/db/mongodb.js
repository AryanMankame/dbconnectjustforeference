const { MongoClient } = require('mongodb');

class MongoDBConnector {
  constructor(config) {
    this.config = config;
    this.client = new MongoClient(config.uri, config.options);
    this.db = null;
  }

  async connect() {
    await this.client.connect();
    this.db = this.client.db(this.config.dbName);
  }

  async createCollection(collectionName, schema) {
    if (!this.db) {
      throw new Error('Database not connected');
    }
    try {
      await this.db.createCollection(collectionNamea);
      console.log('Collection ' + collectionName + ' successfully created with schema');
    } catch (error) {
      console.error('Error creating collection:', error.message);
    }
  }
  async createDocument(collectionName, documentData) {
    try{
      await this.db.collection(collectionName).insertOne(documentData);
    }
    catch(e) {
      console.log('Something went wrong');
    }
  }

  async readCollection(collectionName, query = {}) {
    if (!this.db) {
      throw new Error('Database not connected');
    }
    return this.db.collection(collectionName).find(query).toArray();
  }

  async updateCollection(collectionName, query, update) {
    if (!this.db) {
      throw new Error('Database not connected');
    }
    return this.db.collection(collectionName).updateMany(query, update);
  }

  async deleteCollection(collectionName) {
    if (!this.db) {
      throw new Error('Database not connected');
    }
    await this.db.collection(collectionName).drop();
  }
  

  async disconnect() {
    if (this.client) {
      await this.client.close();
    }
  }
}

module.exports = MongoDBConnector;

// database-connect-simplifier/
// │
// ├── lib/
// │   ├── db/
// │   │   ├── index.js        # Factory to get the right DB handler
// │   │   ├── mongodb.js      # MongoDB specific code
// │   │   ├── mysql.js        # MySQL specific code
// │   │   └── postgresql.js   # PostgreSQL specific code
// │   └── index.js            # Main interface for the package
// │
// ├── test/
// │   └── test.js             # Tests for the package (optional but recommended)
// │
// ├── .gitignore              # Git ignore file
// ├── package.json            # NPM package configuration
// ├── README.md               # Documentation for the package
// └── LICENSE                 # License for the package
