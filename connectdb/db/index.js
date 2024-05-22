const MongoDBConnector = require('./mongodb');
// Future connectors will be added here, e.g. MySQLConnector, PostgreSQLConnector

module.exports = {
  getConnector: (config) => {
    if (config.type === 'mongodb') {
      return new MongoDBConnector(config);
    }
    // Add cases for other database types here
    throw new Error('Unsupported database type');
  }
};

