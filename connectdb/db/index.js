const MongoDBConnector = require('./mongodb');
const PostgreSQLConnector = require('./psql');
// Future connectors will be added here, e.g. MySQLConnector, PostgreSQLConnector

module.exports = {
  getConnector: (config) => {
    if (config.type === 'mongodb') {
      return new MongoDBConnector(config);
    }
    else if(config.type === 'postgresql') {
      return new PostgreSQLConnector(config);
    }
    // Add cases for other database types here
    throw new Error('Unsupported database type');
  }
};

