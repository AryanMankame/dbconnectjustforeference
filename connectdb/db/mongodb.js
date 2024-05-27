const mongoose = require('mongoose');

class MongoDBConnector {
  constructor(config) {
    this.config = config;
    this.db = null;
  }

  async connect() {
    try{
      await mongoose.connect(this.config.uri, this.config.options);
      this.db = mongoose.connection;
    }
    catch(err){
      if(err.name === 'MongoServerError') {
        console.log('Something went wrong while connecting to MongoDB try out these solutions to fix your problem');
        console.log("1. Check your credentials:");
        console.log("   - Username and Password:");
        console.log("      - Ensure you're using the correct username and password for your MongoDB user.");
        console.log("      - Make sure there are no typos or extra spaces.");
        console.log("      - If your password contains special characters, try wrapping them in quotes or using URL encoding.");
        console.log("   - Database Name:");
        console.log("      - Verify that you're connecting to the correct database name.");
        console.log("2. Reset your password:");
        console.log("   - If you're unsure of the password, try resetting it through the MongoDB UI or using the 'db.createUser' command.");
        console.log("3. Verify user creation:");
        console.log("   - If you've created a new user, ensure you've granted them the necessary permissions to access the database and collections.");
        console.log("4. Connection string issues:");
        console.log("   - Double-check your connection string for any errors.");
        console.log("   - Ensure it includes the correct hostname, port, username, and password.");
        console.log("   - If using URL authentication, make sure the format is correct and includes the 'authSource' parameter.");
      }
      throw err;
    }
  }

  async createStore(storeName, schema) {
    if (!this.db) {
      throw new Error('Database not connected');
    }
    try{
      const collectionSchema = new mongoose.Schema(schema);
      mongoose.model(storeName, collectionSchema);
    }
    catch(err) {
      console.log(`There is an issue in creating the collection ${storeName}`)
      throw err;
    }
  }

  async insertItem(storeName, documentData){
    if(!this.db) {
      throw new Error('Database not connected');
    }
    const collection = mongoose.model(storeName);
    await collection.create(documentData);
  }

  async readStore(storeName, query = {}) {
    if (!this.db) {
      throw new Error('Database not connected');
    }
    
    const Collection = mongoose.model(storeName);
    return await Collection.find(query);
  }

  async updateItems(storeName, query, update) {
    if (!this.db) {
      throw new Error('Database not connected');
    }

    const Collection = mongoose.model(storeName);
    return await Collection.updateMany(query, update);
  }

  async deleteStore(storeName) {
    if (!this.db) {
      throw new Error('Database not connected');
    }

    const Collection = mongoose.model(storeName);
    await Collection.collection.drop();
  }
  
  // will do later tasks to be done here are : 1. first we will go through each of the documents of the collection and the 
  async alterStore(storeName, newSchema , changeValues) {
    if(!this.db) {
      throw new Error('Database not connected');
    }
    try{
      console.log(storeName, newSchema, changeValues);
      var collection = mongoose.model(storeName);
      const docs = await collection.find({});
      await collection.deleteMany({});
      console.log('here are the docs' + docs);
      delete mongoose.models[storeName];
      collection = mongoose.model(storeName, new mongoose.Schema(newSchema,{ changeStreamPreAndPostImages: { enabled: true } }));
      const runloop = async () => {
        const promises = docs.map(async (doc) => {
          var newdoc = {};
          Object.keys(changeValues).forEach((key) => {
           console.log(key,changeValues[key]);
           newdoc = {...doc._doc}
           newdoc[key] = changeValues[key];
          })
          await collection.insertMany(newdoc);
        })
        await Promise.all(promises);
        console.log(promises);
      }
      await runloop();
    }
    catch(err){
      throw new Error('Datatypes are not compatible' + err);
    }
  }

  async dropItem(storeName, itemInfo) {
    if(!this.db) {
      throw new Error('Databsae not connected')
    }
    try{
      const Collection = mongoose.model(storeName);
      await Collection.deleteOne(itemInfo)
    }
    catch(err){
      throw new Error('Failed to drop the item')
    }
  }
 
  async dropItems(storeName, itemInfo) {
    if(!this.db) {
      throw new Error('Databsae not connected')
    }
    try{
      const Collection = mongoose.model(storeName);
      await Collection.deleteMany(itemInfo)
    }
    catch(err){
      throw new Error('Failed to drop all the matching items')
    }
  }

  async truncateStore(storeName) {
    if(!this.db) {
      throw new Error('Databsae not connected')
    }
    try{
      const Collection = mongoose.model(storeName);
      console.log(storeName,Collection)
      await Collection.deleteMany({});
    }
    catch(err){
      console.log('Truncate Store Error \n')
      throw new Error(err);
    }
  }

  async createTrigger(firststoreName, operationsToBePerformed) {
    if(!this.db) {
      throw new Error('Databse not connected');
    }
    const firststore = mongoose.model(firststoreName);
    await this.db.db.command({
      collMod: 'users',
      changeStreamPreAndPostImages: { enabled: true }
    });
    console.log('inside mongodb.js',operationsToBePerformed);
    firststore.watch([],{ fullDocumentBeforeChange: "whenAvailable" , fullDocument : 'updateLookup'}).on('change', (data) => {
      switch(data.operationType){
        case 'insert':
          operationsToBePerformed[data.operationType](data.fullDocument);
          break;
        case 'update':
          operationsToBePerformed[data.operationType](data.fullDocument);
          break;
        case 'delete':
          operationsToBePerformed[data.operationType](data.fullDocumentBeforeChange)
          break;
      }
    })
  }

  async findAndPopulateItem(storeName,thingsToBePopulated,query) {
    if(!this.db) {
      throw new Error('Database not connected');
    }
    const collection = mongoose.model(storeName);
    console.log(query);
    return await collection.find(query).populate(thingsToBePopulated).exec();
  }

    
  async disconnect() {
    if (this.db) {
      await mongoose.disconnect();
    }
  }
}

module.exports = MongoDBConnector;
