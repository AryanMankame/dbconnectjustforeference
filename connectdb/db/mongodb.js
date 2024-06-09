const mongoose = require('mongoose');

class MongoDBConnector {
  constructor(config) {
    this.config = config;
    this.db = null;
  }

  async connect() {
    try {
      await mongoose.connect(this.config.uri, this.config.options);
      this.db = mongoose.connection;
    } catch (err) {
      if (err.name === 'MongoServerError') {
        console.log(`Error connecting to MongoDB:
                      - Check credentials (username, password, typos, special characters)
                      - Verify database name
                      - Ensure user has necessary permissions
                      - Review connection string (hostname, port, username, password, URL format)`);
      }
      throw err;
    }
  }

  async createStore(storeName, schema) {
    if (!this.db) {
      throw new Error('Database not connected');
    }
    try {
      const collectionSchema = new mongoose.Schema(schema);
      mongoose.model(storeName, collectionSchema);
    } catch (err) {
      console.log(`There is an issue in creating the collection ${storeName}`);
      throw err;
    }
  }

  async insertItem(storeName, documentData) {
    if (!this.db) {
      throw new Error('Database not connected');
    }
    try {
      const collection = mongoose.model(storeName);
      await collection.create(documentData);
    } catch (err) {
      console.log(`Error inserting item into ${storeName}`);
      throw err;
    }
  }

  async readStore(storeName, query = {}) {
    if (!this.db) {
      throw new Error('Database not connected');
    }
    try {
      const Collection = mongoose.model(storeName);
      return await Collection.find(query);
    } catch (err) {
      console.log(`Error reading from store ${storeName}`);
      throw err;
    }
  }

  async updateItems(storeName, query, update) {
    if (!this.db) {
      throw new Error('Database not connected');
    }
    try {
      const Collection = mongoose.model(storeName);
      return await Collection.updateMany(query, update);
    } catch (err) {
      console.log(`Error updating items in ${storeName}`);
      throw err;
    }
  }

  async deleteStore(storeName) {
    if (!this.db) {
      throw new Error('Database not connected');
    }
    try {
      const Collection = mongoose.model(storeName);
      await Collection.collection.drop();
    } catch (err) {
      console.log(`Error deleting store ${storeName}`);
      throw err;
    }
  }

  async alterStore(storeName, newSchema, changeValues) {
    if (!this.db) {
      throw new Error('Database not connected');
    }
    var originalSchema = {};
    var docs = [];
    try {
      console.log(storeName, newSchema, changeValues);
      var collection = mongoose.model(storeName);
      originalSchema = collection.schema;
      console.log(originalSchema);
      docs = await collection.find({});
      await collection.deleteMany({});
      console.log('here are the docs' + docs);
      delete mongoose.models[storeName];
      collection = mongoose.model(storeName, new mongoose.Schema(newSchema, { changeStreamPreAndPostImages: { enabled: true } }));
      const runloop = async () => {
        const promises = docs.map(async (doc) => {
          var newdoc = {};
          Object.keys(doc._doc).forEach((key) => {
            if(newSchema[key]) {
              newdoc[key] = doc._doc[key];
            }
          })
          Object.keys(changeValues).forEach((key) => {
            console.log(key, changeValues[key]);
            newdoc[key] = changeValues[key];
          })
          await collection.insertMany(newdoc);
        })
        await Promise.all(promises);
        console.log(promises);
      }
      await runloop();
    } catch (err) {
      console.log('Datatypes are not compatible' + err);
      const collec = mongoose.model(storeName, originalSchema);
      collec.insertMany(docs);      
      throw new Error('Datatypes are not compatible' + err);
    }
  }

  async dropItem(storeName, itemInfo) {
    if (!this.db) {
      throw new Error('Database not connected')
    }
    try {
      const Collection = mongoose.model(storeName);
      await Collection.deleteOne(itemInfo);
    } catch (err) {
      console.log(`Failed to drop the item from ${storeName}`);
      throw new Error('Failed to drop the item');
    }
  }

  async dropItems(storeName, itemInfo) {
    if (!this.db) {
      throw new Error('Database not connected')
    }
    try {
      const Collection = mongoose.model(storeName);
      await Collection.deleteMany(itemInfo);
    } catch (err) {
      console.log(`Failed to drop all the matching items from ${storeName}`);
      throw new Error('Failed to drop all the matching items');
    }
  }

  async truncateStore(storeName) {
    if (!this.db) {
      throw new Error('Database not connected')
    }
    try {
      const Collection = mongoose.model(storeName);
      console.log(storeName, Collection);
      await Collection.deleteMany({});
    } catch (err) {
      console.log('Truncate Store Error');
      throw new Error(err);
    }
  }

  async createTrigger(firststoreName, operationsToBePerformed) {
    if (!this.db) {
      throw new Error('Database not connected');
    }
    try {
      const firststore = mongoose.model(firststoreName);
      await this.db.db.command({
        collMod: 'users',
        changeStreamPreAndPostImages: { enabled: true }
      });
      console.log('inside mongodb.js', operationsToBePerformed);
      firststore.watch([], { fullDocumentBeforeChange: "whenAvailable", fullDocument: 'updateLookup' }).on('change', (data) => {
        switch (data.operationType) {
          case 'insert':
            operationsToBePerformed[data.operationType](data.fullDocument);
            break;
          case 'update':
            operationsToBePerformed[data.operationType](data.fullDocument);
            break;
          case 'delete':
            operationsToBePerformed[data.operationType](data.fullDocumentBeforeChange);
            break;
        }
      })
    } catch (err) {
      console.log('Error creating trigger');
      throw err;
    }
  }

  async findAndPopulateItem(storeName, thingsToBePopulated, query) {
    if (!this.db) {
      throw new Error('Database not connected');
    }
    try {
      const collection = mongoose.model(storeName);
      console.log(query);
      return await collection.find(query).populate(thingsToBePopulated).exec();
    } catch (err) {
      console.log(`Error finding and populating item in ${storeName}`);
      throw err;
    }
  } 
  
  async leftJoin(firstStore, secondStore, performOn) {
    if(!this.db) {
      throw new Error('Database not connected');
    }
    try{
      const firststoredocs = await this.readStore(firstStore, {});
      const secondstoredocs = await this.readStore(secondStore, {});
      const valuemap = {};
      secondstoredocs.forEach((doc,idx) => {
        // console.log(doc,idx,performOn,doc.hasOwnProperty(performOn[1]));
        if(doc[performOn[1]] !== undefined) {
          if(valuemap[doc[performOn[1]]] !== undefined) {
            valuemap[Number(doc[performOn[1]])].push(idx);
          }
          else {
            valuemap[Number(doc[performOn[1]])] = [];
            valuemap[Number(doc[performOn[1]])].push(idx);
          }
        }
      })
      const result = [];
      firststoredocs.forEach(doc => {
        if(doc[performOn[0]] != undefined) {
          if(valuemap[doc[performOn[0]]] !==  undefined) {
            valuemap[doc[performOn[0]]].forEach(idx => {
              const newdoc = { ...doc._doc , ...secondstoredocs[idx]._doc};
              result.push(newdoc);
            })
          }
          else {
            result.push(doc);
          }
        }
      })
      console.log(valuemap);
      return result;
    }
    catch(err) {
      throw err;
    }
  }

  async rightJoin(firstStore, secondStore, performOn) {
    if(!this.db) {
      throw new Error('Database not connected');
    }
    try {
      const firststoredocs = await this.readStore(firstStore, {});
      const secondstoredocs = await this.readStore(secondStore, {});
      const valuemap = {};
      firststoredocs.forEach((doc,idx) => {
        // console.log(doc,idx,performOn,doc.hasOwnProperty(performOn[1]));
        if(doc[performOn[0]] !== undefined) {
          if(valuemap[doc[performOn[0]]] !== undefined) {
            valuemap[Number(doc[performOn[0]])].push(idx);
          }
          else {
            valuemap[Number(doc[performOn[0]])] = [];
            valuemap[Number(doc[performOn[0]])].push(idx);
          }
        }
      })
      const result = [];
      secondstoredocs.forEach(doc => {
        if(doc[performOn[1]] != undefined) {
          if(valuemap[doc[performOn[1]]] !==  undefined) {
            valuemap[doc[performOn[1]]].forEach(idx => {
              const newdoc = { ...doc._doc , ...firststoredocs[idx]._doc};
              result.push(newdoc);
            })
          }
          else {
            result.push(doc);
          }
        }
      })
      console.log(valuemap);
      return result;
    }
    catch(err) {
      throw err;
    }
  }

  async equiJoin(firstStore,secondStore,performOn) {
    if(!this.db) {
      throw new Error('Database not connected')
    }
    const firststoredocs = await this.readStore(firstStore, {});
      const secondstoredocs = await this.readStore(secondStore, {});
      const valuemap = {};
      secondstoredocs.forEach((doc,idx) => {
        // console.log(doc,idx,performOn,doc.hasOwnProperty(performOn[1]));
        if(doc[performOn[1]] !== undefined) {
          if(valuemap[doc[performOn[1]]] !== undefined) {
            valuemap[Number(doc[performOn[1]])].push(idx);
          }
          else {
            valuemap[Number(doc[performOn[1]])] = [];
            valuemap[Number(doc[performOn[1]])].push(idx);
          }
        }
      })
      const result = [];
      firststoredocs.forEach(doc => {
        if(doc[performOn[0]] != undefined) {
          if(valuemap[doc[performOn[0]]] !==  undefined) {
            valuemap[doc[performOn[0]]].forEach(idx => {
              const newdoc = { ...doc._doc , ...secondstoredocs[idx]._doc};
              result.push(newdoc);
            })
          }
        }
      })
      console.log(valuemap);
      return result;
  }


  async disconnect() {
    if (this.db) {
      try {
        await mongoose.disconnect();
      } catch (err) {
        console.log('Error disconnecting from the database');
        throw err;
      }
    }
  }
}

module.exports = MongoDBConnector;
