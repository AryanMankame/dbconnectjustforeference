const { Client, Pool } = require("pg");

class PostgreSQLConnector {
  constructor(config) {
    this.config = config;
    if(config.serverless) {
        this.db = new Pool({
            connectionString : config.connectionString
        }) 
    }
    else {
        this.db = new Client({
          host: config.host,
          user: config.user,
          port: config.port,
          password: config.password,
          database: config.database,
        });
    }
    this.schema = config.schema;
  }

  async connect() {
    try {
        await this.db.connect();
        console.log("Connected to PostgreSQL database");
    }
    catch(err) {
        console.log(this.config);
        throw new Error('Database not connected' + err);
    }
  }

  async createStore(storeName,storeSchema) {
    const columns = Object.entries(storeSchema)
      .map(([key, value]) => {
        return `${key} ${value.type}${value.unique ? " UNIQUE" : ""}`;
      })
      .join(", ");

    const query = `CREATE TABLE IF NOT EXISTS ${storeName} (${columns})`;

    try {
      await this.db.query(query);
      console.log(`Table '${storeName}' created successfully with schema`);
    } catch (error) {
      console.error("Error creating table:", error.message);
    }
  }
  async insertItem(storeName, documentData) {
    
    const store = await this.db.query(`SELECT * FROM ${storeName}`);
    if(!store) {
      throw new Error('Store not created');
    }
    const columns = Object.keys(documentData).join(", ");
    const values = Object.values(documentData);
    const placeholders = values.map((_, index) => `$${index + 1}`).join(", ");

    const query = `INSERT INTO ${storeName} (${columns}) VALUES (${placeholders}) RETURNING *`;

    try {
      const res = await this.db.query(query, values);
      console.log("Document inserted:", res.rows[0]);
    } catch (error) {
      console.error("Error inserting document:", error.message);
    }
  }

  async readStore(storeName, query = {}) {
    const whereClauses = Object.entries(query)
      .map(([key, value], index) => {
        return `${key} = $${index + 1}`;
      })
      .join(" AND ");
    const values = Object.values(query);

    const sqlQuery = `SELECT * FROM ${storeName}${
      whereClauses ? " WHERE " + whereClauses : ""
    }`;

    try {
      const res = await this.db.query(sqlQuery, values);
      return res.rows;
    } catch (error) {
      console.error("Error reading collection:", error.message);
    }
  }

  async updateStore(storeName, query, update) {
    if(!this.db) {
      throw new Error('Database not connected');
    }
    const setClauses = Object.entries(update)
      .map(([key, value], index) => {
        return `${key} = $${index + 1}`;
      })
      .join(", ");
    const whereClauses = Object.entries(query)
      .map(([key, value], index) => {
        return `${key} = $${index + Object.keys(update).length + 1}`;
      })
      .join(" AND ");
    const values = [...Object.values(update), ...Object.values(query)];

    const sqlQuery = `UPDATE ${storeName} SET ${setClauses} WHERE ${whereClauses}`;

    try {
      await this.db.query(sqlQuery, values);
      console.log(`Collection '${storeName}' updated successfully`);
    } catch (error) {
      console.error("Error updating collection:", error.message);
    }
  }

  async updateItems(storeName, query, update) {
    if(!this.db) {
      throw new Error('Database not connected');
    }
    const setClauses = Object.entries(update)
      .map(([key, value], index) => {
        return `${key} = $${index + 1}`;
      })
      .join(", ");
    const whereClauses = Object.entries(query)
      .map(([key, value], index) => {
        return `${key} = $${index + Object.keys(update).length + 1}`;
      })
      .join(" AND ");
    const values = [...Object.values(update), ...Object.values(query)];

    const sqlQuery = `UPDATE ${storeName} SET ${setClauses} WHERE ${whereClauses}`;

    try {
      await this.db.query(sqlQuery, values);
      console.log(`Collection '${storeName}' updated successfully`);
    } catch (error) {
      console.error("Error updating collection:", error.message);
    }
  }

  async dropItems(storeName,query = {}) {
    if(!this.db) {
      throw new Error("Database not connected");
    }
    const whereClauses = Object.entries(query)
    .map(([key, value], index) => {
      return `${key} = $${index + 1}`;
    })
    .join(" AND ");
    const values = Object.values(query);

    const sqlQuery = `DELETE FROM ${storeName}${
      whereClauses ? " WHERE " + whereClauses : ""
    }`;

    try {
      const res = await this.db.query(sqlQuery, values);
        return res.rows;
      } catch (error) {
        console.error("Error reading collection:", error.message);
      }
    } 

  async dropStore(storeName) {
    if (!this.db) {
      throw new Error("Database not connected");
    }
    await this.db.query(`DROP TABLE ${storeName}`);
  }

  async truncateStore(storeName) {
    if(!this.db) {
      throw new Error('Database not connected');
    }
    await this.db.query(`TRUNCATE TABLE ${storeName}`);
  }

  async alterStore(storeName,newSchema,defaultVlaues) {
    if(!this.db) {
      throw new Error('Database not connected');
    }
    await this.db.query('BEGIN');
    const entries = await this.readStore(storeName);
    try {
      await this.dropStore(storeName);
      await this.createStore(storeName,newSchema);
      const promises = entries.map(async (entry) => {
        const newdoc = {}
        Object.keys(entry).forEach(key => {
          if(newSchema[key]) {
            newdoc[key] = entry[key];
          }
        });
        Object.keys(defaultVlaues).forEach(key => {
          newdoc[key] = defaultVlaues[key];
        })
        await this.insertItem(storeName, newdoc);
      }) 
      await Promise.all(promises)
      await this.db.query('COMMIT');
    }
    catch (err) {
      await this.db.query('ROLLBACK');
      console.log('found and error \n\n',err);
    }
  }
  
  async leftJoin(firstStore,secondStore,performOn) {
    if(!this.db) {
      throw new Error('Database not connected');
    } 
    try {
      return await this.db.query(`SELECT * FROM ${firstStore} LEFT JOIN ${secondStore} ON ${firstStore}.${performOn} = ${secondStore}.${performOn}`);
    }
    catch(err) {
      console.log('Error Occured while performing left join \n\n\n',err);
    }
  }

  async rightJoin(firstStore,secondStore,performOn) {
    if(!this.db) {
      throw new Error('Database not connected');
    }
    try {
      return await this.db.query(`SELECT * FROM ${firstStore} RIGHT JOIN ${secondStore} ON ${firstStore}.${performOn} = ${secondStore}.${performOn}`)
    }
    catch(err) {
      throw new Error('Error Occured while performing right join \n\n\n',err);
    }
  }
  async disconnect() {
    if(!this.db) {
        throw new Error('Database not connected');
    }
    
  }
}

module.exports = PostgreSQLConnector;