const db = require("../connectdb/db");
const DatabaseConnectSimplifier = require("../connectdb/index");
require('dotenv').config()

const postgresConfig = {
  type: "postgresql",
  serverless : true,
  host: process.env.POSTGRES_URL,
  user: process.env.POSTGRES_USER,
  password: process.env.POSTGRES_PASSWORD,
  database: process.env.POSTGRES_DATABASE,
  port: process.env.POSTGRES_PORT || 5432,
  connectionString : process.env.POSTGRES_URL,
  schema: {
    id: { type: "SERIAL PRIMARY KEY", unique: true },
    name: { type: "VARCHAR(255)", unique: false },
    email: { type: "VARCHAR(255)", unique: true },
  },
};

async function main() {
  const dbConnector = new DatabaseConnectSimplifier(postgresConfig);

  await dbConnector.connect();
  // await dbConnector.insertItem('testusers',{
  //   name : 'xyz',
  //   email : 'xyz@example.com'
  // })
  // await dbConnector.insertItem('testusers',{
  //   name : 'abcd',
  //   email : 'abcd@example.com'
  // });
  // const res = await dbConnector.readStore('testusers')
  // console.log(res);
  // await dbConnector.dropItems('testusers', {
  //   name : 'abcd',
  //   email : 'abcd@example.com'  
  // })
  // const result = await dbConnector.readStore('testusers')
  // console.log(result);
  // await dbConnector.updateItems('testusers', {
  //   name : 'abcd', email : 'abcd@example.com'
  // }, {
  //   name : 'abcd123', email : 'abcd123@example.com'
  // });
  // await dbConnector.alterStore('testusers', {
  //   id: { type: "SERIAL PRIMARY KEY", unique: true },
  //   name: { type: "VARCHAR(255)", unique: false },
  //   email: { type: "VARCHAR(255)", unique: true },
  //   age : { type: "INTEGER", unique: false }
  // }, { age : "rat"})
  // console.log(await dbConnector.readStore('testusers'));
  // await dbConnector.createStore('testusers', {
  //   id: { type: "SERIAL PRIMARY KEY", unique: true },
  //   name: { type: "VARCHAR(255)", unique: false },
  //   email: { type: "VARCHAR(255)", unique: true },
  // });

  // await dbConnector.insertItem('testusers', {
  //   name : 'abc',
  //   email : 'abc@example.com'
  // })

//   await dbConnector.createDocument("users", {
//     id: 5,
//     name: "John Doe",
//     email: "raj.doe@example.com",
//   });

//   const users = await dbConnector.readCollection("users");
//   console.log(users);
  await dbConnector.createStore("testusersinfo", {
    id : { type : "SERIAL PRIMARY KEY", unique : true},
    profession : { type : "VARCHAR(10)", unique : false },
    yoe : { type : "INT", unique : false}
  });
  for(let i=0;i<5;i++) {
      await dbConnector.insertItem('testusers' , {
        id : i,
        name : `user_${i}`,
        email : `user${i}@example.com`
      })
      await dbConnector.insertItem('testusersinfo', { 
        id : i,
        profession : `prof_${i}`,
        yoe : i+1 
      });
  }
  const res = await dbConnector.leftJoin('testusers','testusersinfo','id');
  console.log(res);
}

main();