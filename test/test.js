const DatabaseConnectSimplifier = require('../connectdb/index');

const config = {
  type: 'mongodb',
  uri: 'mongodb+srv://admin:admin@cluster0.tqm8j4u.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0',
  dbName: 'testdb',
  options: {
    useNewUrlParser: true,
    useUnifiedTopology: true,
  },
};

async function main() {
  const db = new DatabaseConnectSimplifier(config);
  try {
    await db.connect();
    await db.createDocument('users',{
        name : 'abcdef',
        email : 'abcdef@example.com'
    })
    const user = await db.readCollection('users', {
        name : 'abcde',
        email : 'abcde@example.com'
    })
    console.log(user)
    console.log('Database connected');
  } catch (error) {
    console.error(error);
  }
}

main();
