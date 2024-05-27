// const DatabaseConnectSimplifier = require('../connectdb/index');

// const config = {
//   type: 'mongodb',
//   uri: 'mongodb+srv://admin:admin@cluster0.tqm8j4u.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0',
//   dbName: 'testdb',
//   options: {
//     useNewUrlParser: true,
//     useUnifiedTopology: true,
//   },
// };

// async function main() {
//   const db = new DatabaseConnectSimplifier(config);
//   try {
//     await db.connect();
//     await db.createDocument('users',{
//         name : 'abcdef',
//         email : 'abcdef@example.com'
//     })
//     const user = await db.readCollection('users', {
//         name : 'abcde',
//         email : 'abcde@example.com'
//     })
//     console.log(user)
//     console.log('Database connected');
//   } catch (error) {
//     console.error(error);
//   }
// }

// main();


const { default: mongoose } = require('mongoose');
const DatabaseConnectSimplifier = require('../connectdb/index');

const config = {
  type: 'mongodb',
  uri: 'mongodb+srv://admin:admin@cluster0.tqm8j4u.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0',
  options: {
    useNewUrlParser: true,
    useUnifiedTopology: true,
    dbName : 'testdb'
  },
};

const userSchema = {
  name: {
    type: Number,
    required: true,
  },
  email: {
    type: String,
    required: true,
    match: /.+@.+\..+/,
  },
};

async function main() {
  const db = new DatabaseConnectSimplifier(config);

  try {
    await db.connect();
    console.log('Database connected');
    await db.createStore('teacher', {
      name: String,
      subjects: [{ type: mongoose.Schema.Types.ObjectId, ref: 'subject' }]
    });
    await db.createStore('subject', {
      name: String,
      description: String,
      courses: [{ type: mongoose.Schema.Types.ObjectId, ref: 'course' }]
    })
    await db.createStore('course', {
      name: String,
      description: String
    });
    const res = await db.findAndPopulateItem('teacher', {
      path : 'subjects',
      populate : {
        path : 'courses'
      }
    },
    { name : 'teacher_1'});
    res.forEach((item) => {
      item.subjects.forEach((course) => {
        console.log(course.courses)
      })
    });
    // for(let i=0; i<5; i++) {
      // await db.insertItem('course' , {
      //   name : `course_${i}`,
      //   description: `Im the ${i} course`
      // });
      // await db.insertItem('subject' , { 
      //   name : `subject_${i}`,
      //   description : `im the ${i} subject`,
      //   courses : [ new mongoose.Types.ObjectId('6654cb207b478415ee8468b2') , new mongoose.Types.ObjectId('6654cb207b478415ee8468b4') ]
      // });
    //   await db.insertItem('teacher' , {
    //     name : `teacher_${i}`,
    //     description : `im the ${i} teacher`,
    //     subjects : [ new mongoose.Types.ObjectId('6654d74f730d6bbe5898276c') , new mongoose.Types.ObjectId('6654d74f730d6bbe5898276e')]
    //   })
    // }
    
    // await db.createStore('users', userSchema);
    // await db.createStore('members', userSchema);
    // console.log('Collection created with schema');

    // Insert a sample document
    // await db.originalCommand("create({ name: 'John Doe', email: 'john.doe@example.com' })");
    // console.log('Document inserted');
    // await db.createTrigger('users','members',{
    //   'insert' : (data) => {
    //     console.log('im the insert function',data);
    //     db.insertItem('members' , { name : data.name, email : data.email })
    //   }
    // });
    // await db.insertItem('users',{name: 37, email: 'ron37.doe@example.com'})
    // await db.dropItem('users',{name: 7, email: 'ron7.doe@example.com'});
    // await db.updateItems('users', { name : 82, email : 'ron6.doe@example.com'} , { name : 83});
    // for(let i=0; i < 10; i++){
    //   await db.insertItem('users',{name: i, email: `ron${i}.doe@example.com` });
    // }
    // Read collection with a query
    // const query = { name: 'Johny Dora', email: 'johnyd.doe@example.com' };
    // const users = await db.readCollection('members', {});
    // console.log('Collection read', users);

    // await db.deleteStore('members');
    // await db.updateItems('users' , { name : 3} , { name : 'absd' });
    // Read updated collection
    // const updatedUsers = await db.readCollection('User', query);
    // console.log('Updated collection read', updatedUsers);

    // await db.deleteCollection('User');
    // console.log('Collection deleted');

    // await db.alterStore('users' , { ...userSchema , age : {
    //   type : Number
    // }}, { age : 18 });

  } catch (error) {
    console.error(error);
  } finally {
    // await db.disconnect();
    console.log('Database disconnected');
  }
}

main();
