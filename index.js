const Connection = require('node-norm/connection');
const { MongoClient, ObjectId } = require('mongodb');

class Mongo extends Connection {
  constructor ({ manager, name, schemas, connectionString, host = '127.0.0.1', port = 27017, user, password, database }) {
    super({ manager, name, schemas });

    if (!connectionString) {
      if (!database) {
        throw new Error('Mongo: Please specify database or connectionString');
      }

      let userData = user ? `${user}:${password}@` : '';
      connectionString = connectionString || `mongodb://${userData}${host}:${port}/${database}`;
    }

    this.connectionString = connectionString;
  }

  async getDB () {
    if (!this._db) {
      // console.log('connect with', this.connectionString)
      this._db = await MongoClient.connect(this.connectionString);
    }

    return this._db;
  }

  async getCollection ({ schema: { name } }) {
    let db = await this.getDB();
    return db.collection(name);
  }

  getCriteria ({ _criteria }) {
    if (typeof _criteria === 'string') {
      return { _id: ObjectId(_criteria) };
    }

    return _criteria;
  }

  async load (query, callback = () => {}) {
    let col = await this.getCollection(query);
    let criteria = this.getCriteria(query);
    let cursor = col.find(criteria);
    if (query._limit > 0) {
      cursor = cursor.limit(query._limit);
    }

    if (query._skip > 0) {
      cursor = cursor.limit(query._skip);
    }

    if (query._sort) {
      cursor = cursor.sort(query._sorts);
    }


    let results = await cursor.toArray();
    return results.map(row => {
      callback(row);
      return row;
    });
  }

  async insert (query, callback) {
    return new Promise(async (resolve, reject) => {
      let col = await this.getCollection(query);
      col.insertMany(query._inserts, function(err, r) {
        if (err) {
          return reject(err);
        }

        r.ops.forEach(op => callback(op));

        resolve(r.insertedCount);
      });
    });
  }

  async update (query) {
    return new Promise(async (resolve, reject) => {
      let col = await this.getCollection(query);
      let criteria = this.getCriteria(query);
      col.updateMany(criteria, { $set: query._sets }, (err, r) => {
        if (err) {
          return reject(err);
        }

        resolve(r.modifiedCount);
      });
    });
  }

  async delete (query) {
    return new Promise(async (resolve, reject) => {
      let col = await this.getCollection(query);
      let criteria = this.getCriteria(query);
      console.log(criteria);
      col.deleteMany(criteria, (err, r) => {
        if (err) {
          return reject(err);
        }

        resolve();
      });
    });
  }

  async drop (query) {
    return new Promise(async (resolve, reject) => {
      let col = await this.getCollection(query);
      let criteria = this.getCriteria(query);
      console.log(criteria);
      col.deleteMany(criteria, (err, r) => {
        if (err) {
          return reject(err);
        }

        resolve();
      });
    });
  }
}

module.exports = Mongo;
