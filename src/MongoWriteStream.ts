import * as _ from "lodash";
import {
  Db,
  InsertWriteOpResult,
  MongoClient,
  MongoClientOptions
} from "mongodb";
import { Writable } from "stream";
import { IParsedObject, nodeCallback } from "./types";

interface IMongoStreamOptions {
  databaseUrl?: string;
  databaseName?: string;
  collectionName?: string;
  mongoClientOptions?: MongoClientOptions;
  onError?: () => void;
  onReconnect?: () => void;
  onTimeout?: () => void;
}
/**
 *  This class can write stream in mongoDB database
 *
 * @export
 * @class MongoWriteStream
 * @extends {Writable}
 */
export class MongoWriteStream extends Writable {
  private databaseName: string = "tsv_to_mongo";
  private collectionName: string = "parsed_tsv";
  private databaseUrl: string = "mongodb://localhost:27017";
  private db: Promise<Db>;
  private client: Promise<MongoClient>;
  private lastInsert?: Promise<void | InsertWriteOpResult>;

  constructor(options?: IMongoStreamOptions) {
    super({ objectMode: true });
    let onReconnect = () => {};
    let onError = () => {};
    let onTimeout = () => {};
    if (options) {
      const { databaseName, databaseUrl, collectionName } = options;
      if (databaseName) {
        this.databaseName = databaseName;
      }
      if (databaseUrl) {
        this.databaseUrl = databaseUrl;
      }
      if (collectionName) {
        this.collectionName = collectionName;
      }
      if (_.isFunction(options.onReconnect)) {
        onReconnect = options.onReconnect;
      }
      if (_.isFunction(options.onError)) {
        onError = options.onError;
      }
      if (_.isFunction(options.onTimeout)) {
        onTimeout = options.onTimeout;
      }
    }
    const mongoOptions = _.isObject(options)
      ? options.mongoClientOptions || {}
      : {};
    this.client = new MongoClient(this.databaseUrl, {
      reconnectTries: 300,
      reconnectInterval: 2000,
      useNewUrlParser: true,
      ...mongoOptions
    }).connect();
    this.db = this.client.then(client => {
      const db = client.db(this.databaseName);
      db.on("reconnect", () => {
        console.log("-> reconnected");
        onReconnect();
      });
      db.on("error", () => {
        onError();
      });
      db.on("timeout", () => {
        onTimeout();
      });
      return db;
    });
  }

  public _write(
    chunk: IParsedObject[],
    encoding: string,
    callback: nodeCallback
  ): void {
    const insertPromice = this.db
      .then(db => {
        const collection = db.collection(this.collectionName);
        return collection.insertMany(chunk);
      })
      .catch(err => {
        console.log(err);
        process.exit();
      });

    this.lastInsert = this.lastInsert
      ? (this.lastInsert = this.lastInsert.then(() => insertPromice))
      : (this.lastInsert = insertPromice);
    callback();
  }

  public _final(callback: nodeCallback) {
    if (this.lastInsert) {
      this.lastInsert.then(() => {
        this.client.then(client => client.close());
      });
    }
    callback();
  }
}
