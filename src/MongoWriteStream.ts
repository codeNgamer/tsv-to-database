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
  onError?: (err: any) => void;
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

  private onReconnect: () => void;
  private onError: (err: any) => void;
  private onTimeout: () => void;
  constructor(options?: IMongoStreamOptions) {
    super({ objectMode: true });
    this.onReconnect = () => {};
    this.onError = (err: any) => {};
    this.onTimeout = () => {};
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
        this.onReconnect = options.onReconnect;
      }
      if (_.isFunction(options.onError)) {
        this.onError = options.onError;
      }
      if (_.isFunction(options.onTimeout)) {
        this.onTimeout = options.onTimeout;
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
        this.onReconnect();
      });
      db.on("error", err => {
        this.onError(err);
      });
      db.on("timeout", () => {
        this.onTimeout();
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
        this.onError(err);
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
