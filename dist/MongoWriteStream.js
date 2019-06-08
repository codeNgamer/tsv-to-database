"use strict";
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (Object.hasOwnProperty.call(mod, k)) result[k] = mod[k];
    result["default"] = mod;
    return result;
};
Object.defineProperty(exports, "__esModule", { value: true });
const _ = __importStar(require("lodash"));
const mongodb_1 = require("mongodb");
const stream_1 = require("stream");
/**
 *  This class can write stream in mongoDB database
 *
 * @export
 * @class MongoWriteStream
 * @extends {Writable}
 */
class MongoWriteStream extends stream_1.Writable {
    constructor(options) {
        super({ objectMode: true });
        this.databaseName = "tsv_to_mongo";
        this.collectionName = "parsed_tsv";
        this.databaseUrl = "mongodb://localhost:27017";
        this.onClose = () => { };
        this.onReconnect = () => { };
        this.onError = (err) => { };
        this.onTimeout = () => { };
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
            if (_.isFunction(options.onClose)) {
                this.onClose = options.onClose;
            }
        }
        const mongoOptions = _.isObject(options)
            ? options.mongoClientOptions || {}
            : {};
        this.client = new mongodb_1.MongoClient(this.databaseUrl, Object.assign({ reconnectTries: 300, reconnectInterval: 2000, useNewUrlParser: true }, mongoOptions)).connect();
        this.db = this.client.then(client => {
            const db = client.db(this.databaseName);
            db.on("reconnect", () => {
                console.log("-> reconnected");
                this.onReconnect();
            });
            db.on("error", err => {
                this.onError(err);
            });
            db.on("close", () => {
                this.onClose();
            });
            db.on("timeout", () => {
                this.onTimeout();
            });
            return db;
        });
    }
    _write(chunk, encoding, callback) {
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
    _final(callback) {
        if (this.lastInsert) {
            this.lastInsert.then(() => {
                this.client.then(client => client.close());
            });
        }
        callback();
    }
}
exports.MongoWriteStream = MongoWriteStream;
