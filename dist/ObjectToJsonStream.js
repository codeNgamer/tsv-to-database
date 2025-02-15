"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const stream_1 = require("stream");
/**
 * This class can convert object stream to json string
 *
 * @export
 * @class ObjectToJsonStream
 * @extends {Transform}
 */
class ObjectToJsonStream extends stream_1.Transform {
    constructor() {
        super({ objectMode: true });
        this.isFirstChunk = true;
    }
    _transform(chunk, encoding, callback) {
        let json = chunk.map(x => `,${JSON.stringify(x)}`).join("");
        if (this.isFirstChunk) {
            json = "[" + json.slice(1);
            this.isFirstChunk = false;
        }
        this.push(json);
        callback();
    }
    _flush(callback) {
        this.push("]");
        callback();
    }
}
exports.ObjectToJsonStream = ObjectToJsonStream;
