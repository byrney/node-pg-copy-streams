'use strict'

module.exports = function (txt, options) {
  return new CopyStreamQuery(txt, options)
}

const { Writable } = require('stream')
const code = require('./message-formats')

// Lifecycle:
//     0. database is ReadyForQuery
//     1. construct stream
//     2. user calls client.query(copyFromStream)
//     3. pg calls submit which sends the query
//     4. pg calls handleCopyInResponse
//     5. send csv bytes
//     6. send CopyDone
//     7. pg calls handleReadyForQuery
class CopyStreamQuery extends Writable {
  constructor(text, options) {
    super(options)
    this.text = text
    this.rowCount = 0
    this._gotCopyInResponse = false
    this.bufferedChunks = []
    this.bufferedLength = 0
    this.maxBuffer = (options && options.maxBuffer) || 1024 * 512
    this.cb_CopyInResponse = null
    this.cb_ReadyForQuery = null
    this.cb_destroy = null
    this.calls = {}
    this.cork()
  }

  stats(method, count) {
    this.calls[method] = this.calls[method] + count || count
  }

  submit(connection) {
    this.connection = connection
    connection.query(this.text)
  }

  callback() {
    // this callback is empty but defining it allows
    // `pg` to discover it and overwrite it
    // with its timeout mechanism when query_timeout config is set
  }

  sendBatches(cb) {
    this.stats('sendBatches', 1)
    this.sendBuffer(Buffer.concat(this.bufferedChunks), cb)
    this.bufferedChunks = []
    this.bufferedLength = 0
  }

  _write(chunk, enc, cb) {
    this.stats('write', 1)
    this.bufferedChunks.push(chunk)
    this.bufferedLength += Buffer.byteLength(chunk)
    if (this.bufferedLength > this.maxBuffer && this._gotCopyInResponse === true) {
      this.sendBatches(cb)
    } else {
      cb()
    }
  }

  _destroy(err, cb) {
    // writable.destroy([error]) was called.
    // send a CopyFail message that will rollback the COPY operation.
    // the cb will be called only after the ErrorResponse message is received
    // from the backend
    if (this.cb_ReadyForQuery) return cb(err)
    this.cb_destroy = cb
    const msg = err ? err.message : 'NODE-PG-COPY-STREAMS destroy() was called'
    const self = this
    const done = function () {
      self.connection.sendCopyFail(msg)
    }
    if (this._gotCopyInResponse) {
      done()
    } else {
      this.cb_CopyInResponse = done
    }
  }

  _final(cb) {
    if (this._gotCopyInResponse) {
      this.sendBatchesAndCopyDone(cb)
    } else {
      this.cb_CopyInResponse = () => this.sendBatchesAndCopyDone(cb)
    }
  }

  sendCopyDone(cb) {
    this.stats('done', 1)
    this.cb_ReadyForQuery = cb
    const Int32Len = 4
    const finBuffer = Buffer.from([code.CopyDone, 0, 0, 0, Int32Len])
    this.connection.stream.write(finBuffer)
  }

  sendBatchesAndCopyDone(cb) {
    if (this.bufferedLength > 0) {
      this.sendBatches(() => this.sendCopyDone(cb))
    } else {
      this.sendCopyDone(cb)
    }
  }

  sendBuffer(buffer, cb) {
    this.stats('sendBuffer', 1)
    const Int32Len = 4
    const lenBuffer = Buffer.from([code.CopyData, 0, 0, 0, 0])
    lenBuffer.writeUInt32BE(buffer.length + Int32Len, 1)
    this.connection.stream.write(lenBuffer)
    if (this.connection.stream.write(buffer)) {
      cb()
    } else {
      this.connection.stream.once('drain', cb)
    }
  }

  handleError(e) {
    // clear `pg` timeout mechanism
    this.callback()

    if (this.cb_destroy) {
      const cb = this.cb_destroy
      this.cb_destroy = null
      cb(e)
    } else {
      this.emit('error', e)
    }
    this.connection = null
  }

  handleCopyInResponse(connection) {
    this._gotCopyInResponse = true
    if (!this.destroyed) {
      this.uncork()
    }
    this.cb_CopyInResponse && this.cb_CopyInResponse()
  }

  handleCommandComplete(msg) {
    // Parse affected row count as in
    // https://github.com/brianc/node-postgres/blob/35e5567f86774f808c2a8518dd312b8aa3586693/lib/result.js#L37
    const match = /COPY (\d+)/.exec((msg || {}).text)
    if (match) {
      this.rowCount = parseInt(match[1], 10)
    }
  }

  handleReadyForQuery() {
    // triggered after ReadyForQuery
    // we delay the _final callback so that the 'finish' event is
    // sent only when the ingested data is visible inside postgres and
    // after the postgres connection is ready for a new query

    // Note: `pg` currently does not call this callback when the backend
    // sends an ErrorResponse message during the query (for example during
    // a CopyFail)

    // clear `pg` timeout mechanism
    this.callback()

    this.cb_ReadyForQuery()
    this.connection = null
  }
}
