const Benchmark = require('benchmark')
const cp = require('duplex-child-process')
const pg = require('pg')
const stream = require('stream')

const copy = require('../').from

const client = function () {
  const client = new pg.Client()
  client.connect()
  return client
}

const psql = 'psql'
const limit = 999999
const inStream = function () {
  const s = stream.Readable()
  for (let i = 0; i < limit; i++) {
    s.push(`${i}\n`)
  }
  s.push(null)
  return s
}

const suite = new Benchmark.Suite()
suite
  .add({
    name: 'pipe into psql COPY',
    defer: true,
    fn: function (d) {
      const c = client()
      c.query('DROP TABLE IF EXISTS plugnumber', function () {
        c.query('CREATE TABLE plugnumber (num int)', function () {
          c.end()
          const seq = inStream()
          const from = cp.spawn(psql, ['postgres', '-c', 'COPY plugnumber FROM STDIN'])
          seq.pipe(from)
          from.on('close', function () {
            d.resolve()
          })
        })
      })
    },
  })
  .add({
    name: 'pipe into pg-copy-stream COPY',
    defer: true,
    fn: function (d) {
      const c = client()
      c.query('DROP TABLE IF EXISTS plugnumber', function () {
        c.query('CREATE TABLE plugnumber (num int)', function () {
          const seq = inStream()
          const from = c.query(copy('COPY plugnumber FROM STDIN'))
          seq.pipe(from)
          from.on('finish', function () {
            c.end()
            d.resolve()
          })
        })
      })
    },
  })

  .on('cycle', function (event) {
    console.log(String(event.target))
  })
  .on('complete', function () {
    console.log('Fastest is ' + this.filter('fastest').map('name'))
  })

const c = client()
c.query('DROP TABLE IF EXISTS plugnumber', function () {
  c.end()
  suite.run()
})
