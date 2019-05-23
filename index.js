#!/usr/bin/env node

var request = require('request');
var program = require('commander');
var fs = require('fs');
var throttled = require('throttled-promise');
var pkg = require('./package');

var _ = require('underscore');
var Q = require('q');

// Since etcd creates the dir keys automatically
// transform the tree of keys to contain only a flat array of leaves
function normalize(obj) {
  obj = obj.node || obj;
  // Is a leaf
  if (!_.has(obj, 'nodes')) {
    return _.pick(obj, 'key', 'value', 'dir');
  }
  return _.flatten(_.map(obj.nodes, normalize));
};

function Dumper() {
  _.bindAll.apply(_, [this].concat(_.functions(this)));
};

Dumper.prototype.restore = function (json, callback) {
  var entries = normalize(json);

  Q.all(_.map(entries, function (entry) {
    callback(entry);
  }));
};

function checkFile(file) {
  if (!file) {
    console.error('Error missing --file option');
    process.exit(1);
  }
}

program
  .version(pkg.version)
  .option('-f, --file <file>', 'backup file')
  .option('-e, --etcd <etcd>', 'etcd url eg: https://0.0.0.0:2379')
  .option('-c, --concurrency <concurrency>', 'max parallel requests')

program
  .command('restore')
  .description('restore keys from backup file')
  .action(function (options) {

    var file = options.parent.file || undefined;
    var etcd = options.parent.etcd || 'http://0.0.0.0:2379';
    var concurrency = options.parent.concurrency || 5;

    checkFile(file);

    var promises = [];

    var strings = fs.readFileSync(file).toString();

    new Dumper().restore(JSON.parse(strings), function (entry) {

      var options = {
        rejectUnauthorized: false,
        uri: etcd + '/v2/keys' + entry.key,
        method: 'put'
      }

      if ((_.has(entry, 'dir'))) {
        options.form = { dir : true };
        console.log(entry.key + " -> DIR");
      } else {
        options.form = { value : entry.value };
        console.log(entry.key + " -> " + entry.value);
      }

      promises.push(new throttled(function (resolve, reject) {
        request(options, function (err, response, body) {
          if (err) {
            return reject(err);
          }
          console.log("Requests left: " + promises.length);
          return resolve(body);
        });
      }));
    });

    // collect results
    throttled.all(promises, concurrency)
      .then(function (results) {
        console.log('Dump restored')
      })
      .catch(function (err) {
        console.log(err);
        process.exit(1);
      });
  });

program
  .command('dump')
  .description('dump keys to backup file')
  .action(function (options) {

    var file = options.parent.file || undefined;
    var etcd = options.parent.etcd || 'http://0.0.0.0:2379';

    var options = {
      rejectUnauthorized: false,
      uri: etcd + '/v2/keys/?recursive=true',
      method: 'get',
      json: true
    }

    request(options, function (err, res, body) {

      if (err) {
        console.error(err);
        process.exit(1);
      }

      var data = JSON.stringify(body, null, 2);

      fs.writeFileSync(file, data, null, function (err) {
        if (err) {
          console.error(err);
          process.exit(1);
        }
      });

    });

  });

program.parse(process.argv);

if (!program.args.length) program.help();
