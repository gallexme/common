const dnode = require('dnode');
const q = require('q');
const net = require('net');
const config = require('./config-manager').config;
const MongoClient = require('mongodb').MongoClient
const Redis = require('ioredis');
var MongoClientConnection = false;
var RedisConnection = false;
var SubscriberRedisConnection = false;
class Queue {
    static fetch(name, cb) {
      RedisConnection.rpop(name, (err,task)=> {
        if(err) {
          return cb(err);
        }
        RedisConnection.lpush(name+".processing", task, (err,id)=> {
          return cb(null,task);
        })
      });
    }
    static markDone(name,id,cb) {
      RedisConnection.lrem(name+".processing",0,id,(err,result) => {
        if(err) {
          return cb(err);
        }
          return cb(null,cb)

      });
    }
    static add(name, id, cb) {
      RedisConnection.lpush(name+".processing", id, (err,id)=> {
        if(err) {
          return cb(err);
        }
        return cb(null,true);
      })
    }
    static addMulti(name,array,cb) {
      RedisConnection.lpush(name+".processing", array, (err,id)=> {
        if(err) {
          return cb(err);
        }
        return cb(null,true);
      })
    }
    static whenAllDone(name, cb) {
      return cb(null,RedisConnection.llen(name+".processing") == 0 &&RedisConnection.llen(name) == 0)
    }
    static reset(name,cb) {
      cb(redisConnection.del(name) && redisConnection.del(name+".processing"));
    }
}
config.common.dbCollections = [
    'leaderboard.power',
    'leaderboard.seasons',
    'leaderboard.world',
    'market.intents',
    'market.orders',
    'market.stats',
    'rooms',
    'rooms.objects',
    'rooms.flags',
    'rooms.intents',
    'rooms.terrain',
    'transactions',
    'users',
    'users.code',
    'users.console',
    'users.messages',
    'users.money',
    'users.notifications',
    'users.resources'
];

config.common.storage = exports;

exports.db = {};
exports.queue = {};
exports.env = {
    keys: {
        ACCESSIBLE_ROOMS: 'accessibleRooms',
        MEMORY: 'memory:',
        GAMETIME: 'gameTime',
        MAP_VIEW: 'mapView:',
        TERRAIN_DATA: 'terrainData',
        SCRIPT_CACHED_DATA: 'scriptCachedData:',
        USER_ONLINE: 'userOnline:',
        MAIN_LOOP_PAUSED: 'mainLoopPaused',
        ROOM_HISTORY: 'roomHistory:'
    }
};
exports.pubsub = {
    keys: {
        QUEUE_DONE: 'queueDone:',
        RUNTIME_RESTART: 'runtimeRestart',
        TICK_STARTED: "tickStarted",
        ROOMS_DONE: "roomsDone"
    }
};
function redisConnect() {
    if (process.env.REDIS_IP) {
        RedisConnection = new Redis({
            port: process.env.REDIS_PORT
                ? process.env.REDIS_PORT
                : 6379,
            host: process.env.REDIS_IP
        });
        SubscriberRedisConnection = new Redis({
            port: process.env.REDIS_PORT
                ? process.env.REDIS_PORT
                : 6379,
            host: process.env.REDIS_IP
        });

    }
}
exports._connect = function storageConnect() {
    redisConnect();
    var defer = q.defer();
    if (process.env.MONGO_URL) {
        console.log("COnnecting with Mongo: " + process.env.MONGO_URL)
        MongoClient.connect(process.env.MONGO_URL, function(err, db) {
            if (err != null) {
                console.error("DB ERROR", db)
                return false;
            }
            db = MongoClientConnection;
            defer.resolve(queueConnect());
        });
    } else {
        defer.resolve(queueConnect());
    }
    return defer.promise

}
function queueConnect() {

    if (exports._connected) {
        return q.when();
    }

    if (!process.env.STORAGE_PORT) {
        throw new Error('STORAGE_PORT environment variable is not set!');
    }

    console.log('Connecting to storage');

    var socket = net.connect(process.env.STORAGE_PORT, process.env.STORAGE_HOST);
    var d = dnode();
    socket.pipe(d).pipe(socket);

    var defer = q.defer();
    var resetDefer = q.defer();

    function resetInterceptor(fn) {
        /*return function() {
            var promise = fn.apply(null, Array.prototype.slice.call(arguments));
            return q.any([promise, resetDefer.promise])
            .then(result => result === 'reset' ? q.reject('Storage connection lost') : result);
        }*/
        // TODO
        return fn;
    }

    d.on('remote', remote => {

        function wrapCollection(collectionName) {
            var wrap = {};
            [
                'find',
                'findOne',
                'by',
                'clear',
                'count',
                'ensureIndex',
                'removeWhere',
                'insert'
            ].forEach(method => {
                wrap[method] = function() {
                    return q.ninvoke(remote, 'dbRequest', collectionName, method, Array.prototype.slice.call(arguments));
                }
            });
            wrap.update = resetInterceptor(function(query, update, params) {
                return q.ninvoke(remote, 'dbUpdate', collectionName, query, update, params);
            });
            wrap.bulk = resetInterceptor(function(bulk) {
                return q.ninvoke(remote, 'dbBulk', collectionName, bulk);
            });
            wrap.findEx = resetInterceptor(function(query, opts) {
                return q.ninvoke(remote, 'dbFindEx', collectionName, query, opts);
            });
            return wrap;
        }

        if (!MongoClientConnection) {
            config.common.dbCollections.forEach(i => exports.db[i] = wrapCollection(i));
            exports.resetAllData = () => q.ninvoke(remote, 'dbResetAllData');
        } else {

            config.common.dbCollections.forEach(i => exports.db[i] = MongoClientConnection.collection(i));

        }

        if (!RedisConnection) {
          console.log("NO REDIS CLIENT");
            Object.assign(exports.queue, {
                fetch: resetInterceptor(q.nbind(remote.queueFetch, remote)),
                add: resetInterceptor(q.nbind(remote.queueAdd, remote)),
                addMulti: resetInterceptor(q.nbind(remote.queueAddMulti, remote)),
                markDone: resetInterceptor(q.nbind(remote.queueMarkDone, remote)),
                whenAllDone: resetInterceptor(q.nbind(remote.queueWhenAllDone, remote)),
                reset: resetInterceptor(q.nbind(remote.queueReset, remote))
            });
            Object.assign(exports.env, {
                get: resetInterceptor(q.nbind(remote.dbEnvGet, remote)),
                mget: resetInterceptor(q.nbind(remote.dbEnvMget, remote)),
                set: resetInterceptor(q.nbind(remote.dbEnvSet, remote)),
                setex: resetInterceptor(q.nbind(remote.dbEnvSetex, remote)),
                expire: resetInterceptor(q.nbind(remote.dbEnvExpire, remote)),
                ttl: resetInterceptor(q.nbind(remote.dbEnvTtl, remote)),
                del: resetInterceptor(q.nbind(remote.dbEnvDel, remote)),
                hmset: resetInterceptor(q.nbind(remote.dbEnvHmset, remote))
            });
            Object.assign(exports.pubsub, {
                publish: resetInterceptor(q.nbind(remote.publish, remote)),
                subscribe: (channel, cb) => remote.subscribe(channel, cb)
            });
        } else {
          console.log("REDIS CLIENT");
          Object.assign(exports.queue, {
              fetch: Queue.fetch,
              add: Queue.add,
              addMulti: Queue.addMulti,
              markDone: Queue.markDone,
              whenAllDone:Queue.whenAllDone,
              reset: Queue.reset
          });
          Object.assign(exports.env, {
              get: RedisConnection.get,
              mget:RedisConnection.mget,
              set: RedisConnection.set,
              setex: RedisConnection.setex,
              expire: RedisConnection.expire,
              ttl: RedisConnection.ttl,
              del: RedisConnection.del,
              hmset: RedisConnection.hmset
          });
            Object.assign(exports.pubsub, {
                publish: RedisConnection.publish,
                subscribe: (channel, cb) => SubscriberRedisConnection.subscribe
            });

        }

        exports._connected = true;

        defer.resolve();
    });

    socket.on('error', err => {
        console.error('Storage connection lost', err);
        resetDefer.resolve('reset');
        exports._connected = false;
        setTimeout(exports._connect, 1000);
    });
    socket.on('end', () => {
        console.error('Storage connection lost');
        resetDefer.resolve('reset');
        exports._connected = false;
        setTimeout(exports._connect, 1000)
    });

    return defer.promise;
};
