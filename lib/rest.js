var util = require('util')
    , EventEmitter = require('events').EventEmitter
    , MongoClient = require('mongodb').MongoClient
    , ObjectID = require('mongodb').ObjectID
    , jwt = require("jsonwebtoken");





function RestAPI() {
    this.routes = [
        { method: 'get', mount: '', handler: this._query },
        { method: 'get', mount: '/:id', handler: this._get },
        { method: 'put', mount: '', handler: this._create },
        { method: 'post', mount: '', handler: this._create },
        { method: 'post', mount: '/:id', handler: this._update },
        { method: 'delete', mount: '/:id', handler: this._remove }
    ];
}
util.inherits(RestAPI, EventEmitter);
RestAPI.prototype.getRoutes = function () {
    return this.routes;
}

RestAPI.prototype._query = function (req, res) {
    var self = this;

    self.resolveAuthentification(req, function () {
        if (!req.user || !req.user.bucket) {
            return exports.sendError( res, 401, 'Unauthorized');
        }
        self.query.call(self, req.user.bucket, exports.responseCallback(res));
    });
}
RestAPI.prototype._get = function (req, res) {
    var self = this;

    self.resolveAuthentification(req, function () {
        if (!req.user || !req.user.bucket) {
            return exports.sendError( res, 401, 'Unauthorized');
        }
        if (!req.params.id) {
            exports.sendError(res, 400, 'id missing');
        } else {
            self.get.call( self, req.user.bucket, { _id: ObjectID(req.params.id) }, exports.responseCallback(res));
        }
    });
}
RestAPI.prototype._create = function (req, res) {
    var self = this;

    self.resolveAuthentification(req, function () {
        if (!req.user || !req.user.bucket) {
            return exports.sendError( res, 401, 'Unauthorized');
        }
        if (!req.body) {
            exports.sendError(res, 400, 'body missing');
        } else {
            self.create.call( self, req.user.bucket, req.body, exports.responseCallback(res));
        }
    });

}
RestAPI.prototype._update = function (req, res) {

    var self = this;

    self.resolveAuthentification(req,  function () {
        if (!req.user || !req.user.bucket) {
            return exports.sendError( res, 401, 'Unauthorized');
        }
        if (!req.params.id) {
            exports.sendError(res, 400, 'id missing');
        } else if (!req.body) {
            exports.sendError(res, 400, 'body missing');
        } else {
            self.update.call( self, req.user.bucket, { _id: ObjectID(req.params.id) }, req.body, exports.responseCallback(res));
        }
    });
}
RestAPI.prototype._remove = function (req, res) {

    var self = this;

    self.resolveAuthentification(req, function () {
        if (!req.user || !req.user.bucket) {
            return exports.sendError( res, 401, 'Unauthorized');
        }

        if (!req.params.id) {
            exports.sendError(res, 400, 'id missing');
        } else {
            self.remove.call(self, req.user.bucket, { _id: ObjectID(req.params.id) }, exports.responseCallback(res));
        }
    });
};


/**
 * Constructor
 * @param uri URI of mongodb base
 * @param privateKey Private key to validate
 * @param collection Name of collection
 * @param prefixMode If true, use a prefix mode instead of multiple connexion
 * @constructor
 */
function RestMongoAPI(uri, privateKey, collection, prefixMode) {
    this.mongoUri = uri;
    this.connections = {};
    this.privateKey = privateKey;
    this.collection = collection;
    this.prefixMode = prefixMode;

    RestAPI.call(this);

}





util.inherits(RestMongoAPI, RestAPI);


RestMongoAPI.prototype.resolveAuthentification = function(req, cb) {

    var token;

    if (req.method === 'OPTIONS' && req.headers.hasOwnProperty('access-control-request-headers')) {
        if (req.headers['access-control-request-headers'].split(', ').indexOf('authorization') != -1) {
            return cb();
        }
    }

    if( req.query.token) {
      token = req.query.token;
    }
    else if (req.headers && req.headers.authorization) {


      var parts = req.headers.authorization.split(' ');
      if (parts.length == 2) {
        var scheme = parts[0]
          , credentials = parts[1];

        if (/^Bearer$/i.test(scheme)) {
          token = credentials;
        }
      } else {
        return cb();
      }

    }
    else {
      return cb();
    }

    jwt.verify(token, this.privateKey, function (err, decoded) {
        if (err) return cb();

        req.user = decoded;

        return cb(token);
    });
}


RestMongoAPI.prototype._getBucketConnection = function( bucket, callback ) {
    var self = this;

    if( this.prefixMode ) {
      if( this.connections['main'] == undefined ) {

        if (this.mongoUri.collection) {
          self.connections['main'] = {
            db: this.mongoUri,
            connected_at: new Date()
          };

          if( bucket && bucket.length > 0 )
            callback( self.connections['main'].db, bucket + "_" );
          else
            callback( self.connections['main'].db, "" );
        }
        else {
          MongoClient.connect(this.mongoUri, { auto_reconnect: true }, function (err, db) {
            if (err) return self.emit('error', err);
            self.connections['main'] = {
              db: db,
              connected_at: new Date()
            };
            self.emit('connect', db);
            db.on('error', function (err) {
              self.emit('error', err);
            });

            if( bucket && bucket.length > 0 )
              callback( self.connections['main'].db, bucket + "_" );
            else
              callback( self.connections['main'].db, "" );
          });
        }
      }
      else if( callback) {
        if( bucket && bucket.length > 0 )
          callback( self.connections['main'].db, bucket + "_" );
        else
          callback( self.connections['main'].db, "" );
      }
    }
    else {
      if( this.connections[bucket] == undefined ) {
          // Instantiate new mongo connection
          MongoClient.connect(this.mongoUri + "/" + bucket, { auto_reconnect: true }, function (err, db) {
              if (err) return self.emit('error', err);
              self.connections[bucket] = {
                  db: db,
                  connected_at: new Date()
              };
              self.emit('connect', db);
              db.on('error', function (err) {
                  self.emit('error', err);
              });

              callback( self.connections[bucket].db, "" );
          });
      }
      else if( callback ) {
          callback( this.connections[bucket].db, "" );
      }
    }
};


RestMongoAPI.prototype._return = function (err, data, isArray, cb) {
    if (!cb) {
        return;
    } else if (err) {
        cb(500, err.message ? err.message : err);
    } else if (!data) {
        cb(404);
    } else if (isArray) {
        cb(null, data);
    } else {
        cb(null, util.isArray(data) ? data[0] : data);
    }
}
RestMongoAPI.prototype.query = function (bucket, criteria, options, cb) {
    if (typeof criteria === 'function') {
        cb = criteria;
        criteria = {};
        options = {};
    } else if (typeof options === 'function') {
        cb = options;
        options = {};
    }

    var self = this;


    self._getBucketConnection( bucket, function(db, prefix) {
        db.collection(prefix + self.collection).find(criteria, options).toArray(function (err, data) {
            self._return(err, data, true, cb);
        });
    });
}
RestMongoAPI.prototype.get = function (bucket, criteria, options, cb) {
    if (typeof options === 'function') {
        cb = options;
        options = {};
    }
    var self = this;
    self._getBucketConnection( bucket, function(db, prefix) {
        db.collection(prefix + self.collection).findOne(criteria, options, function (err, data) {
            self._return(err, data, false, cb);
        });
    });
}
RestMongoAPI.prototype.create = function (bucket, data, options, cb) {
    if (typeof options === 'function') {
        cb = options;
        options = {};
    }
    var self = this;
    self._getBucketConnection( bucket, function(db, prefix) {
        db.collection(prefix + self.collection).insert(data, options, function (err, data) {
            self._return(err, data, false, cb);
        });
    });
}
RestMongoAPI.prototype.update = function (bucket, criteria, data, options, cb) {
    if (typeof options === 'function') {
        cb = options;
        options = { w: 1 };
    }
    delete data._id;
    var self = this;

    self._getBucketConnection( bucket, function(db, prefix) {
        db.collection(prefix + self.collection).update(criteria, data, options, function (err, _data) {
            if (criteria._id) {
                data._id = criteria._id.toString();
            }
            self._return(err, _data ? data : null, false, cb);
        });
    });
}
RestMongoAPI.prototype.remove = function (bucket, criteria, options, cb) {
    if (typeof options === 'function') {
        cb = options;
        options = {};
    }
    var self = this;

    self._getBucketConnection( bucket, function(db, prefix) {
        db.collection(prefix + self.collection).remove(criteria, options, function (err, data) {
            if (err) {
                cb(500, err.message ? err.message : err);
            } else if (!data) {
                cb(400);
            } else {
                cb(null, '');
            }
        });
    });
}

exports.use = function (express, mount, api) {
    var routes = api.getRoutes();

    function getHandler(context, f) {
        return function (req, res, next) {
            f.call(context, req, res, next);
        }
    }

    for (var i = 0; i < routes.length; i++) {
        express[routes[i].method](mount + routes[i].mount, getHandler(api, routes[i].handler));
    }
}
exports.throwError = function (code, message) {
    console.error(code, message);
    var err = new Error(message);
    err.code = code;
    throw err;
}
exports.sendError = function (res, code, message) {
    var data = {
        success: false,
        code: code,
        error: message || ''
    };
    res.jsonp(code, data);
}
exports.responseCallback = function (res) {
    var self = this;
    return function (err, data) {
        if (err) {
            self.sendError(res, err.code || parseInt(err) || 400, err.message || data);
        } else {
            res.jsonp(data);
        }
    }
}
exports.RestAPI = RestAPI;
exports.RestMongoAPI = RestMongoAPI;

