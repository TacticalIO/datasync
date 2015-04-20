var myIP = require('my-ip'),
  mqtt = require('mqtt'),
  debug = require('debug')('datasync');

var singleton;

var DataSynchroniser = function(options) {
  var self = this;
  options = options || {};

  options.ip = options.ip || myIP();
  options.port = options.port || 1883;
  options.syncperiod = options.syncperiod || 100;
  options.rpctimeout = options.rpctimeout || 500;
  
  var pack = JSON.stringify, unpack = JSON.parse;

  self.uuid = Math.random().toString(36).slice(2);

	var data = {},
		mqttClient = mqtt.createClient(options.port, options.ip),
    observers = {},
    services = {},
    syncheap = { id: self.uuid, data: {}},
    rpcResultProcessing = {}, 
    resetTimer = {};

	mqttClient.on('message', function(topic, message) {
    var message = unpack(message);
    debug('message in: ', message);
    if (topic === 'datasync' && message.id !== self.uuid) {
      var data = message.data,
        variables = Object.keys(data);
      for (var i = 0; i < variables.length; i++) {
        self.internalSet(variables[i], data[variables[i]].value, data[variables[i]].t);
      }
    } else if (topic === 'rpc' && message.id !== self.uuid) {
      if (message.service && services[message.service]) {
        var result = services[message.service](message.args, message.id);
        if (result) self.rpcResponse(message.token, result);
      } else {
        if (rpcResultProcessing[message.token]) {
          for (var i = 0; i < rpcResultProcessing[message.token].length; i++) {
            rpcResultProcessing[message.token][i](message.result);
          }
          delete rpcResultProcessing[message.token];
        }
      }
    } else if (topic === 'subscriptions' && message.id !== self.uuid) {
      self.refresh();
      if (options.onsubscribe) options.onsubscribe(message);
    }
	});

  self.registerService = function(method, callback) {
    services[method] = callback;
  }

  self.unregisterService = function(method) {
    if (services[method]) {
      delete services[method];
    } else {
      throw new Error('[RPC] Service does not exist and cannot be unregistered: ' + method);
    }
  }

  self.registerServicesProvider = function(obj, prefix) {
    for (var method in obj) {
      if (obj.hasOwnProperty(method) && (typeof obj[method] == 'function')) {
        self.registerService((prefix ? prefix : '') + method, obj[method]);
        debug('service registered', (prefix ? prefix : '') + method);
      }
    }
  }

  self.unregisterServicesProvider = function(obj, prefix) {
    for (var method in obj) {
      if (obj.hasOwnProperty(method) && (typeof method == 'function')) {
        self.unregisterService((prefix ? prefix : '') + method, obj[method]);
        debug('service unregistered', (prefix ? prefix : '') + method);
      }
    }
  }

  self.observe = function(name, fct) {
    if (!observers[name]) {
      observers[name] = [ fct ];
    } else {
      observers[name].push(fct);
    }
  }

  self.observers = function(name) {
    if (name) {
      return observers[name];
    } else {
      return observers;
    }
  }

  self.resetObservers = function(name) {
    if (observers[name]) delete observers[name];
  }

  self.set = function(name, value, duration) {
  	var old = data[name] ? data[name].value : undefined;

  	data[name] = {
      value: value,
      t: new Date().getTime()
    }
    if (duration && !resetTimer[name]) {
      resetTimer[name] = setTimeout(function(name, v) {
        self.set(name, v);
        delete resetTimer[name];
      }, duration, name, old);
    }
    if (observers[name]) {
      for (var i = 0; i < observers[name].length; i++) {
        observers[name][i](value, old, data[name].t);
      }
    }
    if (observers['_ALL_']) {
      // recommended not more than one single global observer
      observers['_ALL_'][0](name, value, old, data[name].t);
    }
    syncheap.data[name] = data[name];
  }

  // time conservative setter used internally on message reception
  self.internalSet = function(name, value, t) {
    var old = data[name] ? data[name].value : undefined; 
    data[name] = {
      value: value,
      t: t || new Date().getTime()
    }
    if (observers[name]) {
      for (var i = 0; i < observers[name].length; i++) {
        observers[name][i](value, old, data[name].t);
      }
    }
    if (observers['_ALL_']) {
      // recommended not more than one single global observer
      observers['_ALL_'][0](name, value, old, data[name].t);
    }
  }

  self.get = function(name) {
  	if (data[name]) {
  		return data[name].value;
  	}
  	return undefined;
  }

  self.timestamp = function(name) {
  	if (data[name]) {
  		return data[name].t;
  	}
  	return undefined;
  }

  self.subscribe = function(topic) {
		mqttClient.subscribe(topic, { qos: 1 });
  }

  self.unsubscribe = function(topic) {
		mqttClient.unsubscribe(topic);
  }

  self.publish = function(topic, message) {
  	mqttClient.publish(topic, pack(message));
  }

  self.sync = function() {
    if (JSON.stringify(syncheap.data) != '{}') {
      mqttClient.publish('datasync', pack(syncheap));
      syncheap = { id: self.uuid, data: {}};
    }
  }

  self.forceSync = function(name) {
    var toSync = { id: self.uuid, data: { name: self.data[name] }};
    mqttClient.publish('datasync', pack(toSync));
  }

  self.refresh = function() {
    if (JSON.stringify(data) != '{}') {
      mqttClient.publish('datasync', pack({
        id: self.uuid,
        data: data
      }));
    }
  }

  self.rpc = function(remoteService, args, cb) {
    var token = Math.random().toString(36).slice(2);
    mqttClient.publish('rpc', pack({
      id: self.uuid,
      token: token,
      service: remoteService,
      args: args
    }));
    if (cb) {
      if (!rpcResultProcessing[token]) {
        rpcResultProcessing[token] = [ cb ];
      } else {
        rpcResultProcessing[token].push(cb);
      }

      setTimeout(function(atoken) {
        if (rpcResultProcessing[atoken]) {
          for (var i = 0; i < rpcResultProcessing[atoken].length; i++) {
            rpcResultProcessing[atoken][i]();
          }
          delete rpcResultProcessing[atoken];
        }
      }, options.rpctimeout, token);      
    }
  }

  self.rpcResponse = function(token, result) {
    mqttClient.publish('rpc', pack({
      id: self.uuid,
      token: token,
      result: result
    }));
  }

  self.data = function() {
  	return data;
  }

  self.subscribe('datasync');
  self.subscribe('rpc');  
  self.subscribe('subscriptions');  
  self.publish('subscriptions', { id: self.uuid, ip: myIP(), os: process.os, arch: process.arch });

  setInterval(self.sync, options.syncperiod);

  // only observer: does not change values locally
  if (options.observer) {
    self.set = function() {};
    self.refresh = function() {};
  }

  singleton = self;
}

exports.data = function(options) {
  singleton || (singleton = new DataSynchroniser(options))
  return singleton;
}