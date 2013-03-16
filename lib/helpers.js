var _ = require('underscore')
;

exports.init = function () {

  var o = {};
  o.ALL_SPACES = / /g;

  // ****************************************************************
  // ****************** NOTES: **************************************
  // ****************************************************************
  // Emitters are used only for logging and introspection and inspection.
  // They should not be used for adding functionality to error handling
  //   and flow control.

  // ****************************************************************
  // ****************** Helpers *************************************
  // ****************************************************************

  o.throw_it = function (msg) {
    if (!msg)
      throw new Error('Unknown error');
    if (msg && msg.message)
      throw msg;
    throw new Error("" + msg);
  }

  o.array_ize_all = function (args) {
    if (!_.isArray(args) && !(_.isObject(args) && args.hasOwnProperty('0')))
      return args;

    return _.map(args, function (v) {
      if (_.isObject(v) && v.hasOwnProperty('0'))
        v = _.toArray(v);
      if (_.isArray(v))
        v = array_ize_all(v);
      return v;
    });
  };

  o.find_job = function (raw) { // ie find job in arguments array
    var args = array_ize_all(raw);
    return _.find(_.flatten(args), function (v) {
      if (v && v.is_job)
        return v;
      return false;
    });
  }

  o.origin = function (unk) {
    return _.last(parents(unk));
  };

  o.parent = function (unk) {
    return unk.parent_job || unk.river;
  }

  o.parents = function (unk) {
    if (!parent(unk))
      return [];

    var anc     = [];
    var current = parent(unk);

    while(!current) {
      anc.push(current);
      current = parent(current);
    }

    return parents;
  }

  o.find_parent_in_error = function (unk) {
    return _.find(parents(unk), function (p) {
      return p && p.has_error();
    });
  }

  o.chain_has_error = function (unk) {
    return unk.has_error() || !!( find_parent_in_error(unk) );
  }

  o.read_able_set = function (k, v) {
    if (k === 'invalid')
      k = 'not_valid';

    this.data[k] = v;
    return this;
  }

  o.read_able_get = function (key, def_val) {
    if (key === 'invalid')
      key = 'not_valid';
    if (this.data.hasOwnProperty(key))
      return this.data[key];
    return def_val;
  }

  o.read_able_erase = function (k, def_v) {
    var v = this.get(k, def_v);
    this.set(k);
    return v;
  }

  o.read_able = function (o) {
    if (!o.data)
      o.data = {};
    o.set = read_able_set;
    o.get = read_able_get;
    o.erase = read_able_erase;
    return o;
  }

  o = _.map(o, function (v, name) {
    return ("var " + name + " = " + v.toString() + " ; ");
  }).join(" ");

  return o;

};
