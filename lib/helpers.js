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

  o.find_job = function (args) { // ie find job in arguments array
    return _.find(_.flatten(_.toArray(args)), function (v) {
      return v && v.is_job;
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
    return def_v;
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
