var  _ = require('underscore')
, h       = eval(require('da_river/lib/helpers').init())
;

// ****************************************************************
// ****************** Job *****************************************
// ****************************************************************


var Job = exports.Job = function () { this.is_job = true; };
var ERROR_NAMES = ['not_found', 'not_valid', 'error'];

Job.new = function (vals) {
  var me      = new Job();
  me.result   = undefined;
  me.is_fin   = false;
  me.data     = {_finish_:[]};
  me.replys   = [];
  me.events   = {reply: []};
  read_able(me);

  // save propertys to job: eg group, id, etc.
  _.each(vals, function (v, k) {
    if (k === 'func' && !_.isFunction(v)) {
      me[k] = function (j) {
        var args = _.toArray(v);
        var obj  = args.shift();
        var meth = args.shift();
        args.push(j);
        return obj[meth].apply( obj, args );
      }
    } else {
      me[k] = v;
    }
  });

  return me;
};

Job.prototype.reply = function (f) {
  this.events.reply.push(f);
  return this;
};

Job.prototype._reply_ = function (rep, msg) {
  var me = this
  , type = rep
  , l    = arguments.length;

  if (l === 1) {
    if (rep && rep.has_error && rep.has_error()) {
      var err = rep.about_error;
      me.has_error(err.type, err.msg);
    } else {
      me.replys.push(rep);
      me.result = rep;
    }
  } else if (l > 1) {

    if (type === 'invalid')
      type = 'not_valid';

    if(!type)
      type = 'error';

    if (!msg)
      msg = 'Unknown error: ' + msg;

    if (!msg.message) {
      msg = new Error('' + msg)
      msg.original = _.toArray(arguments);
    }

    if (!msg.type)
      msg.type = type;

    me.has_error(type, msg);
  }

  return me;
};

Job.prototype.finish = function (rep, msg) {
  var me = this;
  if (me.is_fin || parent(me).is_finished() || me.has_error())
    return null;

  if (me.events.reply.length && arguments.length === 1) {
    me._reply_(rep);
    me.events.reply.pop()(me, me.result);
    return me;
  }

  me.is_fin = true;

  var args = arguments;
  var a = args;
  var l = args.length;

  // save the reply
  if (l > 0) {
    me._reply_.apply(me, arguments);
  } // ============  end l > 0

  if (!me.has_error())
    return parent(me).finish(me);

  var err  = me.about_error;
  var type = err.type;
  var msg  = err.msg;

  var err_func = me.get(type);
  if (!err_func)
    return parent(me).finish(me);

  var fin = {
    job    : me,
    finish : function () {
      if (arguments.length > 0)
        me._reply_.apply(me, arguments);
      return parent(me).finish(me);
    }
  };

  me.set(type, null);
  return err_func(fin);

};


Job.prototype.is_finished = function () {
  return !!this.is_fin;
};

Job.prototype.has_error = function (type, msg) {
  var me = this;

  if (!arguments.length)
    return !!me.about_error;

  me.about_error = { msg: msg, type: type };
  return me;
};


